defmodule NeonFS.Cluster.BootstrapProvider do
  @moduledoc """
  A `Config.Provider` that configures autonomous cluster formation from a
  data-only `bootstrap.json`.

  A node needs several things to go from an empty disk to a cluster member —
  the cluster's name, how many peers to expect, who those peers are *and which
  distribution port each listens on*, and which drives to register. Supplying
  them by environment variable does not survive contact with an orchestrator:
  `NEONFS_BOOTSTRAP_PEERS` has no way to express a peer's port, so peers parse
  to port 0 and get dropped, and `NeonFS.Epmd` cannot resolve them.

  ## Why JSON, and why a provider

  `Config.Reader` evaluates its input as Elixir. Reading a config file a
  Kubernetes ConfigMap supplies would therefore be arbitrary code execution
  from cluster state, which is not a trade worth making for a file that only
  ever carries data. OTP 28's `:json` is already used elsewhere in the tree, so
  parsing costs no dependency.

  Providers run *after* `config/runtime.exs` — releases prepend runtime.exs to
  the provider list — so anything here overrides the environment variables, and
  those remain as the fallback layer rather than being replaced outright.

  ## Living in `neonfs_client`

  Every package depends on this one, so the alternative is a copy of the same
  config block in each interface's `runtime.exs`, which is how two of them
  already drifted.

  ## What it does not do

  Nothing about *when* bootstrap should happen: this reads a file and returns
  configuration. Whether a node consults it at all — a node that has already
  initialised or joined must not re-bootstrap — is the caller's gate, not this
  module's.

  It also never deletes or rewrites the file. A ConfigMap mount is read-only,
  so a pod could not remove its own copy anyway, and a deliberate re-bootstrap
  after wiping the state volume needs it to still be there.

  ## Shape

      {
        "cluster_name": "neonfs",
        "bootstrap_expect": 3,
        "bootstrap_timeout_ms": 300000,
        "peers": [
          {"node": "neonfs@10.0.0.1", "dist_port": 9100},
          {"node": "neonfs@10.0.0.2", "dist_port": 9100}
        ],
        "drives": [
          {"id": "drive1", "path": "/mnt/neonfs/drive1", "tier": "hot", "capacity": "1T"}
        ]
      }

  Only `cluster_name` is required. A file with no `peers` configures a
  single-node bootstrap; one with no `drives` leaves the drive list to the
  environment, which is what a node whose drives are discovered rather than
  declared wants.
  """

  @behaviour Config.Provider

  @default_timeout_ms 300_000

  @impl Config.Provider
  def init(path) when is_binary(path), do: path

  @impl Config.Provider
  def load(config, path) do
    case File.read(path) do
      # Absent is the common case: the provider ships in every release and most
      # deployments configure by environment instead. Silence, not an error.
      {:error, :enoent} ->
        config

      {:error, reason} ->
        abort("cannot be read (#{:file.format_error(reason)})", path)

      {:ok, contents} ->
        Config.Reader.merge(config, bootstrap_config(contents, path))
    end
  end

  defp bootstrap_config(contents, path) do
    document = decode!(contents, path)
    cluster_name = required(document, "cluster_name", path)
    {peers, peer_ports} = parse_peers(document, path)

    core = [
      auto_bootstrap: true,
      cluster_name: cluster_name,
      bootstrap_expect: expect(document, peers, path),
      bootstrap_peers: peers,
      bootstrap_peer_ports: peer_ports,
      bootstrap_timeout: timeout(document, path)
    ]

    [neonfs_core: with_drives(core, document, path)]
  end

  defp decode!(contents, path) do
    case :json.decode(contents) do
      document when is_map(document) -> document
      _other -> abort("must contain a JSON object", path)
    end
  rescue
    # `:json.decode/1` raises rather than returning an error tuple, and its
    # exception says nothing about which file was at fault.
    error in [ErlangError, ArgumentError] ->
      abort("is not valid JSON (#{Exception.message(error)})", path)
  end

  defp required(document, key, path) do
    case Map.get(document, key) do
      value when is_binary(value) and value != "" -> value
      nil -> abort("is missing the required key #{inspect(key)}", path)
      other -> abort("has a non-string #{inspect(key)}: #{inspect(other)}", path)
    end
  end

  # Absent means "expect only me", which is the single-node case rather than an
  # omission worth refusing.
  defp expect(document, peers, path) do
    case Map.get(document, "bootstrap_expect") do
      nil -> max(length(peers), 1)
      value when is_integer(value) and value > 0 -> value
      other -> abort("has an invalid \"bootstrap_expect\": #{inspect(other)}", path)
    end
  end

  defp timeout(document, path) do
    case Map.get(document, "bootstrap_timeout_ms") do
      nil -> @default_timeout_ms
      value when is_integer(value) and value > 0 -> value
      other -> abort("has an invalid \"bootstrap_timeout_ms\": #{inspect(other)}", path)
    end
  end

  # The port is the reason this file exists, so a peer without one is refused
  # rather than silently defaulted: `NeonFS.Epmd` cannot resolve a peer it
  # cannot address, and the failure would otherwise surface as a formation
  # timeout with nothing pointing back here.
  defp parse_peers(document, path) do
    document
    |> Map.get("peers", [])
    |> Enum.map(&parse_peer(&1, path))
    |> Enum.reduce({[], %{}}, fn {node, port}, {nodes, ports} ->
      {[node | nodes], Map.put(ports, node, port)}
    end)
    |> then(fn {nodes, ports} -> {Enum.reverse(nodes), ports} end)
  end

  defp parse_peer(%{"node" => node, "dist_port" => port}, _path)
       when is_binary(node) and is_integer(port) and port > 0 do
    {String.to_atom(node), port}
  end

  defp parse_peer(peer, path) do
    abort(
      ~s|has a peer that is not {"node": "name@host", "dist_port": <port>}: #{inspect(peer)}|,
      path
    )
  end

  defp with_drives(core, document, path) do
    case Map.get(document, "drives") do
      nil ->
        core

      [] ->
        core

      drives when is_list(drives) ->
        Keyword.put(core, :drives, Enum.map(drives, &drive(&1, path)))

      other ->
        abort("has a non-list \"drives\": #{inspect(other)}", path)
    end
  end

  # Capacity stays a string here. Parsing it needs `NeonFS.Core.DriveConfig`,
  # which a provider running before applications start cannot rely on, and the
  # drive registry already parses the same strings from `cluster.json`.
  defp drive(%{"id" => id, "path" => drive_path} = drive, _path)
       when is_binary(id) and is_binary(drive_path) do
    %{
      id: id,
      path: drive_path,
      tier: Map.get(drive, "tier", "hot"),
      capacity: to_string(Map.get(drive, "capacity", "0"))
    }
  end

  defp drive(drive, path) do
    abort(~s|has a drive without a string "id" and "path": #{inspect(drive)}|, path)
  end

  # A misconfigured bootstrap file must stop the boot rather than degrade into
  # a node that starts, forms nothing, and passes its health check — which is
  # the failure mode this whole design exists to remove.
  defp abort(problem, path) do
    raise ArgumentError, "bootstrap file #{inspect(path)} #{problem}"
  end
end
