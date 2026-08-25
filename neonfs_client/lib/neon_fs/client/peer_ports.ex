defmodule NeonFS.Client.PeerPorts do
  @moduledoc """
  The single writer of `NEONFS_PEER_PORTS`.

  `NeonFS.Epmd` replaces EPMD, so a node dialling a peer has to *learn* that
  peer's distribution port. `NEONFS_PEER_PORTS` is one of its two sources —
  `"name@host:port,name@host:port"` — and this module owns it.

  ## Why an owner, rather than each writer setting it

  There are three writers and they learn about different peers:

    * `NeonFS.Client.Join` knows the node it joined via;
    * `NeonFS.Cluster.Formation` knows the peers it formed with;
    * `NeonFS.Client.Discovery` knows every registered service, which is the
      only source that ever names a *sibling interface node*.

  Each of the first two used to `System.put_env/2` the whole variable, which
  clobbered whatever was already there. That was latent while they never
  overlapped. It stops being latent the moment `Discovery` also writes: a
  rejoin or a re-formation would wipe every discovered sibling, the entries
  would silently reappear on the next refresh, and in between a dial would
  fail with `:nxdomain` for no reason anyone could reproduce.

  So `publish/1` **merges**, keyed by node name, newest wins. Anyone adding a
  fourth writer has to come through here; a direct `System.put_env/2` will
  appear to work and will quietly drop siblings.

  ## Why it lives here and not under `NeonFS.Epmd`

  `NeonFS.Epmd` runs before OTP applications start and must not depend on
  application config, GenServers or ETS. A sibling module in that namespace
  invites a call in the wrong direction. This one is an ordinary client-side
  module beside `Connection`, `Discovery` and `Registrar`; the env var is the
  whole of the interface between them.

  ## Port zero

  A node with no `NEONFS_DIST_PORT` reports `dist_port: 0`. Those are dropped
  rather than published: publishing a zero would resolve to a broken port,
  where omitting it lets `NeonFS.Epmd` fall through to `cluster.json`.
  """

  require Logger

  @env "NEONFS_PEER_PORTS"

  @doc """
  Merges `entries` into `NEONFS_PEER_PORTS`, keyed by node name.

  Accepts a map or an enumerable of `{node, port}`, where `node` is an atom or
  a `"name@host"` string. Entries whose port is not a positive integer are
  dropped — see "Port zero" above.

  Returns the entries now published, so a caller can log or assert on them.
  """
  @spec publish(Enumerable.t()) :: %{String.t() => pos_integer()}
  def publish(entries) do
    additions =
      entries
      |> Enum.flat_map(&normalise/1)
      |> Map.new()

    merged = Map.merge(current(), additions)

    if merged == %{} do
      merged
    else
      System.put_env(@env, encode(merged))
      merged
    end
  end

  @doc """
  The peer ports currently published, as `%{"name@host" => port}`.
  """
  @spec current() :: %{String.t() => pos_integer()}
  def current do
    case System.get_env(@env) do
      nil -> %{}
      value -> decode(value)
    end
  end

  @doc """
  Clears the variable. For tests — nothing in production unpublishes.
  """
  @spec reset() :: :ok
  def reset, do: System.delete_env(@env)

  defp normalise({node, port}) when is_atom(node), do: normalise({Atom.to_string(node), port})

  defp normalise({node, port}) when is_binary(node) and is_integer(port) and port > 0,
    do: [{node, port}]

  defp normalise(_entry), do: []

  # Sorted so the variable is stable across republishes of the same set —
  # otherwise every refresh looks like a change to anything watching it.
  defp encode(ports) do
    ports
    |> Enum.sort()
    |> Enum.map_join(",", fn {node, port} -> "#{node}:#{port}" end)
  end

  defp decode(value) do
    value
    |> String.split(",", trim: true)
    |> Enum.flat_map(&decode_entry/1)
    |> Map.new()
  end

  defp decode_entry(entry) do
    entry = String.trim(entry)

    with [name, host_and_port] <- String.split(entry, "@", parts: 2),
         {host, port} when port > 0 <- split_host_port(host_and_port) do
      [{name <> "@" <> host, port}]
    else
      _ ->
        Logger.debug("Ignoring unparseable #{@env} entry", entry: entry)
        []
    end
  end

  # Split on the last colon, so an IPv6 host keeps its own.
  defp split_host_port(string) do
    case :binary.matches(string, ":") do
      [] ->
        :error

      matches ->
        {pos, 1} = List.last(matches)
        host = binary_part(string, 0, pos)
        port = binary_part(string, pos + 1, byte_size(string) - pos - 1)

        case Integer.parse(port) do
          {port, ""} -> {host, port}
          _ -> :error
        end
    end
  end
end
