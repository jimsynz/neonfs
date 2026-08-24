defmodule NeonFS.CLI.Handler.BlockRouting do
  @moduledoc """
  CLI command handlers that route to block interface nodes: attaching a
  device, detaching it, listing what is attached, and reporting which
  frontends each node can serve.

  ## Attaching is not symmetrical, and this does not pretend otherwise

  A ublk device is created by the kernel of the host running the block
  target, so attaching over ublk is something a block node does and the
  device appears *there*. NBD is the other way round: the client runs
  `nbd-client`, and the device appears wherever that ran. This module can
  therefore only perform the first kind.

  So `attach/2` reports rather than attaches when the frontend resolves to
  NBD, and says where to dial. Refusing outright would be worse — the
  operator's next question is exactly the endpoint — and silently doing
  nothing while returning success would be worse still. An operator or
  script that needs a device path asks for `:ublk` by name, which fails
  saying which check failed rather than answering something else.

  ## Node targeting

  A named node wins. Otherwise the first block node that can serve the
  requested frontend is chosen, preferring the local one: a ublk device is
  only usable on its own host, so "here" is the right default for the same
  reason it is for a FUSE mount.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.ServiceRegistry
  alias NeonFS.Error.{Invalid, Unavailable}

  # A ublk attach waits for the kernel to publish the device, which the
  # target bounds at 30s; this has to outlast that or a slow attach looks
  # like a dead node.
  @attach_rpc_timeout 60_000
  @detach_rpc_timeout 30_000
  @query_rpc_timeout 15_000

  @frontends ~w(auto ublk nbd)

  @doc """
  Attaches `export` as a block device on a block node.

  `export` is `<volume>` or `<volume>:<path>`. `frontend` is `"auto"`,
  `"ublk"` or `"nbd"`.

  Answers `{:ok, map}` where `:frontend` says what happened: `:ublk` carries
  a `:device_path` on the `:node` named, `:nbd` carries an `:endpoint` and
  the reason ublk was not used, and nothing was attached.
  """
  @spec attach(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def attach(export, frontend) when is_binary(export) and is_binary(frontend) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, preference} <- parse_frontend(frontend),
         {:ok, node} <- block_node(),
         {:ok, resolved} <- resolve(node, preference) do
      attach_resolved(node, export, resolved)
    end
  end

  @doc """
  Detaches the ublk device serving `export`, wherever this cluster has one.

  Idempotent: an export nothing has attached is `:ok`, since that is the
  state the caller asked for. NBD is not detachable from here — the device
  belongs to whichever host ran `nbd-client`.
  """
  @spec detach(String.t()) :: {:ok, map()} | {:error, term()}
  def detach(export) when is_binary(export) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, nodes} <- block_nodes() do
      detached =
        nodes
        |> Enum.filter(&attached_here?(&1, export))
        |> Enum.map(&detach_on(&1, export))

      {:ok, %{export: export, detached: detached}}
    end
  end

  @doc """
  Every block device this cluster has attached, by node and frontend.

  Both routes are reported: ublk targets this cluster owns, and the NBD
  connections its listeners are serving. An operator looking for "what is
  using this volume" wants both, and single-attach means seeing two is
  itself the answer to a different question.
  """
  @spec list_devices() :: {:ok, [map()]} | {:error, term()}
  def list_devices do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, nodes} <- block_nodes() do
      {:ok, nodes |> Enum.flat_map(&devices_on/1) |> Enum.sort_by(&{&1.node, &1.export})}
    end
  end

  @doc """
  What each block node can serve, and why not when it cannot serve ublk.

  The reason is the useful part: "unavailable" sends an operator to
  `modprobe` for a problem that may be a release assembled without its
  native helper.
  """
  @spec frontends() :: {:ok, [map()]} | {:error, term()}
  def frontends do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, nodes} <- block_nodes() do
      {:ok, Enum.sort_by(Enum.map(nodes, &frontends_on/1), & &1.node)}
    end
  end

  defp parse_frontend(frontend) when frontend in @frontends,
    do: {:ok, String.to_existing_atom(frontend)}

  defp parse_frontend(other) do
    {:error,
     wrap_error(
       Invalid.exception(
         message:
           "unknown frontend #{inspect(other)}; expected one of #{Enum.join(@frontends, ", ")}"
       )
     )}
  end

  defp resolve(node, preference) do
    case block_rpc(node, NeonFS.Block, :select, [preference], @query_rpc_timeout) do
      {:ok, resolved} ->
        {:ok, resolved}

      {:error, _reason} = error ->
        error

      other ->
        {:error, wrap_error(Unavailable.exception(message: "block node said #{inspect(other)}"))}
    end
  end

  defp attach_resolved(node, export, :ublk) do
    case block_rpc(node, NeonFS.Block, :attach_ublk, [export, []], @attach_rpc_timeout) do
      {:ok, device_path} ->
        {:ok, %{export: export, frontend: :ublk, node: node, device_path: device_path}}

      {:error, _reason} = error ->
        error
    end
  end

  # Nothing is attached here, and the answer says so rather than implying a
  # device exists somewhere.
  defp attach_resolved(node, export, :nbd) do
    with {:ok, endpoint} <- nbd_endpoint(node) do
      {:ok,
       %{
         export: export,
         frontend: :nbd,
         node: node,
         endpoint: endpoint,
         attached: false,
         reason: ublk_unavailable_reason(node)
       }}
    end
  end

  defp nbd_endpoint(node) do
    case service_metadata(node) do
      %{nbd_endpoint: {host, port}} ->
        {:ok, %{host: to_string(host), port: port}}

      _absent ->
        {:error,
         wrap_error(
           Unavailable.exception(message: "block node #{node} advertises no NBD endpoint")
         )}
    end
  end

  defp ublk_unavailable_reason(node) do
    case block_rpc(node, NeonFS.Block.Ublk.Capability, :check, [], @query_rpc_timeout) do
      :ok -> nil
      {:error, reason} -> inspect(reason)
      _unreachable -> nil
    end
  end

  defp attached_here?(node, export) do
    case block_rpc(node, NeonFS.Block.Ublk.Supervisor, :attached, [], @query_rpc_timeout) do
      attached when is_list(attached) -> export in attached
      _unreachable -> false
    end
  end

  defp detach_on(node, export) do
    case block_rpc(node, NeonFS.Block, :detach_ublk, [export], @detach_rpc_timeout) do
      :ok -> %{node: node, frontend: :ublk, detached: true}
      {:error, reason} -> %{node: node, frontend: :ublk, detached: false, reason: inspect(reason)}
    end
  end

  defp devices_on(node) do
    ublk_devices(node) ++ nbd_devices(node)
  end

  defp ublk_devices(node) do
    case block_rpc(node, NeonFS.Block.Ublk.Supervisor, :attached, [], @query_rpc_timeout) do
      attached when is_list(attached) ->
        Enum.map(attached, &%{node: node, export: &1, frontend: :ublk})

      _unreachable ->
        []
    end
  end

  # `attached/0` counts holders per export, which for NBD is connections.
  defp nbd_devices(node) do
    case block_rpc(node, NeonFS.Block.DeviceRegistry, :attached, [], @query_rpc_timeout) do
      attached when is_map(attached) ->
        Enum.map(attached, fn {export, holders} ->
          %{node: node, export: export, frontend: :nbd, holders: holders}
        end)

      _unreachable ->
        []
    end
  end

  defp frontends_on(node) do
    case block_rpc(node, NeonFS.Block, :frontends, [], @query_rpc_timeout) do
      frontends when is_list(frontends) ->
        %{node: node, frontends: frontends, ublk_unavailable: ublk_unavailable_reason(node)}

      _unreachable ->
        %{node: node, frontends: [], ublk_unavailable: "node unreachable"}
    end
  end

  defp block_node do
    with {:ok, nodes} <- block_nodes() do
      {:ok, Enum.find(nodes, List.first(nodes), &(&1 == Node.self()))}
    end
  end

  defp block_nodes do
    case registered_block_nodes() ++ connected_block_nodes() do
      [] -> {:error, wrap_error(Unavailable.exception(message: "no block service available"))}
      nodes -> {:ok, Enum.uniq(nodes)}
    end
  end

  defp registered_block_nodes do
    :block |> ServiceRegistry.list_by_type() |> Enum.map(& &1.node)
  rescue
    ArgumentError -> []
  end

  defp connected_block_nodes do
    Enum.filter([Node.self() | Node.list()], &block_node?/1)
  end

  defp block_node?(node) do
    name = Atom.to_string(node)
    String.starts_with?(name, "neonfs_block@") or String.starts_with?(name, "neonfs@")
  end

  defp service_metadata(node) do
    :block
    |> ServiceRegistry.list_by_type()
    |> Enum.find_value(%{}, fn service ->
      if service.node == node, do: service.metadata || %{}
    end)
  rescue
    ArgumentError -> %{}
  end

  # Single dispatch point, with the RPC module read from app env so a test
  # can assert which node a command was routed to without a second node.
  defp block_rpc(node, module, fun, args, timeout) do
    rpc_mod = Application.get_env(:neonfs_core, :block_rpc_mod, :rpc)

    case rpc_mod.call(node, module, fun, args, timeout) do
      {:badrpc, reason} ->
        {:error,
         wrap_error(Unavailable.exception(message: "block RPC failed: #{inspect(reason)}"))}

      result ->
        result
    end
  end
end
