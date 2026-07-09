defmodule NeonFS.Client do
  @moduledoc """
  Top-level convenience API for NeonFS cluster client operations.

  Provides functions for service registration, discovery, and routing
  RPC calls to core nodes.
  """

  alias NeonFS.Client.{Discovery, Router, ServiceInfo}

  # `NeonFS.Core` facade operations that mutate a volume's per-volume metadata
  # tree (and so resolve + update its root segment). Each takes the volume name
  # as its first argument, so `core_call/3` routes them to a node that holds the
  # root — turning the #1045 remote re-dispatch into the rare case instead of
  # paying it on every write (#1046 / #1076). Reads and volume-lifecycle ops
  # (create/delete_volume) stay cost-based.
  @core_metadata_writes ~w(
    commit_chunks
    delete_file
    mkdir
    rename_file
    sync_file
    sync_file_by_id
    truncate_file
    update_file_meta
    write_file_at
    write_file_at_by_id
    write_file_streamed
  )a

  @doc """
  Registers this node as a service of the given type.

  Makes an RPC call to `NeonFS.Core.ServiceRegistry.register/1` on a
  connected core node.

  ## Parameters
  - `type` - Service type (`:core`, `:fuse`, `:s3`, etc.)
  - `metadata` - Optional metadata map describing capabilities
  """
  @spec register(NeonFS.Client.ServiceType.t(), map()) :: :ok | {:error, term()}
  def register(type, metadata \\ %{}) do
    info = ServiceInfo.new(Node.self(), type, metadata: metadata)

    case core_call(NeonFS.Core.ServiceRegistry, :register, [info]) do
      :ok -> :ok
      {:error, _} = error -> error
      other -> {:error, {:unexpected, other}}
    end
  end

  @doc """
  Deregisters this node's service from the service registry.
  """
  @spec deregister(NeonFS.Client.ServiceType.t()) :: :ok | {:error, term()}
  def deregister(type) do
    case core_call(NeonFS.Core.ServiceRegistry, :deregister, [Node.self(), type]) do
      :ok -> :ok
      {:error, _} = error -> error
      other -> {:error, {:unexpected, other}}
    end
  end

  @doc """
  Interface-agnostic `fsync`/`sync`/COMMIT durability barrier: blocks until
  every chunk of the file at `path` on `volume_name` has at least the
  volume's `min_copies` durable replicas (#1500 / #1502).

  Every interface (FUSE fsync/flush, NFS COMMIT, CIFS fsync) calls this one
  entry point so the barrier has identical semantics everywhere. RPCs to
  `NeonFS.Core.sync_file/2` via the root-holder route.
  """
  @spec sync_file(String.t(), String.t()) :: :ok | {:error, term()}
  def sync_file(volume_name, path) do
    core_call(NeonFS.Core, :sync_file, [volume_name, path])
  end

  @doc """
  `file_id`-keyed counterpart to `sync_file/2` for handle-based callers
  (FUSE / NFSv4 fd holders) whose file may have been detached by another
  peer. RPCs to `NeonFS.Core.sync_file_by_id/2`.
  """
  @spec sync_file_by_id(String.t(), binary()) :: :ok | {:error, term()}
  def sync_file_by_id(volume_name, file_id) do
    core_call(NeonFS.Core, :sync_file_by_id, [volume_name, file_id])
  end

  @doc """
  Lists services of the given type.

  Returns cached results from local discovery, or queries core directly.
  """
  @spec list_services(NeonFS.Client.ServiceType.t()) :: [ServiceInfo.t()]
  def list_services(type) do
    Discovery.list_by_type(type)
  end

  @doc """
  Routes an RPC call to the best available core node.

  Volume-scoped `NeonFS.Core` metadata writes (see `@core_metadata_writes`) are
  dispatched to a node holding the volume's root segment via
  `Router.volume_metadata_call/4`; everything else uses cost-based routing.
  Routing is by volume name (the write op's first argument) and falls back to
  cost-based routing if the volume can't be resolved, so passing anything other
  than a known volume name is safe.

  ## Examples

      NeonFS.Client.core_call(NeonFS.Core.VolumeRegistry, :get_by_name, ["my-volume"])
  """
  @spec core_call(module(), atom(), [term()]) :: term()
  def core_call(NeonFS.Core, function, [volume_name | _] = args)
      when function in @core_metadata_writes do
    Router.volume_metadata_call(volume_name, NeonFS.Core, function, args)
  end

  def core_call(module, function, args) do
    Router.call(module, function, args)
  end

  @doc """
  Routes a volume-scoped metadata *write* through an id-keyed core API
  (e.g. `NeonFS.Core.WriteOperation` / `NeonFS.Core.FileIndex`) to a node
  holding the volume's root segment, resolved by `volume_id` (#1087).

  For callers that hold the volume id rather than its name. Falls back to
  cost-based routing if the volume can't be resolved.
  """
  @spec write_call_by_id(String.t(), module(), atom(), [term()]) :: term()
  def write_call_by_id(volume_id, module, function, args) do
    Router.volume_metadata_call_by_id(volume_id, module, function, args)
  end
end
