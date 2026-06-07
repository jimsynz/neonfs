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
end
