defmodule NeonFS.Client do
  @moduledoc """
  Top-level convenience API for NeonFS cluster client operations.

  Provides functions for service registration, discovery, and routing
  RPC calls to core nodes.
  """

  alias NeonFS.Client.{Discovery, Router, ServiceInfo}

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
  Deregisters this node from the service registry.
  """
  @spec deregister() :: :ok | {:error, term()}
  def deregister do
    case core_call(NeonFS.Core.ServiceRegistry, :deregister, [Node.self()]) do
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

  ## Examples

      NeonFS.Client.core_call(NeonFS.Core.VolumeRegistry, :get_by_name, ["my-volume"])
  """
  @spec core_call(module(), atom(), [term()]) :: term()
  def core_call(module, function, args) do
    Router.call(module, function, args)
  end
end
