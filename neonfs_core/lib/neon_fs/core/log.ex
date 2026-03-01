defmodule NeonFS.Core.Log do
  @moduledoc """
  Structured logging helpers for NeonFS.

  Provides convenience functions for setting Logger metadata fields
  used by the JSON formatter in production. In dev/test the metadata
  is still attached to log lines but rendered by the console formatter.

  ## Standard metadata fields

    * `:component`  — logical subsystem (e.g. `"cluster.join"`, `"fuse.handler"`)
    * `:node_name`  — the Erlang node name
    * `:volume_id`  — the volume being operated on (when applicable)
    * `:request_id` — a unique ID for request tracing

  ## Usage

      NeonFS.Core.Log.with_metadata([component: "cluster.join", volume_id: vol_id], fn ->
        Logger.info("Joined cluster")
      end)

  """

  require Logger

  @type metadata :: keyword()

  @doc """
  Executes `fun` with the given Logger metadata merged in, then restores
  the previous metadata afterwards.
  """
  @spec with_metadata(metadata(), (-> result)) :: result when result: var
  def with_metadata(metadata, fun) when is_list(metadata) and is_function(fun, 0) do
    previous = Logger.metadata()
    Logger.metadata(metadata)

    try do
      fun.()
    after
      Logger.metadata(previous)
    end
  end

  @doc """
  Sets the `:component` metadata field for the current process.
  """
  @spec set_component(String.t()) :: :ok
  def set_component(component) when is_binary(component) do
    Logger.metadata(component: component)
  end

  @doc """
  Sets the `:volume_id` metadata field for the current process.
  """
  @spec set_volume(String.t()) :: :ok
  def set_volume(volume_id) when is_binary(volume_id) do
    Logger.metadata(volume_id: volume_id)
  end

  @doc """
  Sets the `:node_name` metadata field to the current node.
  """
  @spec set_node_name :: :ok
  def set_node_name do
    Logger.metadata(node_name: node())
  end

  @doc """
  Sets the `:request_id` metadata field to a new unique value.

  Returns the generated request ID.
  """
  @spec set_request_id :: String.t()
  def set_request_id do
    request_id = generate_request_id()
    Logger.metadata(request_id: request_id)
    request_id
  end

  @doc """
  Sets the `:request_id` metadata field to the given value.
  """
  @spec set_request_id(String.t()) :: :ok
  def set_request_id(request_id) when is_binary(request_id) do
    Logger.metadata(request_id: request_id)
  end

  defp generate_request_id do
    Base.hex_encode32(:crypto.strong_rand_bytes(10), case: :lower, padding: false)
  end
end
