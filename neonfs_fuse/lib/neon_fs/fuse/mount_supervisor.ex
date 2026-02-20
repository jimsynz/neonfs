defmodule NeonFS.FUSE.MountSupervisor do
  @moduledoc """
  DynamicSupervisor for FUSE mount handler processes.

  Each mount gets its own supervised Handler process started under this
  supervisor. When a mount is created, the handler is added dynamically.
  When a mount is removed or fails, the handler is stopped and removed.

  ## Isolation

  Using a DynamicSupervisor ensures mount failures are isolated:
  - One mount failing doesn't affect other mounts
  - Handlers can be added/removed at runtime
  - Supervisor handles cleanup automatically
  """

  use DynamicSupervisor

  @doc """
  Start the mount supervisor.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Start a handler process for a mount under supervision.

  Returns `{:ok, pid}` on success, `{:error, reason}` on failure.
  """
  @spec start_handler(keyword()) :: DynamicSupervisor.on_start_child()
  def start_handler(handler_opts) do
    child_spec = {NeonFS.FUSE.Handler, handler_opts}
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  @doc """
  Start a MetadataCache process for a mount under supervision.

  Returns `{:ok, pid}` on success, `{:error, reason}` on failure.
  """
  @spec start_cache(keyword()) :: DynamicSupervisor.on_start_child()
  def start_cache(cache_opts) do
    child_spec = {NeonFS.FUSE.MetadataCache, cache_opts}
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  @doc """
  Stop a handler process.

  Returns `:ok` on success, `{:error, :not_found}` if not found.
  """
  @spec stop_handler(pid()) :: :ok | {:error, :not_found}
  def stop_handler(handler_pid) when is_pid(handler_pid) do
    DynamicSupervisor.terminate_child(__MODULE__, handler_pid)
  end

  @doc """
  Stop a cache process.

  Returns `:ok` on success, `{:error, :not_found}` if not found.
  """
  @spec stop_cache(pid()) :: :ok | {:error, :not_found}
  def stop_cache(cache_pid) when is_pid(cache_pid) do
    DynamicSupervisor.terminate_child(__MODULE__, cache_pid)
  end

  @impl true
  def init(_opts) do
    # :one_for_one strategy: each child (handler) restarts independently
    # :temporary restart: handlers don't restart on failure (mount cleanup handles this)
    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 3, max_seconds: 5)
  end
end
