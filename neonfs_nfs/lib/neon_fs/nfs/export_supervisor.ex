defmodule NeonFS.NFS.ExportSupervisor do
  @moduledoc """
  DynamicSupervisor for NFS handler and cache processes.

  Unlike FUSE which has one handler per mount, NFS uses a single handler
  for all exported volumes. This supervisor manages that handler and
  any per-volume cache processes added later.
  """

  use DynamicSupervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Start the NFS handler under supervision.
  """
  @spec start_handler(keyword()) :: DynamicSupervisor.on_start_child()
  def start_handler(handler_opts) do
    child_spec = {NeonFS.NFS.Handler, handler_opts}
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  @doc """
  Stop the handler process.
  """
  @spec stop_handler(pid()) :: :ok | {:error, :not_found}
  def stop_handler(handler_pid) when is_pid(handler_pid) do
    DynamicSupervisor.terminate_child(__MODULE__, handler_pid)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 3, max_seconds: 5)
  end
end
