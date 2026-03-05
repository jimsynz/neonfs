defmodule NeonFS.NFS.Application do
  @moduledoc """
  OTP Application for neonfs_nfs.

  Starts the NFS supervision tree and handles graceful shutdown.
  """

  use Application
  require Logger

  alias NeonFS.NFS.{ExportManager, Supervisor}

  @impl true
  def start(_type, _args) do
    Logger.metadata(node_name: node())

    # In test mode, don't start the supervisor - tests use start_supervised
    # for the specific components they need
    result =
      if Application.get_env(:neonfs_nfs, :start_supervisor, true) do
        Supervisor.start_link()
      else
        # Return a minimal supervisor for test mode
        Elixir.Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
      end

    # Register with the cluster after supervisor is running
    # (Connection GenServer must be started first)
    case result do
      {:ok, _} -> register_with_cluster()
      _ -> :ok
    end

    result
  end

  @impl true
  def stop(_state) do
    # Graceful shutdown: unexport all volumes
    case Process.whereis(ExportManager) do
      nil ->
        :ok

      _pid ->
        Enum.each(ExportManager.list_exports(), fn export ->
          Logger.debug("Unexporting volume before shutdown", volume_name: export.volume_name)
          ExportManager.unexport(export.id)
        end)
    end

    :ok
  end

  defp register_with_cluster do
    # Async registration — don't block startup if core is unreachable.
    # Delay gives Connection time to establish bootstrap connections.
    Task.start(fn ->
      Process.send_after(self(), :register, 500)

      receive do
        :register ->
          case NeonFS.Client.register(:nfs, %{
                 capabilities: [:export, :read, :write],
                 version: to_string(Application.spec(:neonfs_nfs, :vsn) || "0.0.0")
               }) do
            :ok ->
              Logger.info("Registered as NFS service with cluster")

            {:error, reason} ->
              Logger.warning("Failed to register with cluster", reason: inspect(reason))
          end
      end
    end)
  end
end
