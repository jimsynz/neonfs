defmodule NeonFS.Systemd do
  @moduledoc """
  Systemd readiness notification for NeonFS services.

  When the `NOTIFY_SOCKET` environment variable is set (indicating a
  `Type=notify` systemd service), sends `READY=1` via Unix datagram socket.
  """

  require Logger

  @doc "Sends `READY=1` to systemd if `NOTIFY_SOCKET` is set, otherwise no-op."
  @spec notify_ready() :: :ok
  def notify_ready do
    case System.get_env("NOTIFY_SOCKET") do
      nil -> :ok
      socket_path -> send_ready(socket_path)
    end
  end

  defp send_ready(socket_path) do
    path = normalize_socket_path(socket_path)

    with {:ok, sock} <- :socket.open(:local, :dgram, :default),
         :ok <- :socket.sendto(sock, "READY=1", %{family: :local, path: path}) do
      :socket.close(sock)
      Logger.info("Sent READY=1 to systemd")
      :ok
    else
      {:error, reason} ->
        Logger.warning("Failed to send sd_notify READY=1", reason: inspect(reason))
        :ok
    end
  end

  defp normalize_socket_path("@" <> rest), do: <<0>> <> rest
  defp normalize_socket_path(path), do: path
end
