defmodule NeonFS.CIFS.Supervisor do
  @moduledoc """
  Top-level supervisor for `neonfs_cifs`.

  Owns the ThousandIsland UDS listener that accepts connections from
  the Samba `vfs_neonfs.so` C shim and (optionally) the registrar
  process that advertises this node as a `:cifs` service in the
  cluster's service registry.

  The default UDS path is `/run/neonfs/cifs.sock`; override via
  `Application.put_env(:neonfs_cifs, :socket_path, ...)` (or pass
  `:listener` as `{:tcp, port}` for tests that want a TCP listener
  without UDS plumbing).
  """

  use Supervisor
  require Logger

  alias NeonFS.CIFS.{ConnectionHandler, Listener}
  alias NeonFS.Client.Registrar

  @default_socket_path "/run/neonfs/cifs.sock"

  # Errors that mean "the host can't host the plugin socket" rather
  # than a misconfiguration we should crash on. With the default
  # socket path under `RuntimeDirectory=neonfs` these are unusual,
  # but they can surface if `:socket_path` is overridden to
  # somewhere the daemon can't reach.
  @skip_errors [:eacces, :enoent, :enotdir, :erofs]

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    register? = Application.get_env(:neonfs_cifs, :register_service, true)

    children =
      case listener_child_spec() do
        {:ok, listener} ->
          [listener] |> maybe_add_registrar(register?)

        {:skip, message} ->
          Logger.warning("CIFS listener disabled: #{message}")
          []
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp listener_child_spec do
    case Application.get_env(:neonfs_cifs, :listener, :socket) do
      :socket ->
        socket_path = Application.get_env(:neonfs_cifs, :socket_path, @default_socket_path)

        case prepare_socket(socket_path) do
          :ok ->
            {:ok, Listener.child_spec(socket_path: socket_path, handler: ConnectionHandler)}

          {:skip, _} = skip ->
            skip
        end

      {:tcp, port} ->
        {:ok, Listener.child_spec(tcp_port: port, handler: ConnectionHandler)}
    end
  end

  defp prepare_socket(socket_path) do
    socket_dir = Path.dirname(socket_path)

    case File.mkdir_p(socket_dir) do
      :ok ->
        File.rm(socket_path)
        :ok

      {:error, reason} when reason in @skip_errors ->
        {:skip,
         "cannot prepare socket directory #{inspect(socket_dir)} (#{reason}). " <>
           "Check `:socket_path` and the daemon's filesystem permissions."}
    end
  end

  defp maybe_add_registrar(children, false), do: children

  defp maybe_add_registrar(children, true) do
    children ++
      [
        {Registrar,
         metadata: registration_metadata(), type: :cifs, name: NeonFS.Client.Registrar.CIFS}
      ]
  end

  defp registration_metadata do
    %{
      capabilities: [:samba_vfs],
      version: to_string(Application.spec(:neonfs_cifs, :vsn) || "0.0.0")
    }
  end
end
