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

  alias NeonFS.CIFS.{ConnectionHandler, Listener}
  alias NeonFS.Client.Registrar

  @default_socket_path "/run/neonfs/cifs.sock"

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    register? = Application.get_env(:neonfs_cifs, :register_service, true)

    children =
      [listener_child_spec()]
      |> maybe_add_registrar(register?)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp listener_child_spec do
    case Application.get_env(:neonfs_cifs, :listener, :socket) do
      :socket ->
        socket_path = Application.get_env(:neonfs_cifs, :socket_path, @default_socket_path)
        Listener.child_spec(socket_path: socket_path, handler: ConnectionHandler)

      {:tcp, port} ->
        Listener.child_spec(tcp_port: port, handler: ConnectionHandler)
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
