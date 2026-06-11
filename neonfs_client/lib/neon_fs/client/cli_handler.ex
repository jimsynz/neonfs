defmodule NeonFS.Client.CLIHandler do
  @moduledoc """
  RPC target for CLI commands that must work on every node type.

  Present on all node types (core, FUSE, NFS, omnibus) since neonfs_client
  is a universal dependency. The CLI binary calls this module via Erlang
  distribution regardless of node type.
  """

  alias NeonFS.Client.{HealthCheck, Join, ServiceType}

  # Started OTP application → service type. A standalone interface node
  # runs exactly one of these; omnibus runs several, in which case core
  # wins (it layers Ra membership on the join).
  @app_types [
    neonfs_core: :core,
    neonfs_cifs: :cifs,
    neonfs_containerd: :containerd,
    neonfs_csi: :csi,
    neonfs_docker: :docker,
    neonfs_fuse: :fuse,
    neonfs_nfs: :nfs,
    neonfs_s3: :s3,
    neonfs_webdav: :webdav
  ]

  @doc """
  Returns serialised node health status for the CLI.
  """
  @spec handle_node_status() :: {:ok, map()}
  def handle_node_status do
    set_cli_metadata()

    HealthCheck.handle_node_status()
  end

  @doc """
  Joins this node to a cluster using an invite token (#1161).

  Mirrors `NeonFS.CLI.Handler.join_cluster/3` so `neonfs cluster join`
  works against any node type. When `type_str` is omitted, the service
  type is detected from the running applications; a core node's join is
  delegated to core's handler so Ra membership still happens.

  ## Returns
  - `{:ok, %{"status" => "joining", ...}}` — invite redeemed; the join
    completes asynchronously (validate via `cluster status`, #1033).
  - `{:error, reason}` on a synchronous failure.
  """
  @spec join_cluster(String.t(), String.t(), String.t() | nil) ::
          {:ok, map()} | {:error, term()}
  def join_cluster(token, via_address, type_str \\ nil)
      when is_binary(token) and is_binary(via_address) do
    set_cli_metadata()

    with {:ok, type} <- resolve_service_type(type_str) do
      do_join(token, via_address, type)
    end
  end

  # A core node's join must run core's finalize hook (Ra membership,
  # quorum-ring rebuild), which lives behind
  # `NeonFS.CLI.Handler.join_cluster/3` in neonfs_core. The module is
  # assembled at runtime so neonfs_client carries no compile dependency
  # on core.
  defp do_join(token, via_address, :core) do
    core_cli_handler = Module.concat([NeonFS, CLI, Handler])
    core_cli_handler.join_cluster(token, via_address, "core")
  end

  defp do_join(token, via_address, type) do
    case Join.join_cluster(token, via_address, type) do
      {:ok, :joining} ->
        {:ok,
         %{
           "status" => "joining",
           "via_address" => via_address,
           "node_name" => Atom.to_string(Node.self()),
           "type" => Atom.to_string(type)
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_service_type(nil), do: detect_service_type()

  defp resolve_service_type(type_str) when is_binary(type_str) do
    type = String.to_existing_atom(type_str)

    if type in ServiceType.all() do
      {:ok, type}
    else
      {:error, {:invalid_service_type, type_str}}
    end
  rescue
    ArgumentError -> {:error, {:invalid_service_type, type_str}}
  end

  defp detect_service_type do
    started = MapSet.new(Application.started_applications(), fn {app, _, _} -> app end)

    @app_types
    |> Enum.find(fn {app, _type} -> MapSet.member?(started, app) end)
    |> case do
      {_app, type} -> {:ok, type}
      nil -> {:error, :unknown_service_type}
    end
  end

  defp set_cli_metadata do
    Logger.metadata(
      component: :cli,
      request_id: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    )
  end
end
