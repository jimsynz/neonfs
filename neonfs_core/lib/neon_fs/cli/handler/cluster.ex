defmodule NeonFS.CLI.Handler.Cluster do
  @moduledoc """
  Cluster-lifecycle CLI command handlers: status, init, invite creation
  and join.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `cluster_status/0`, `cluster_init/3`, `create_invite/1`
  and `join_cluster/3` RPC entry points here, so the CLI wire contract is
  unchanged. The dangerous recovery operations (force-reset, remove-node,
  reconstruct-from-disk) live in `NeonFS.CLI.Handler.ClusterRecovery`.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Cluster.{Init, Invite, Join, State}
  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.Error.{Invalid, Unavailable}

  @doc """
  Returns cluster status information.

  ## Returns
  - `{:ok, map}` - Status map with cluster information
  """
  @spec cluster_status() :: {:ok, map()}
  def cluster_status do
    set_cli_metadata()

    if State.exists?() do
      {:ok,
       %{
         name: get_cluster_name(),
         node: Atom.to_string(Node.self()),
         status: :running,
         volumes: count_volumes(),
         uptime_seconds: get_uptime()
       }}
    else
      {:ok,
       %{
         name: nil,
         node: Atom.to_string(Node.self()),
         status: :not_initialised,
         volumes: 0,
         uptime_seconds: get_uptime()
       }}
    end
  end

  @doc """
  Initializes a new cluster with the given name.

  ## Parameters
  - `cluster_name` - Name for the new cluster (string)
  - `drive_config` (optional) - Map describing the first drive to register
    as part of bootstrap. Shape: `%{"path" => path, "tier" => "hot" |
    "warm" | "cold"}`. Without it the bootstrap falls back to drives
    registered via the `:neonfs_core, :drives` application environment;
    a freshly-installed daemon ships with none, so the CLI should always
    supply a drive.
  - `opts` (optional) - Map of bootstrap options:
    - `"system_replicas"` (positive integer, default `1`) - replication
      factor to seed the `_system` volume with. Use this on a cluster
      you intend to scale up so the system volume isn't stuck at
      `replicate:1` after the first node-join ratchet.

  ## Returns
  - `{:ok, map}` - Success map with cluster_id
  - `{:error, reason}` - Error tuple
  """
  @spec cluster_init(String.t(), map() | nil, map()) :: {:ok, map()} | {:error, term()}
  def cluster_init(cluster_name, drive_config \\ nil, opts \\ %{})
      when is_binary(cluster_name) and (is_map(drive_config) or is_nil(drive_config)) and
             is_map(opts) do
    set_cli_metadata()
    init_opts = cluster_init_opts(opts)

    cluster_name
    |> Init.init_cluster(drive_config, init_opts)
    |> format_cluster_init_result()
  end

  @doc """
  Creates an invite token for joining nodes.

  ## Parameters
  - `expires_in` - Duration in seconds the token is valid for

  ## Returns
  - `{:ok, %{"token" => string}}` - Success map with invite token
  - `{:error, reason}` - Error tuple
  """
  @spec create_invite(pos_integer()) :: {:ok, map()} | {:error, term()}
  def create_invite(expires_in) when is_integer(expires_in) and expires_in > 0 do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case Invite.create_invite(expires_in) do
        {:ok, token} ->
          {:ok, %{"token" => token}}

        {:error, :cluster_not_initialized} ->
          {:error, Unavailable.exception(message: "Cluster not initialised")}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Joins an existing cluster using an invite token.

  ## Parameters
  - `token` - Invite token from existing cluster
  - `via_node` - Node name of existing cluster member (string)
  - `type` - Service type string (e.g. "core", "fuse"). Defaults to "core".

  ## Returns
  - `{:ok, map}` - Success map with cluster information
  - `{:error, reason}` - Error tuple
  """
  @spec join_cluster(String.t(), String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def join_cluster(token, via_address, type_str \\ "core")
      when is_binary(token) and is_binary(via_address) and is_binary(type_str) do
    set_cli_metadata()
    type = String.to_existing_atom(type_str)

    case Join.join_cluster(token, via_address, type) do
      {:ok, :joining} ->
        # The join finishes asynchronously: the node restarts TLS distribution
        # (dropping this connection) and then connects + completes Ra membership.
        # The audit-log entry and quorum-ring rebuild now happen in that worker
        # once the node is connected. The CLI reconnects and validates via
        # `cluster status` (#1033).
        {:ok,
         %{
           "status" => "joining",
           "via_address" => via_address,
           "node_name" => Atom.to_string(Node.self())
         }}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  # Private

  defp cluster_init_opts(opts) do
    case Map.get(opts, "system_replicas") do
      n when is_integer(n) and n >= 1 -> [system_replicas: n]
      _ -> []
    end
  end

  defp format_cluster_init_result({:ok, cluster_id}) do
    case State.load() do
      {:ok, state} ->
        {:ok,
         %{
           cluster_id: cluster_id,
           cluster_name: state.cluster_name,
           node_id: state.this_node.id,
           node_name: Atom.to_string(state.this_node.name),
           created_at: DateTime.to_iso8601(state.created_at)
         }}

      {:error, _} ->
        {:ok, %{cluster_id: cluster_id}}
    end
  end

  defp format_cluster_init_result({:error, :already_initialised}),
    do: {:error, Invalid.exception(message: "Cluster already initialised")}

  defp format_cluster_init_result({:error, :no_drives_available}),
    do:
      {:error,
       Invalid.exception(
         message:
           "No drives available — pass `--drive <path>` to `neonfs cluster init` " <>
             "to designate the initial drive"
       )}

  defp format_cluster_init_result({:error, {:drive_preflight_failed, reason}}) do
    detail = if is_binary(reason), do: reason, else: inspect(reason)

    {:error,
     Invalid.exception(
       message:
         "Initial drive preflight failed: #{detail}. " <>
           "The cluster was not initialised — fix the drive and re-run " <>
           "`neonfs cluster init`."
     )}
  end

  defp format_cluster_init_result({:error, {:initial_drive_failed, reason}}) do
    {:error,
     Invalid.exception(
       message:
         "Ra cluster bootstrapped but the initial drive failed to register: " <>
           "#{inspect(reason)}. The cluster will report `running` from `neonfs cluster status` " <>
           "but has no drives or system volume yet — re-run `neonfs drive add <path>` to " <>
           "finish bootstrap. (#980)"
     )}
  end

  defp format_cluster_init_result({:error, reason}), do: {:error, wrap_error(reason)}

  defp get_cluster_name do
    # For Phase 1, use the node name as cluster name
    # Phase 2 will have proper cluster naming via Ra
    Node.self()
    |> Atom.to_string()
    |> String.split("@")
    |> List.first()
    |> Kernel.||("neonfs")
  end

  defp count_volumes do
    VolumeRegistry.list()
    |> length()
  end
end
