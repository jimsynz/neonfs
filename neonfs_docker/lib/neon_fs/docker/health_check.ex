defmodule NeonFS.Docker.HealthCheck do
  @moduledoc """
  Docker plugin-specific health checks registered with the universal
  `NeonFS.Client.HealthCheck` framework.

  Each check returns a map with `:status` (`:healthy | :degraded |
  :unhealthy`) plus optional metadata. The framework aggregates the
  worst status across every registered check.

  Registered checks:

    * `:cluster` — degraded when no core nodes are discoverable
      (quorum unreachable from this plugin's perspective). Mirrors
      the pattern in `NeonFS.S3.HealthCheck.cluster_status/0`.
    * `:registrar` — unhealthy when this plugin's `Registrar` isn't
      registered with the cluster's service registry yet.
    * `:volume_store` — unhealthy when the local volume store
      GenServer isn't running.
    * `:mount_tracker` — unhealthy when the FUSE mount-tracker
      GenServer isn't running.
  """

  alias NeonFS.Client.{Discovery, HealthCheck}
  alias NeonFS.Docker.{MountTracker, VolumeStore}

  @doc """
  Registers Docker-plugin health checks with the universal framework.
  """
  @spec register_checks() :: :ok
  def register_checks do
    HealthCheck.register(:docker,
      cluster: &check_cluster/0,
      registrar: &check_registrar/0,
      volume_store: &check_volume_store/0,
      mount_tracker: &check_mount_tracker/0
    )
  end

  @doc """
  Returns the current cluster status for the plugin's perspective —
  whether *any* core node is currently discoverable.

  Used by `register_checks/0`'s `:cluster` entry and exposed
  publicly so an operator-side probe can inspect richer fields than
  the boolean health/unhealthy collapse the universal framework
  surfaces.
  """
  @spec cluster_status() :: %{
          status: :ok | :degraded | :unavailable,
          writable: boolean(),
          readable: boolean(),
          reason: String.t() | nil,
          quorum_reachable: boolean()
        }
  def cluster_status do
    cluster_status(core_nodes_fn())
  end

  @doc """
  Same shape as `cluster_status/0`, parameterised on the core-node
  discovery function. Tests inject a closure to drive the degraded
  arm without bringing up a real cluster.
  """
  @spec cluster_status((-> [node()])) :: %{
          status: :ok | :degraded | :unavailable,
          writable: boolean(),
          readable: boolean(),
          reason: String.t() | nil,
          quorum_reachable: boolean()
        }
  def cluster_status(core_nodes_fn) when is_function(core_nodes_fn, 0) do
    case core_nodes_fn.() do
      nodes when is_list(nodes) and nodes != [] ->
        %{
          status: :ok,
          writable: true,
          readable: true,
          reason: nil,
          quorum_reachable: true
        }

      _ ->
        %{
          status: :unavailable,
          writable: false,
          readable: false,
          reason: "no-core-nodes",
          quorum_reachable: false
        }
    end
  end

  defp core_nodes_fn do
    case Application.get_env(:neonfs_docker, :core_nodes_fn) do
      nil -> &Discovery.get_core_nodes/0
      fun when is_function(fun, 0) -> fun
    end
  end

  defp check_cluster do
    # `cluster_status/0` returns either `:ok` (some core node is
    # discoverable) or `:unavailable` (none are). Map onto the
    # framework's `:healthy | :unhealthy` collapse. A future probe
    # that distinguishes "core reachable but quorum lost" can grow
    # `:degraded` here — the universal framework already encodes it.
    case cluster_status() do
      %{status: :ok} ->
        %{status: :healthy}

      %{status: :unavailable, reason: reason} ->
        %{status: :unhealthy, reason: reason}
    end
  end

  defp check_registrar do
    case Process.whereis(NeonFS.Client.Registrar.Docker) do
      nil ->
        %{status: :unhealthy, reason: :not_running}

      pid ->
        state = :sys.get_state(pid)

        if state.registered? do
          %{status: :healthy}
        else
          %{status: :degraded, reason: :not_registered}
        end
    end
  rescue
    _ -> %{status: :unhealthy, reason: :not_available}
  end

  defp check_volume_store do
    if Process.whereis(VolumeStore) do
      volumes = VolumeStore.list()
      %{status: :healthy, volume_count: length(volumes)}
    else
      %{status: :unhealthy, reason: :not_running}
    end
  rescue
    _ -> %{status: :unhealthy, reason: :not_available}
  end

  defp check_mount_tracker do
    if Process.whereis(MountTracker) do
      mounts = MountTracker.list()
      %{status: :healthy, mount_count: length(mounts)}
    else
      %{status: :unhealthy, reason: :not_running}
    end
  rescue
    _ -> %{status: :unhealthy, reason: :not_available}
  end
end
