defmodule NeonFS.TestCase do
  @moduledoc """
  Test case helpers for NeonFS Core tests.

  Since `start_children?: false` is set in test config, tests must explicitly
  start the subsystems they need. This module provides helpers to do that.

  ## Usage

      use NeonFS.TestCase

      setup do
        start_core_subsystems()
        :ok
      end

  Or for more control:

      setup do
        start_persistence()
        start_blob_store()
        start_volume_registry()
        :ok
      end
  """

  use ExUnit.CaseTemplate

  alias NeonFS.Core.{MetadataRing, RaServer, RaSupervisor}

  using do
    quote do
      import NeonFS.TestCase
    end
  end

  @doc """
  Starts all core subsystems in the correct order.

  This starts: Persistence, BlobStore, ChunkIndex, FileIndex, VolumeRegistry.
  Does NOT start RaSupervisor - use `start_ra/0` separately if needed.
  """
  def start_core_subsystems do
    start_persistence()
    start_drive_registry()
    start_blob_store()
    start_chunk_index()
    start_file_index()
    start_volume_registry()
  end

  @doc """
  Starts Persistence with test configuration.
  """
  def start_persistence do
    meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/tmp/neonfs_test/meta")
    snapshot_interval_ms = Application.get_env(:neonfs_core, :snapshot_interval_ms, 100)

    start_supervised!(
      %{
        id: NeonFS.Core.Persistence,
        start:
          {NeonFS.Core.Persistence, :start_link,
           [[meta_dir: meta_dir, snapshot_interval_ms: snapshot_interval_ms]]},
        shutdown: 30_000
      },
      restart: :temporary
    )
  end

  @doc """
  Starts DriveRegistry with test configuration.
  """
  def start_drive_registry do
    drives =
      Application.get_env(:neonfs_core, :drives) ||
        [
          %{
            id: "default",
            path:
              Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs_test/blobs"),
            tier: :hot,
            capacity: 0
          }
        ]

    stop_if_running(NeonFS.Core.DriveRegistry)
    cleanup_ets_table(:drive_registry)

    start_supervised!(
      {NeonFS.Core.DriveRegistry, drives: drives, sync_interval_ms: 0},
      restart: :temporary
    )
  end

  @doc """
  Starts BlobStore with test configuration.
  """
  def start_blob_store do
    drives =
      Application.get_env(:neonfs_core, :drives) ||
        [
          %{
            id: "default",
            path:
              Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs_test/blobs"),
            tier: :hot,
            capacity: 0
          }
        ]

    prefix_depth = Application.get_env(:neonfs_core, :blob_store_prefix_depth, 2)

    start_supervised!(
      {NeonFS.Core.BlobStore, drives: drives, prefix_depth: prefix_depth},
      restart: :temporary
    )
  end

  @doc """
  Ensures ChunkAccessTracker is running and its ETS table exists.
  """
  def ensure_chunk_access_tracker do
    case GenServer.whereis(NeonFS.Core.ChunkAccessTracker) do
      nil ->
        start_supervised!(
          {NeonFS.Core.ChunkAccessTracker, decay_interval_ms: 0},
          restart: :temporary
        )

      _pid ->
        :ok
    end

    case :ets.whereis(:chunk_access_tracker) do
      :undefined ->
        :ets.new(:chunk_access_tracker, [:named_table, :set, :public])

      _ ->
        :ok
    end
  end

  @doc """
  Starts ChunkIndex.
  """
  def start_chunk_index do
    stop_if_running(NeonFS.Core.ChunkIndex)
    cleanup_ets_table(:chunk_index)
    start_supervised!(NeonFS.Core.ChunkIndex, restart: :temporary)
  end

  @doc """
  Starts FileIndex with mock quorum infrastructure.

  Automatically builds mock quorum opts when none are provided.

  ## Options

    * `:quorum_opts` — explicit quorum opts to use (overrides auto-built ones).
  """
  def start_file_index(opts \\ []) do
    opts =
      if Keyword.has_key?(opts, :quorum_opts) do
        opts
      else
        {quorum_opts, _store} = build_mock_quorum_opts()
        [quorum_opts: quorum_opts]
      end

    stop_if_running(NeonFS.Core.FileIndex)
    cleanup_ets_table(:file_index_by_id)

    start_supervised!(
      {NeonFS.Core.FileIndex, opts},
      restart: :temporary
    )
  end

  @doc """
  Builds mock quorum opts backed by a local ETS table.

  Returns `{quorum_opts, store}` where `store` is the ETS table reference.
  """
  def build_mock_quorum_opts do
    store = :ets.new(:test_quorum_store, [:set, :public])

    ring =
      MetadataRing.new([node()],
        virtual_nodes_per_physical: 4,
        replicas: 1
      )

    write_fn = fn _node, _segment, key, value ->
      :ets.insert(store, {key, value})
      :ok
    end

    read_fn = fn _node, _segment, key ->
      case :ets.lookup(store, key) do
        [{^key, value}] -> {:ok, value, {1_000_000, 0, node()}}
        [] -> {:error, :not_found}
      end
    end

    delete_fn = fn _node, _segment, key ->
      :ets.delete(store, key)
      :ok
    end

    quorum_opts = [
      ring: ring,
      write_fn: write_fn,
      read_fn: read_fn,
      delete_fn: delete_fn,
      quarantine_checker: fn _ -> false end,
      read_repair_fn: fn _work_fn, _opts -> {:ok, "noop"} end,
      local_node: node()
    ]

    {quorum_opts, store}
  end

  @doc """
  Starts StripeIndex with mock quorum infrastructure.
  """
  def start_stripe_index do
    {quorum_opts, _store} = build_mock_quorum_opts()
    stop_if_running(NeonFS.Core.StripeIndex)
    cleanup_ets_table(:stripe_index)

    start_supervised!(
      {NeonFS.Core.StripeIndex, quorum_opts: quorum_opts},
      restart: :temporary
    )
  end

  @doc """
  Starts ServiceRegistry.
  """
  def start_service_registry do
    stop_if_running(NeonFS.Core.ServiceRegistry)
    cleanup_ets_table(:services_by_node)
    cleanup_ets_table(:services_by_type)
    start_supervised!(NeonFS.Core.ServiceRegistry, restart: :temporary)
  end

  @doc """
  Starts VolumeRegistry.
  """
  def start_volume_registry do
    stop_if_running(NeonFS.Core.VolumeRegistry)
    cleanup_ets_table(:volumes_by_id)
    cleanup_ets_table(:volumes_by_name)
    start_supervised!(NeonFS.Core.VolumeRegistry, restart: :temporary)
  end

  @doc """
  Starts ACLManager.
  """
  def start_acl_manager do
    stop_if_running(NeonFS.Core.ACLManager)
    cleanup_ets_table(:volume_acls)
    start_supervised!(NeonFS.Core.ACLManager, restart: :temporary)
  end

  @doc """
  Starts AuditLog with test configuration.
  """
  def start_audit_log(opts \\ []) do
    stop_if_running(NeonFS.Core.AuditLog)
    cleanup_ets_table(:audit_log)

    opts = Keyword.merge([max_events: 1000, prune_interval_ms: 0], opts)
    start_supervised!({NeonFS.Core.AuditLog, opts}, restart: :temporary)
  end

  defp stop_if_running(name) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal, 5000)
    end

    Process.sleep(10)
  end

  defp cleanup_ets_table(table) do
    case :ets.whereis(table) do
      :undefined -> :ok
      ref -> :ets.delete(ref)
    end
  end

  @doc """
  Starts RaSupervisor (which includes RaServer).

  Requires a named Erlang node. Call `ensure_node_named/0` first if needed.
  """
  def start_ra do
    if Node.self() == :nonode@nohost do
      raise "Ra requires a named Erlang node. Call ensure_node_named/0 first."
    end

    # Stop any existing Ra supervisor and clean up state thoroughly
    stop_ra()

    # CRITICAL: Delete the Ra data directory BEFORE starting
    # Ra's :default system uses cwd/node_name as data directory
    delete_ra_data_directory()

    result = start_supervised!(RaSupervisor, restart: :temporary)

    # Reset Ra state AFTER starting to clear any accumulated data from
    # the global Ra :default system that persists across tests
    RaServer.reset!()

    result
  end

  @doc """
  Stops the Ra supervisor and resets Ra state.

  Use this in tests that need to ensure Ra is NOT running.
  """
  def stop_ra do
    reset_ra_server_if_running()
    stop_ra_supervisor_if_running()
    cleanup_ra_from_default_system()
    cleanup_ra_directories()
    Process.sleep(50)
    :ok
  end

  defp reset_ra_server_if_running do
    case Process.whereis(RaServer) do
      nil -> :ok
      _pid -> RaServer.reset!()
    end
  end

  defp stop_ra_supervisor_if_running do
    case Process.whereis(RaSupervisor) do
      nil -> :ok
      pid -> try do: Supervisor.stop(pid, :normal, 5000), catch: (:exit, _ -> :ok)
    end
  end

  defp cleanup_ra_from_default_system do
    server_id = {:neonfs_meta, Node.self()}
    try do: :ra.stop_server(:default, server_id), catch: (_, _ -> :ok)
    try do: :ra.force_delete_server(:default, server_id), catch: (_, _ -> :ok)
  end

  defp cleanup_ra_directories do
    case Application.get_env(:neonfs_core, :ra_data_dir) do
      nil -> :ok
      ra_data_dir -> File.rm_rf(ra_data_dir)
    end

    delete_ra_data_directory()
  end

  # Deletes Ra's actual data directory which is based on the node name.
  # Ra's :default system uses CWD/node_name as its data directory,
  # which is where WAL and segment files are stored.
  # This is critical for test isolation - cluster membership changes
  # are persisted in the WAL and recovered on restart.
  #
  # IMPORTANT: We must delete the ENTIRE directory, including the WAL file,
  # because the WAL contains cluster membership changes that are shared
  # across all servers in the Ra system.
  defp delete_ra_data_directory do
    node_name = Node.self() |> Atom.to_string()
    ra_node_dir = Path.join(File.cwd!(), node_name)

    # Delete the entire Ra data directory (including WAL, segments, and server data)
    # This ensures a completely clean state for the next test
    File.rm_rf!(ra_node_dir)
  end

  @doc """
  Ensures the Erlang node is named for Ra tests.

  Returns `:ok` if successful, raises if node naming fails.
  """
  def ensure_node_named do
    if Node.self() == :nonode@nohost do
      node_name = :"neonfs_test_#{System.system_time(:millisecond)}"

      case Node.start(node_name, name_domain: :shortnames) do
        {:ok, _pid} ->
          :ok

        {:error, reason} ->
          raise "Failed to start named node: #{inspect(reason)}"
      end
    else
      :ok
    end
  end

  @doc """
  Clears all ETS tables used by the core subsystems.

  Useful for test isolation when reusing subsystems across tests.
  """
  def clear_ets_tables do
    tables = [
      :volumes_by_id,
      :volumes_by_name,
      :volume_acls,
      :audit_log,
      :file_index_by_id,
      :chunk_index,
      :stripe_index
    ]

    for table <- tables do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end

    :ok
  end

  @doc """
  Sets up a temporary directory for test metadata and configures the application.

  Returns the tmp_dir path.
  """
  def setup_tmp_dir(context) do
    tmp_dir =
      context[:tmp_dir] || System.tmp_dir!() |> Path.join("neonfs_test_#{:rand.uniform(999_999)}")

    File.mkdir_p!(tmp_dir)

    Application.put_env(:neonfs_core, :meta_dir, tmp_dir)
    Application.put_env(:neonfs_core, :blob_store_base_dir, Path.join(tmp_dir, "blobs"))

    tmp_dir
  end

  @doc """
  Configures all application directories to use the given tmp_dir.

  This sets meta_dir, blob_store_base_dir, and ra_data_dir to subdirectories
  of the given tmp_dir, ensuring test isolation.
  """
  def configure_test_dirs(tmp_dir) do
    blob_dir = Path.join(tmp_dir, "blobs")
    Application.put_env(:neonfs_core, :meta_dir, tmp_dir)
    Application.put_env(:neonfs_core, :blob_store_base_dir, blob_dir)

    Application.put_env(:neonfs_core, :drives, [
      %{id: "default", path: blob_dir, tier: :hot, capacity: 0}
    ])

    Application.put_env(:neonfs_core, :ra_data_dir, Path.join(tmp_dir, "ra"))
    :ok
  end

  @doc """
  Writes a cluster.json file with a master key for encryption tests.
  """
  def write_cluster_json(dir, master_key) do
    alias NeonFS.Cluster.State, as: ClusterState

    cluster_state =
      ClusterState.new(
        "test-cluster-#{System.unique_integer([:positive])}",
        "test-cluster",
        master_key,
        %{id: "node-1", name: node(), joined_at: DateTime.utc_now()}
      )

    Application.put_env(:neonfs_core, :meta_dir, dir)
    :ok = ClusterState.save(cluster_state)
  end

  @doc """
  Cleans up application directory configuration.
  """
  def cleanup_test_dirs do
    Application.delete_env(:neonfs_core, :meta_dir)
    Application.delete_env(:neonfs_core, :blob_store_base_dir)
    Application.delete_env(:neonfs_core, :drives)
    Application.delete_env(:neonfs_core, :ra_data_dir)
    :ok
  end
end
