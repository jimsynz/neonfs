defmodule NeonFS.Core.RaTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{MetadataStateMachine, RaServer, RaSupervisor}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    # Start Ra supervisor (which includes RaServer)
    start_ra()

    # Initialize Ra cluster
    :ok = RaServer.init_cluster()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  describe "MetadataStateMachine" do
    test "init/1 returns empty v5 state" do
      state = MetadataStateMachine.init(%{})
      assert state.data == %{}
      assert state.chunks == %{}
      assert state.files == %{}
      assert state.services == %{}
      assert state.volumes == %{}
      assert state.stripes == %{}
      assert state.segment_assignments == %{}
      assert state.intents == %{}
      assert state.active_intents_by_conflict_key == %{}
      assert state.version == 0
    end

    test "apply/3 handles put command" do
      state = MetadataStateMachine.init(%{})
      {new_state, result, _effects} = MetadataStateMachine.apply(%{}, {:put, :foo, :bar}, state)

      assert new_state.data == %{foo: :bar}
      assert new_state.version == 1
      assert result == :ok
    end

    test "apply/3 handles delete command" do
      state = MetadataStateMachine.init(%{})

      # Add a key
      {state, _, _} = MetadataStateMachine.apply(%{}, {:put, :foo, :bar}, state)

      # Delete it
      {new_state, result, _effects} = MetadataStateMachine.apply(%{}, {:delete, :foo}, state)

      assert new_state.data == %{}
      assert new_state.version == 2
      assert result == :ok
    end

    # The *value* is asserted once, in `metadata_state_machine_test.exs`.
    # This block is about the Ra callback surface, and a second copy of the
    # literal only means every version bump has two places to remember.
    test "version/0 returns a state machine version" do
      assert is_integer(MetadataStateMachine.version())
    end

    test "which_module/1 returns correct module" do
      assert MetadataStateMachine.which_module(1) == MetadataStateMachine
    end
  end

  describe "RaSupervisor" do
    test "start_link/1 starts the Ra supervisor" do
      # Supervisor is started by the application, just verify it's running
      assert Process.whereis(RaSupervisor) != nil
    end

    # The identity is a runtime value now, so a literal here would only assert
    # what this test's own `start_ra/0` chose. What matters is that every part
    # of it agrees — the server id's registered name is the cluster name, and
    # the UID and data directory belong to the same cluster.
    test "cluster_name/0 returns the cluster name the supervisor started with" do
      identity = RaSupervisor.identity()

      assert RaSupervisor.cluster_name() == identity.cluster_name
      assert RaSupervisor.system() == identity.system
      assert RaSupervisor.data_dir() == identity.data_dir
      assert RaSupervisor.uid() == "#{identity.uid_prefix}_#{RaSupervisor.sanitised_node()}"
    end

    test "server_id/0 returns server ID for current node" do
      {cluster_name, node_name} = RaSupervisor.server_id()
      assert cluster_name == RaSupervisor.cluster_name()
      assert node_name == Node.self()
    end

    # Bullet 4 of this work's acceptance: production behaviour is unchanged.
    # `identity/0` falls back to the production values when no supervisor has
    # written any, which is what a real node — where nothing passes these
    # options — resolves to.
    test "falls back to the production identity when nothing has been written" do
      key = {RaSupervisor, :identity}
      saved = :persistent_term.get(key, :absent)
      :persistent_term.erase(key)

      try do
        assert %{system: :default, cluster_name: :neonfs_meta, uid_prefix: "neonfs_meta"} =
                 RaSupervisor.identity()

        assert RaSupervisor.server_id() == {:neonfs_meta, Node.self()}
      after
        unless saved == :absent, do: :persistent_term.put(key, saved)
      end
    end

    # A test's cluster is deliberately *not* the production one, in every
    # dimension Ra keys on. If any of these ever matched the default, this
    # test's Ra would be sharing a system, a registered name or a UID with
    # whatever else ran in this VM.
    test "a test's identity shares nothing with the production default" do
      %{system: system, cluster_name: cluster_name, uid_prefix: uid_prefix} =
        RaSupervisor.identity()

      refute system == :default
      refute cluster_name == :neonfs_meta
      refute uid_prefix == "neonfs_meta"
    end
  end

  # Bullet 1 of this work's acceptance: a cluster a test starts does not
  # disturb another test's. Demonstrated rather than asserted about the
  # helper, and done by hand rather than through `start_ra/0` because two
  # clusters in one test need two explicit lifetimes.
  describe "isolation between clusters" do
    test "a fresh cluster cannot see the previous one's data" do
      first = unique_identity()
      run_cluster(first, fn -> RaSupervisor.command({:put, :leaked, :from_first}) end)

      second = unique_identity()

      state =
        run_cluster(second, fn ->
          {:ok, state} = RaSupervisor.get_state()
          state
        end)

      refute second[:system] == first[:system]
      refute second[:cluster_name] == first[:cluster_name]
      refute Map.has_key?(state.data, :leaked)
    end

    # The previous behaviour deleted a shared directory to get a clean slate.
    # Each cluster owning its own is what replaces that, and it is the reason
    # nothing has to be destroyed on the way in.
    test "each cluster owns its own data directory" do
      first = unique_identity()
      second = unique_identity()

      refute first[:data_dir] == second[:data_dir]
    end

    defp unique_identity do
      suffix = System.unique_integer([:positive, :monotonic])

      [
        system: :"ra_isolation_#{suffix}",
        cluster_name: :"ra_isolation_meta_#{suffix}",
        uid_prefix: "ra_isolation_#{suffix}",
        data_dir: Path.join(System.tmp_dir!(), "neonfs_ra_isolation_#{suffix}")
      ]
    end

    defp run_cluster(identity, fun) do
      # The module-named supervisor from `setup`'s `start_ra/0` has to be out
      # of the way before another can register.
      stop_ra()

      {:ok, sup} = RaSupervisor.start_link(identity)

      try do
        :ok = RaServer.init_cluster()
        fun.()
      after
        server_id = {identity[:cluster_name], Node.self()}
        Supervisor.stop(sup, :normal, 5_000)
        try do: :ra.force_delete_server(identity[:system], server_id), catch: (_, _ -> :ok)
        try do: :ra_system.stop(identity[:system]), catch: (_, _ -> :ok)
        File.rm_rf(identity[:data_dir])
      end
    end
  end

  describe "Ra command execution" do
    test "command/1 puts a value" do
      # Execute a put command
      assert {:ok, :ok, _leader} = RaSupervisor.command({:put, :test_key, :test_value})
    end

    test "query/1 reads state" do
      # Put a value
      {:ok, :ok, _} = RaSupervisor.command({:put, :query_test, :value})

      # Query the state
      {:ok, state} = RaSupervisor.query(fn state -> state.data[:query_test] end)
      assert state == :value
    end

    test "local_query/1 reads state from the local replica" do
      {:ok, :ok, _} = RaSupervisor.command({:put, :local_query_test, :local_value})

      {:ok, value} =
        RaSupervisor.local_query(fn state -> state.data[:local_query_test] end)

      assert value == :local_value
    end

    test "local_query/1 unwraps the {idxterm, reply} payload from :ra.local_query" do
      # ra.local_query returns {:ok, {{idx, term}, reply}, local_server}; the
      # wrapper must peel off the idxterm so callers see the same shape as
      # query/2.
      {:ok, :ok, _} = RaSupervisor.command({:put, :unwrap_test, 42})

      assert {:ok, 42} = RaSupervisor.local_query(fn state -> state.data[:unwrap_test] end)
    end

    test "get_state/0 returns full state" do
      # Put a value
      {:ok, :ok, _} = RaSupervisor.command({:put, :state_test, :state_value})

      # Get full state
      {:ok, state} = RaSupervisor.get_state()
      assert state.data[:state_test] == :state_value
      assert state.version > 0
    end

    test "command/1 deletes a value" do
      # Put a value
      {:ok, :ok, _} = RaSupervisor.command({:put, :delete_me, :value})

      # Verify it exists
      {:ok, state} = RaSupervisor.query(fn state -> state.data[:delete_me] end)
      assert state == :value

      # Delete it
      assert {:ok, :ok, _} = RaSupervisor.command({:delete, :delete_me})

      # Verify it's gone
      {:ok, state} = RaSupervisor.query(fn state -> state.data[:delete_me] end)
      assert state == nil
    end

    test "multiple commands increment version" do
      # Get initial version
      {:ok, initial_state} = RaSupervisor.get_state()
      initial_version = initial_state.version

      # Execute multiple commands
      {:ok, :ok, _} = RaSupervisor.command({:put, :v1, 1})
      {:ok, :ok, _} = RaSupervisor.command({:put, :v2, 2})
      {:ok, :ok, _} = RaSupervisor.command({:put, :v3, 3})

      # Verify version incremented
      {:ok, final_state} = RaSupervisor.get_state()
      assert final_state.version == initial_version + 3
    end
  end

  describe "Ra persistence" do
    @tag :persistence
    test "state persists across Ra server restarts" do
      # Put some data
      {:ok, :ok, _} = RaSupervisor.command({:put, :persist_key, :persist_value})

      # Get state before restart
      {:ok, state_before} = RaSupervisor.get_state()
      assert state_before.data[:persist_key] == :persist_value

      # Stop the Ra server. The system is this test's own, not `:default` —
      # naming `:default` here answered `{:error, :system_not_started}`.
      server_id = RaSupervisor.server_id()
      system = RaSupervisor.system()
      :ok = :ra.stop_server(system, server_id)

      # Wait for shutdown
      :timer.sleep(500)

      # Restart the server (not start - it already exists in Ra's registry)
      :ok = :ra.restart_server(system, server_id)

      # Wait for server to be ready
      :timer.sleep(500)

      # Query state - should have persisted data
      {:ok, state_after} = RaSupervisor.get_state()
      assert state_after.data[:persist_key] == :persist_value
    end
  end

  # `NeonFS.TestCase.stop_ra/0` bounces the `:ra` application to wipe
  # its global ETS/dets state between cases, so a reset can land while
  # Ra's dets-backed directory is closed. The server the reset wants
  # gone went with the application — that must read as "already clean",
  # not as a crash that takes the caller's test with it.
  describe "reset! with the :ra application stopped" do
    setup do
      :ok = Application.stop(:ra)
      on_exit(fn -> {:ok, _} = Application.ensure_all_started(:ra) end)
      :ok
    end

    test "returns :ok instead of crashing the server" do
      assert :ok = RaServer.reset!()
      assert Process.alive?(Process.whereis(RaServer))
    end

    test "leaves the server resettable once :ra is back" do
      :ok = RaServer.reset!()
      {:ok, _} = Application.ensure_all_started(:ra)

      assert :ok = RaServer.reset!()
    end
  end

  describe "telemetry events" do
    test "put command emits telemetry" do
      # Use a unique key to avoid test pollution from previous tests
      unique_key = :"telemetry_put_#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        "test-ra-put",
        [:neonfs, :ra, :command, :put],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, measurements, metadata})
        end,
        nil
      )

      # Execute command with unique key
      {:ok, :ok, _} = RaSupervisor.command({:put, unique_key, :value})

      # Wait for telemetry event matching our unique key
      assert_receive {:telemetry, measurements, %{key: ^unique_key}}, 1000
      assert is_integer(measurements.version)

      # Clean up
      :telemetry.detach("test-ra-put")
    end

    test "delete command emits telemetry" do
      # Use a unique key to avoid test pollution from previous tests
      unique_key = :"telemetry_delete_#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        "test-ra-delete",
        [:neonfs, :ra, :command, :delete],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, measurements, metadata})
        end,
        nil
      )

      # Put then delete with unique key
      {:ok, :ok, _} = RaSupervisor.command({:put, unique_key, :value})
      {:ok, :ok, _} = RaSupervisor.command({:delete, unique_key})

      # Wait for telemetry event matching our unique key
      assert_receive {:telemetry, measurements, %{key: ^unique_key}}, 1000
      assert is_integer(measurements.version)

      # Clean up
      :telemetry.detach("test-ra-delete")
    end
  end
end
