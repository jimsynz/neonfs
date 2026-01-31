defmodule NeonFS.Core.RaTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.RaSupervisor

  @moduletag :ra

  setup do
    # Ensure Ra data directory is clean for each test
    ra_dir = Application.get_env(:neonfs_core, :ra_data_dir)
    File.rm_rf!(ra_dir)
    File.mkdir_p!(ra_dir)

    # Wait for Ra server to be ready
    :timer.sleep(100)

    on_exit(fn ->
      # Clean up Ra data after test
      File.rm_rf!(ra_dir)
    end)

    :ok
  end

  describe "MetadataStateMachine" do
    test "init/1 returns empty state" do
      state = MetadataStateMachine.init(%{})
      assert state.data == %{}
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

    test "version/0 returns state machine version" do
      assert MetadataStateMachine.version() == 1
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

    test "cluster_name/0 returns the cluster name" do
      assert RaSupervisor.cluster_name() == :neonfs_meta
    end

    test "server_id/0 returns server ID for current node" do
      {cluster_name, node_name} = RaSupervisor.server_id()
      assert cluster_name == :neonfs_meta
      assert node_name == Node.self()
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

      # Stop the Ra server
      server_id = RaSupervisor.server_id()
      :ok = :ra.stop_server(server_id)

      # Wait for shutdown
      :timer.sleep(500)

      # Start it again with the same configuration
      ra_dir = Application.get_env(:neonfs_core, :ra_data_dir)
      node_name = Node.self()
      cluster_name = RaSupervisor.cluster_name()

      server_id = {cluster_name, node_name}

      machine_config = %{
        module: MetadataStateMachine,
        init: fn -> MetadataStateMachine.init(%{}) end
      }

      ra_config = %{
        id: server_id,
        uid: "neonfs_meta_#{node_name}",
        cluster_name: cluster_name,
        machine: machine_config,
        log_init_args: %{
          uid: "neonfs_meta_#{node_name}",
          data_dir: ra_dir
        },
        initial_members: [server_id]
      }

      {:ok, _pid} = :ra.start_server(ra_config)

      # Wait for server to be ready
      :timer.sleep(500)

      # Query state - should have persisted data
      {:ok, state_after} = RaSupervisor.get_state()
      assert state_after.data[:persist_key] == :persist_value
    end
  end

  describe "telemetry events" do
    test "put command emits telemetry" do
      # Attach a test handler
      test_pid = self()

      :telemetry.attach(
        "test-ra-put",
        [:neonfs, :ra, :command, :put],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, measurements, metadata})
        end,
        nil
      )

      # Execute command
      {:ok, :ok, _} = RaSupervisor.command({:put, :telemetry_test, :value})

      # Wait for telemetry event
      assert_receive {:telemetry, measurements, metadata}, 1000
      assert is_integer(measurements.version)
      assert metadata.key == :telemetry_test

      # Clean up
      :telemetry.detach("test-ra-put")
    end

    test "delete command emits telemetry" do
      # Attach a test handler
      test_pid = self()

      :telemetry.attach(
        "test-ra-delete",
        [:neonfs, :ra, :command, :delete],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, measurements, metadata})
        end,
        nil
      )

      # Put then delete
      {:ok, :ok, _} = RaSupervisor.command({:put, :to_delete, :value})
      {:ok, :ok, _} = RaSupervisor.command({:delete, :to_delete})

      # Wait for telemetry event
      assert_receive {:telemetry, measurements, metadata}, 1000
      assert is_integer(measurements.version)
      assert metadata.key == :to_delete

      # Clean up
      :telemetry.detach("test-ra-delete")
    end
  end
end
