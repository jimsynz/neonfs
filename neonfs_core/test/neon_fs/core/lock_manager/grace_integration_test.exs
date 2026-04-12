defmodule NeonFS.Core.LockManager.GraceIntegrationTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.LockManager
  alias NeonFS.Core.LockManager.GraceCoordinator
  alias NeonFS.Core.MetadataRing

  @grace_duration_ms 500

  setup do
    start_supervised!({Registry, keys: :unique, name: NeonFS.Core.LockManager.Registry})

    start_supervised!({GraceCoordinator, grace_duration_ms: @grace_duration_ms})

    start_supervised!(NeonFS.Core.LockManager.Supervisor)

    :ok
  end

  describe "lock/5 grace period" do
    test "rejects non-reclaim lock during grace period" do
      {file_id, _departed} = setup_grace_for_local_file()

      assert {:error, :grace_period} =
               LockManager.lock(file_id, :client_a, {0, 100}, :exclusive)
    end

    test "accepts reclaim lock during grace period" do
      {file_id, _departed} = setup_grace_for_local_file()

      assert :ok =
               LockManager.lock(file_id, :client_a, {0, 100}, :exclusive, reclaim: true)
    end

    test "accepts lock after grace period expires" do
      {file_id, departed} = setup_grace_for_local_file()

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :lock_manager, :grace, :ended]
        ])

      assert_receive {[:neonfs, :lock_manager, :grace, :ended], ^ref, %{},
                      %{departed_node: ^departed}},
                     @grace_duration_ms + 500

      assert :ok =
               LockManager.lock(file_id, :client_a, {0, 100}, :exclusive)
    end

    test "accepts lock for files not in grace" do
      {_file_in_grace, departed} = setup_grace_for_local_file()

      # Find a file that was mastered by the local node in the two-node ring
      ring =
        MetadataRing.new([Node.self(), departed], virtual_nodes_per_physical: 64, replicas: 1)

      file_not_in_grace = find_file_mastered_by(ring, Node.self())

      refute GraceCoordinator.in_grace?(file_not_in_grace)

      assert :ok =
               LockManager.lock(file_not_in_grace, :client_a, {0, 100}, :exclusive)
    end
  end

  describe "open/5 grace period" do
    test "rejects non-reclaim open during grace period" do
      {file_id, _departed} = setup_grace_for_local_file()

      assert {:error, :grace_period} =
               LockManager.open(file_id, :client_a, :read, :none)
    end

    test "accepts reclaim open during grace period" do
      {file_id, _departed} = setup_grace_for_local_file()

      assert :ok =
               LockManager.open(file_id, :client_a, :read, :none, reclaim: true)
    end
  end

  describe "grant_lease/4 grace period" do
    test "rejects non-reclaim lease during grace period" do
      {file_id, _departed} = setup_grace_for_local_file()

      assert {:error, :grace_period} =
               LockManager.grant_lease(file_id, :client_a, :read)
    end

    test "accepts reclaim lease during grace period" do
      {file_id, _departed} = setup_grace_for_local_file()

      assert :ok =
               LockManager.grant_lease(file_id, :client_a, :read, reclaim: true)
    end
  end

  describe "operations unaffected by grace" do
    test "unlock works during grace period" do
      {file_id, _departed} = setup_grace_for_local_file()

      # Reclaim a lock first, then unlock it
      assert :ok =
               LockManager.lock(file_id, :client_a, {0, 100}, :exclusive, reclaim: true)

      assert :ok = LockManager.unlock(file_id, :client_a, {0, 100})
    end

    test "renew works during grace period" do
      {file_id, _departed} = setup_grace_for_local_file()

      assert :ok =
               LockManager.lock(file_id, :client_a, {0, 100}, :exclusive, reclaim: true)

      assert :ok = LockManager.renew(file_id, :client_a, ttl: 60_000)
    end
  end

  ## Helpers

  defp setup_grace_for_local_file do
    departed = :"departed_#{System.unique_integer([:positive])}@host"
    ring = MetadataRing.new([Node.self(), departed], virtual_nodes_per_physical: 64, replicas: 1)

    # Find a file mastered by the departed node in the two-node ring.
    # After the departed node leaves, this file's master becomes us (single node),
    # so lock operations route locally.
    file_id = find_file_mastered_by(ring, departed)

    # Inject grace state
    coordinator = Process.whereis(GraceCoordinator)

    :sys.replace_state(coordinator, fn state ->
      %{state | ring: ring}
    end)

    send(coordinator, {:nodedown, departed})
    :sys.get_state(coordinator)

    {file_id, departed}
  end

  defp find_file_mastered_by(ring, target_node) do
    Enum.find_value(0..999, fn i ->
      file_id = "file-#{i}"

      case MetadataRing.locate(ring, file_id) do
        {_segment, [^target_node | _]} -> file_id
        _ -> nil
      end
    end) || raise "could not find file mastered by #{target_node}"
  end
end
