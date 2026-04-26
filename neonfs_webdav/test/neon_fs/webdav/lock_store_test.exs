defmodule NeonFS.WebDAV.LockStoreTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.FileMeta
  alias NeonFS.WebDAV.LockStore

  @file_path ["my-volume", "docs", "file.txt"]
  @file_id "test-file-id-001"

  setup do
    LockStore.reset()

    on_exit(fn ->
      Application.delete_env(:neonfs_webdav, :core_call_fn)
      Application.delete_env(:neonfs_webdav, :lock_manager_call_fn)
      Application.delete_env(:neonfs_webdav, :namespace_coordinator_call_fn)
      Application.delete_env(:neonfs_webdav, :namespace_holder_pid_fn)
      LockStore.reset()
    end)

    :ok
  end

  defp setup_mock_core(file_id \\ @file_id) do
    Application.put_env(:neonfs_webdav, :core_call_fn, fn
      :get_file_meta, [_volume, _path] ->
        {:ok, %FileMeta{id: file_id, volume_id: "vol-1", path: "/docs/file.txt"}}
    end)
  end

  defp setup_mock_lock_manager do
    test_pid = self()

    Application.put_env(:neonfs_webdav, :lock_manager_call_fn, fn function, args ->
      send(test_pid, {:lock_manager, function, args})

      case function do
        :lock -> :ok
        :unlock -> :ok
        :renew -> :ok
      end
    end)
  end

  defp setup_conflicting_lock_manager do
    Application.put_env(:neonfs_webdav, :lock_manager_call_fn, fn :lock, _args ->
      {:error, :timeout}
    end)
  end

  defp setup_core_not_found do
    Application.put_env(:neonfs_webdav, :core_call_fn, fn
      :get_file_meta, _args -> {:error, :not_found}
    end)
  end

  defp setup_core_unavailable do
    Application.put_env(:neonfs_webdav, :core_call_fn, fn
      :get_file_meta, _args -> {:error, :unavailable}
    end)
  end

  describe "lock/6 with DLM" do
    setup do
      setup_mock_core()
      setup_mock_lock_manager()
      :ok
    end

    test "acquires exclusive lock via DLM and returns token" do
      assert {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert is_binary(token)
      assert byte_size(token) > 0

      file_id = @file_id

      assert_received {:lock_manager, :lock, [^file_id, ^token, {0, _}, :exclusive, opts]}

      assert Keyword.get(opts, :ttl) == 300_000
    end

    test "acquires shared lock via DLM" do
      assert {:ok, token} = LockStore.lock(@file_path, :shared, :write, 0, "user-a", 600)
      file_id = @file_id

      assert_received {:lock_manager, :lock, [^file_id, ^token, {0, _}, :shared, opts]}

      assert Keyword.get(opts, :ttl) == 600_000
    end

    test "returns conflict when DLM rejects lock" do
      setup_conflicting_lock_manager()

      assert {:error, :conflict} =
               LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
    end

    test "stores lock info in ETS" do
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)

      assert [lock] = LockStore.get_locks(@file_path)
      assert lock.token == token
      assert lock.scope == :exclusive
      assert lock.type == :write
      assert lock.owner == "user-a"
      assert lock.timeout == 300
    end
  end

  describe "lock/6 local-only (core unavailable)" do
    setup do
      setup_core_unavailable()
      :ok
    end

    test "falls back to local locking" do
      assert {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert is_binary(token)

      assert [lock] = LockStore.get_locks(@file_path)
      assert lock.token == token
    end

    test "detects local exclusive conflict" do
      assert {:ok, _} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)

      assert {:error, :conflict} =
               LockStore.lock(@file_path, :exclusive, :write, 0, "user-b", 300)
    end

    test "detects local shared vs exclusive conflict" do
      assert {:ok, _} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert {:error, :conflict} = LockStore.lock(@file_path, :shared, :write, 0, "user-b", 300)
    end

    test "allows multiple shared locks" do
      assert {:ok, _} = LockStore.lock(@file_path, :shared, :write, 0, "user-a", 300)
      assert {:ok, _} = LockStore.lock(@file_path, :shared, :write, 0, "user-b", 300)

      assert length(LockStore.get_locks(@file_path)) == 2
    end

    test "exclusive conflicts with existing shared" do
      assert {:ok, _} = LockStore.lock(@file_path, :shared, :write, 0, "user-a", 300)

      assert {:error, :conflict} =
               LockStore.lock(@file_path, :exclusive, :write, 0, "user-b", 300)
    end
  end

  describe "lock/6 for root and volume paths" do
    test "locks root path locally" do
      assert {:ok, token} = LockStore.lock([], :exclusive, :write, 0, "user-a", 300)
      assert is_binary(token)
    end

    test "locks volume path locally" do
      assert {:ok, token} = LockStore.lock(["my-volume"], :exclusive, :write, 0, "user-a", 300)
      assert is_binary(token)
    end
  end

  describe "unlock/1" do
    setup do
      setup_mock_core()
      setup_mock_lock_manager()
      :ok
    end

    test "releases DLM lock and removes from ETS" do
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert_received {:lock_manager, :lock, _}

      assert :ok = LockStore.unlock(token)
      file_id = @file_id
      assert_received {:lock_manager, :unlock, [^file_id, ^token, {0, _}]}

      assert LockStore.get_locks(@file_path) == []
    end

    test "returns not_found for unknown token" do
      assert {:error, :not_found} = LockStore.unlock("nonexistent-token")
    end

    test "unlocks local-only lock without DLM call" do
      setup_core_unavailable()

      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert :ok = LockStore.unlock(token)

      assert LockStore.get_locks(@file_path) == []
    end
  end

  describe "refresh/2" do
    setup do
      setup_mock_core()
      setup_mock_lock_manager()
      :ok
    end

    test "renews DLM lock and updates ETS" do
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert_received {:lock_manager, :lock, _}

      assert {:ok, updated} = LockStore.refresh(token, 600)
      assert updated.timeout == 600

      file_id = @file_id
      assert_received {:lock_manager, :renew, [^file_id, ^token, opts]}
      assert Keyword.get(opts, :ttl) == 600_000
    end

    test "returns not_found for unknown token" do
      assert {:error, :not_found} = LockStore.refresh("nonexistent", 600)
    end

    test "returns not_found for expired lock" do
      setup_core_unavailable()

      # Create a lock with 1-second timeout, then manipulate expiry
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 1)

      [{^token, lock_info}] = :ets.lookup(NeonFS.WebDAV.LockStore, token)
      expired = %{lock_info | expires_at: System.system_time(:second) - 10}
      :ets.insert(NeonFS.WebDAV.LockStore, {token, expired})

      assert {:error, :not_found} = LockStore.refresh(token, 600)
      assert LockStore.get_locks(@file_path) == []
    end

    test "refreshes local-only lock without DLM call" do
      setup_core_unavailable()

      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert {:ok, updated} = LockStore.refresh(token, 600)
      assert updated.timeout == 600
    end
  end

  describe "get_locks/1" do
    test "returns empty list for unlocked path" do
      assert LockStore.get_locks(@file_path) == []
    end

    test "returns active locks on path" do
      setup_core_unavailable()

      {:ok, _} = LockStore.lock(@file_path, :shared, :write, 0, "user-a", 300)
      {:ok, _} = LockStore.lock(@file_path, :shared, :write, 0, "user-b", 300)

      locks = LockStore.get_locks(@file_path)
      assert length(locks) == 2
      assert Enum.all?(locks, &(&1.scope == :shared))
    end

    test "excludes expired locks" do
      setup_core_unavailable()

      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 1)

      [{^token, lock_info}] = :ets.lookup(NeonFS.WebDAV.LockStore, token)
      expired = %{lock_info | expires_at: System.system_time(:second) - 10}
      :ets.insert(NeonFS.WebDAV.LockStore, {token, expired})

      assert LockStore.get_locks(@file_path) == []
    end

    test "does not return locks from different paths" do
      setup_core_unavailable()

      other_path = ["my-volume", "other", "file.txt"]
      {:ok, _} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      {:ok, _} = LockStore.lock(other_path, :exclusive, :write, 0, "user-b", 300)

      assert length(LockStore.get_locks(@file_path)) == 1
      assert length(LockStore.get_locks(other_path)) == 1
    end

    test "does not expose internal fields in returned lock info" do
      setup_mock_core()
      setup_mock_lock_manager()

      {:ok, _} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)

      [lock] = LockStore.get_locks(@file_path)
      refute Map.has_key?(lock, :file_id)
      refute Map.has_key?(lock, :lock_null)
    end
  end

  describe "check_token/2" do
    setup do
      setup_core_unavailable()
      :ok
    end

    test "returns ok for valid token on matching path" do
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert :ok = LockStore.check_token(@file_path, token)
    end

    test "returns invalid_token for wrong path" do
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      other_path = ["my-volume", "other", "file.txt"]
      assert {:error, :invalid_token} = LockStore.check_token(other_path, token)
    end

    test "returns invalid_token for unknown token" do
      assert {:error, :invalid_token} = LockStore.check_token(@file_path, "bogus-token")
    end

    test "returns invalid_token for expired lock and cleans up" do
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 1)

      [{^token, lock_info}] = :ets.lookup(NeonFS.WebDAV.LockStore, token)
      expired = %{lock_info | expires_at: System.system_time(:second) - 10}
      :ets.insert(NeonFS.WebDAV.LockStore, {token, expired})

      assert {:error, :invalid_token} = LockStore.check_token(@file_path, token)
      # Should have been cleaned up
      assert :ets.lookup(NeonFS.WebDAV.LockStore, token) == []
    end
  end

  describe "cross-protocol conflict detection" do
    test "DLM conflict prevents WebDAV lock" do
      setup_mock_core()
      setup_conflicting_lock_manager()

      assert {:error, :conflict} =
               LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)

      assert LockStore.get_locks(@file_path) == []
    end
  end

  describe "reset/0" do
    test "clears all locks" do
      setup_core_unavailable()

      {:ok, _} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert length(LockStore.get_locks(@file_path)) == 1

      LockStore.reset()
      assert LockStore.get_locks(@file_path) == []
    end
  end

  describe "lock-null resources" do
    setup do
      setup_core_not_found()
      setup_mock_lock_manager()
      :ok
    end

    test "locks non-existent file via DLM with path-based ID" do
      assert {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert is_binary(token)

      assert_received {:lock_manager, :lock, [path_id, ^token, {0, _}, :exclusive, opts]}
      assert String.starts_with?(path_id, "lock-null:")
      assert Keyword.get(opts, :ttl) == 300_000
    end

    test "generates deterministic path-based IDs" do
      {:ok, token1} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert_received {:lock_manager, :lock, [id1, ^token1, _, _, _]}

      LockStore.reset()

      {:ok, token2} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-b", 300)
      assert_received {:lock_manager, :lock, [id2, ^token2, _, _, _]}

      assert id1 == id2
    end

    test "different paths generate different IDs" do
      {:ok, token1} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert_received {:lock_manager, :lock, [id1, ^token1, _, _, _]}

      other_path = ["my-volume", "other", "file.txt"]
      {:ok, token2} = LockStore.lock(other_path, :exclusive, :write, 0, "user-b", 300)
      assert_received {:lock_manager, :lock, [id2, ^token2, _, _, _]}

      refute id1 == id2
    end

    test "is_lock_null? returns true for lock-null path" do
      {:ok, _} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert LockStore.lock_null?(@file_path)
    end

    test "is_lock_null? returns false for non-locked path" do
      refute LockStore.lock_null?(@file_path)
    end

    test "is_lock_null? returns false for existing file lock" do
      setup_mock_core()

      {:ok, _} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      refute LockStore.lock_null?(@file_path)
    end

    test "is_lock_null? returns false for expired lock-null" do
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 1)

      [{^token, lock_info}] = :ets.lookup(NeonFS.WebDAV.LockStore, token)
      expired = %{lock_info | expires_at: System.system_time(:second) - 10}
      :ets.insert(NeonFS.WebDAV.LockStore, {token, expired})

      refute LockStore.lock_null?(@file_path)
    end

    test "get_lock_null_paths returns child lock-null paths" do
      {:ok, _} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      other = ["my-volume", "docs", "other.txt"]
      {:ok, _} = LockStore.lock(other, :exclusive, :write, 0, "user-b", 300)

      paths = LockStore.get_lock_null_paths(["my-volume", "docs"])
      assert length(paths) == 2
      assert @file_path in paths
      assert other in paths
    end

    test "get_lock_null_paths excludes non-child paths" do
      {:ok, _} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      nested = ["my-volume", "docs", "sub", "deep.txt"]
      {:ok, _} = LockStore.lock(nested, :exclusive, :write, 0, "user-b", 300)

      paths = LockStore.get_lock_null_paths(["my-volume", "docs"])
      assert @file_path in paths
      refute nested in paths
    end

    test "get_lock_null_paths excludes expired entries" do
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 1)

      [{^token, lock_info}] = :ets.lookup(NeonFS.WebDAV.LockStore, token)
      expired = %{lock_info | expires_at: System.system_time(:second) - 10}
      :ets.insert(NeonFS.WebDAV.LockStore, {token, expired})

      assert LockStore.get_lock_null_paths(["my-volume", "docs"]) == []
    end

    test "unlock releases DLM lock for lock-null resource" do
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert_received {:lock_manager, :lock, [path_id, ^token, _, _, _]}

      assert :ok = LockStore.unlock(token)
      assert_received {:lock_manager, :unlock, [^path_id, ^token, {0, _}]}

      refute LockStore.lock_null?(@file_path)
    end

    test "refresh renews DLM lock for lock-null resource" do
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert_received {:lock_manager, :lock, [path_id, ^token, _, _, _]}

      assert {:ok, updated} = LockStore.refresh(token, 600)
      assert updated.timeout == 600

      assert_received {:lock_manager, :renew, [^path_id, ^token, opts]}
      assert Keyword.get(opts, :ttl) == 600_000
    end
  end

  describe "promote_lock_null/2" do
    setup do
      setup_core_not_found()
      setup_mock_lock_manager()
      :ok
    end

    test "promotes lock-null to real file lock" do
      {:ok, token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert_received {:lock_manager, :lock, [path_id, ^token, _, _, _]}
      assert LockStore.lock_null?(@file_path)

      real_file_id = "real-file-id-001"
      assert :ok = LockStore.promote_lock_null(@file_path, real_file_id)

      assert_received {:lock_manager, :lock, [^real_file_id, ^token, {0, _}, :exclusive, _]}
      assert_received {:lock_manager, :unlock, [^path_id, ^token, {0, _}]}

      refute LockStore.lock_null?(@file_path)
      assert [lock] = LockStore.get_locks(@file_path)
      assert lock.token == token
    end

    test "does nothing for non-lock-null paths" do
      setup_mock_core()
      {:ok, _token} = LockStore.lock(@file_path, :exclusive, :write, 0, "user-a", 300)
      assert_received {:lock_manager, :lock, _}

      assert :ok = LockStore.promote_lock_null(@file_path, "new-id")
      refute_received {:lock_manager, :lock, _}
    end

    test "does nothing for paths with no locks" do
      assert :ok = LockStore.promote_lock_null(@file_path, "some-id")
      refute_received {:lock_manager, _, _}
    end
  end

  describe "collection locking with Depth:infinity" do
    @collection_path ["my-volume", "docs"]
    @child_path ["my-volume", "docs", "file.txt"]
    @nested_path ["my-volume", "docs", "sub", "deep.txt"]
    @sibling_path ["my-volume", "other", "file.txt"]

    setup do
      setup_core_unavailable()
      :ok
    end

    test "stores depth in lock info" do
      {:ok, _token} =
        LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      [lock] = LockStore.get_locks(@collection_path)
      assert lock.depth == :infinity
    end

    test "get_locks_covering returns direct locks" do
      {:ok, token} =
        LockStore.lock(@child_path, :exclusive, :write, 0, "user-a", 300)

      [lock] = LockStore.get_locks_covering(@child_path)
      assert lock.token == token
    end

    test "get_locks_covering returns ancestor depth:infinity locks" do
      {:ok, token} =
        LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      [lock] = LockStore.get_locks_covering(@child_path)
      assert lock.token == token

      [lock] = LockStore.get_locks_covering(@nested_path)
      assert lock.token == token
    end

    test "get_locks_covering excludes ancestor depth:0 locks" do
      {:ok, _} = LockStore.lock(@collection_path, :exclusive, :write, 0, "user-a", 300)

      assert LockStore.get_locks_covering(@child_path) == []
    end

    test "get_locks_covering excludes sibling directory locks" do
      {:ok, _} =
        LockStore.lock(["my-volume", "other"], :exclusive, :write, :infinity, "user-a", 300)

      assert LockStore.get_locks_covering(@child_path) == []
    end

    test "check_token accepts descendant path under depth:infinity lock" do
      {:ok, token} =
        LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      assert :ok = LockStore.check_token(@child_path, token)
      assert :ok = LockStore.check_token(@nested_path, token)
    end

    test "check_token rejects descendant path under depth:0 lock" do
      {:ok, token} = LockStore.lock(@collection_path, :exclusive, :write, 0, "user-a", 300)

      assert {:error, :invalid_token} = LockStore.check_token(@child_path, token)
    end

    test "check_token rejects sibling path under depth:infinity lock" do
      {:ok, token} =
        LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      assert {:error, :invalid_token} = LockStore.check_token(@sibling_path, token)
    end

    test "locking descendant conflicts with ancestor depth:infinity" do
      {:ok, _} =
        LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      assert {:error, :conflict} =
               LockStore.lock(@child_path, :exclusive, :write, 0, "user-b", 300)
    end

    test "locking depth:0 ancestor does not conflict with descendant" do
      {:ok, _} = LockStore.lock(@collection_path, :exclusive, :write, 0, "user-a", 300)

      assert {:ok, _} = LockStore.lock(@child_path, :exclusive, :write, 0, "user-b", 300)
    end

    test "locking depth:infinity on path with locked descendant conflicts" do
      {:ok, _} = LockStore.lock(@child_path, :exclusive, :write, 0, "user-a", 300)

      assert {:error, :conflict} =
               LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-b", 300)
    end

    test "locking depth:0 on path with locked descendant does not conflict" do
      {:ok, _} = LockStore.lock(@child_path, :exclusive, :write, 0, "user-a", 300)

      assert {:ok, _} =
               LockStore.lock(@collection_path, :exclusive, :write, 0, "user-b", 300)
    end

    test "shared depth:infinity allows shared descendant lock" do
      {:ok, _} = LockStore.lock(@collection_path, :shared, :write, :infinity, "user-a", 300)

      assert {:ok, _} = LockStore.lock(@child_path, :shared, :write, 0, "user-b", 300)
    end

    test "exclusive depth:infinity blocks shared descendant lock" do
      {:ok, _} =
        LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      assert {:error, :conflict} =
               LockStore.lock(@child_path, :shared, :write, 0, "user-b", 300)
    end

    test "get_locks excludes ancestor depth:infinity locks" do
      {:ok, _} =
        LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      assert LockStore.get_locks(@child_path) == []
    end

    test "get_locks_covering excludes expired ancestor locks" do
      {:ok, token} =
        LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 1)

      [{^token, lock_info}] = :ets.lookup(NeonFS.WebDAV.LockStore, token)
      expired = %{lock_info | expires_at: System.system_time(:second) - 10}
      :ets.insert(NeonFS.WebDAV.LockStore, {token, expired})

      assert LockStore.get_locks_covering(@child_path) == []
    end

    test "get_locks_covering does not expose internal fields" do
      setup_mock_core()
      setup_mock_lock_manager()

      {:ok, _} =
        LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      [lock] = LockStore.get_locks_covering(@child_path)
      refute Map.has_key?(lock, :file_id)
      refute Map.has_key?(lock, :lock_null)
    end
  end

  describe "get_descendant_locks/1" do
    @collection_path ["my-volume", "docs"]
    @child_path ["my-volume", "docs", "file.txt"]
    @nested_path ["my-volume", "docs", "sub", "deep.txt"]
    @sibling_path ["my-volume", "other", "file.txt"]

    setup do
      setup_core_unavailable()
      :ok
    end

    test "returns locks on direct and nested descendants" do
      {:ok, child} = LockStore.lock(@child_path, :exclusive, :write, 0, "user-a", 300)
      {:ok, nested} = LockStore.lock(@nested_path, :exclusive, :write, 0, "user-b", 300)

      tokens =
        @collection_path
        |> LockStore.get_descendant_locks()
        |> Enum.map(& &1.token)
        |> Enum.sort()

      assert tokens == Enum.sort([child, nested])
    end

    test "excludes the target path itself" do
      {:ok, _} =
        LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      assert LockStore.get_descendant_locks(@collection_path) == []
    end

    test "excludes sibling paths" do
      {:ok, _} = LockStore.lock(@sibling_path, :exclusive, :write, 0, "user-a", 300)

      assert LockStore.get_descendant_locks(@collection_path) == []
    end

    test "excludes expired descendant locks" do
      {:ok, token} = LockStore.lock(@child_path, :exclusive, :write, 0, "user-a", 1)

      [{^token, lock_info}] = :ets.lookup(NeonFS.WebDAV.LockStore, token)
      expired = %{lock_info | expires_at: System.system_time(:second) - 10}
      :ets.insert(NeonFS.WebDAV.LockStore, {token, expired})

      assert LockStore.get_descendant_locks(@collection_path) == []
    end

    test "does not expose internal fields" do
      {:ok, _} = LockStore.lock(@child_path, :exclusive, :write, 0, "user-a", 300)

      [lock] = LockStore.get_descendant_locks(@collection_path)
      refute Map.has_key?(lock, :file_id)
      refute Map.has_key?(lock, :lock_null)
      refute Map.has_key?(lock, :namespace_claim_id)
    end
  end

  describe "Depth: infinity LOCK against the namespace coordinator" do
    @collection_path ["my-volume", "docs"]
    @child_path ["my-volume", "docs", "file.txt"]

    setup do
      # An always-alive holder pid for the coordinator to "monitor".
      {:ok, holder} = Agent.start_link(fn -> nil end)
      Application.put_env(:neonfs_webdav, :namespace_holder_pid_fn, fn -> holder end)
      setup_core_unavailable()
      {:ok, holder: holder}
    end

    defp setup_namespace_coordinator(reply_fn) do
      test_pid = self()

      Application.put_env(:neonfs_webdav, :namespace_coordinator_call_fn, fn function, args ->
        send(test_pid, {:namespace_coordinator, function, args})
        reply_fn.(function, args)
      end)
    end

    test "Depth: infinity acquires a subtree claim from the coordinator", %{holder: holder} do
      setup_namespace_coordinator(fn :claim_subtree_for, _ -> {:ok, "ns-claim-1"} end)

      assert {:ok, _token} =
               LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      assert_received {:namespace_coordinator, :claim_subtree_for,
                       ["/my-volume/docs", :exclusive, ^holder]}
    end

    test "Depth: 0 takes no namespace claim" do
      setup_namespace_coordinator(fn _, _ ->
        flunk("coordinator must not be called for depth=0")
      end)

      assert {:ok, _token} = LockStore.lock(@child_path, :exclusive, :write, 0, "user-a", 300)
    end

    test "coordinator-reported conflict refuses the lock" do
      setup_namespace_coordinator(fn :claim_subtree_for, _ ->
        {:error, :conflict, "ns-claim-99"}
      end)

      assert {:error, :conflict} =
               LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)
    end

    test "coordinator unavailability falls back to single-node ETS detection" do
      setup_namespace_coordinator(fn _, _ -> {:error, :unavailable} end)

      assert {:ok, _} =
               LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      # Single-node hierarchical detection still kicks in for the second
      # claim from the same node.
      assert {:error, :conflict} =
               LockStore.lock(@child_path, :exclusive, :write, 0, "user-b", 300)
    end

    test "UNLOCK releases the coordinator claim" do
      setup_namespace_coordinator(fn
        :claim_subtree_for, _ -> {:ok, "ns-claim-7"}
        :release, _ -> :ok
      end)

      {:ok, token} =
        LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      assert :ok = LockStore.unlock(token)

      assert_received {:namespace_coordinator, :release, ["ns-claim-7"]}
    end

    test "claim is released when DLM acquisition fails after the claim succeeds" do
      setup_mock_core()

      # DLM rejects → we must release the namespace claim we just took.
      Application.put_env(:neonfs_webdav, :lock_manager_call_fn, fn :lock, _ ->
        {:error, :timeout}
      end)

      setup_namespace_coordinator(fn
        :claim_subtree_for, _ -> {:ok, "ns-claim-13"}
        :release, _ -> :ok
      end)

      assert {:error, :conflict} =
               LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-a", 300)

      assert_received {:namespace_coordinator, :release, ["ns-claim-13"]}
    end

    test "claim is released when local pre-check rejects after coordinator grants" do
      # Pre-load a conflicting local lock so local_conflict?/3 returns true
      # *after* the coordinator has already granted the claim. The store
      # must release the just-taken claim in that case.
      {:ok, _existing} =
        LockStore.lock(@child_path, :exclusive, :write, 0, "user-a", 300)

      setup_namespace_coordinator(fn
        :claim_subtree_for, _ -> {:ok, "ns-claim-21"}
        :release, _ -> :ok
      end)

      assert {:error, :conflict} =
               LockStore.lock(@collection_path, :exclusive, :write, :infinity, "user-b", 300)

      assert_received {:namespace_coordinator, :release, ["ns-claim-21"]}
    end
  end
end
