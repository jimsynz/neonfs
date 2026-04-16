defmodule WebdavServer.LockStore.ETSTest do
  use ExUnit.Case, async: false

  alias WebdavServer.LockStore.ETS

  setup do
    ETS.reset()
    :ok
  end

  describe "lock/6" do
    test "creates a lock and returns a token" do
      assert {:ok, token} = ETS.lock(["file.txt"], :exclusive, :write, 0, "owner", 1800)
      assert is_binary(token)
      assert byte_size(token) > 0
    end

    test "exclusive lock conflicts with existing exclusive lock" do
      {:ok, _} = ETS.lock(["file.txt"], :exclusive, :write, 0, nil, 1800)
      assert {:error, :conflict} = ETS.lock(["file.txt"], :exclusive, :write, 0, nil, 1800)
    end

    test "exclusive lock conflicts with existing shared lock" do
      {:ok, _} = ETS.lock(["file.txt"], :shared, :write, 0, nil, 1800)
      assert {:error, :conflict} = ETS.lock(["file.txt"], :exclusive, :write, 0, nil, 1800)
    end

    test "shared lock conflicts with existing exclusive lock" do
      {:ok, _} = ETS.lock(["file.txt"], :exclusive, :write, 0, nil, 1800)
      assert {:error, :conflict} = ETS.lock(["file.txt"], :shared, :write, 0, nil, 1800)
    end

    test "multiple shared locks are allowed" do
      {:ok, _} = ETS.lock(["file.txt"], :shared, :write, 0, "user1", 1800)
      {:ok, _} = ETS.lock(["file.txt"], :shared, :write, 0, "user2", 1800)
      assert length(ETS.get_locks(["file.txt"])) == 2
    end

    test "locks on different paths do not conflict" do
      {:ok, _} = ETS.lock(["a.txt"], :exclusive, :write, 0, nil, 1800)
      {:ok, _} = ETS.lock(["b.txt"], :exclusive, :write, 0, nil, 1800)
    end

    test "stores depth in lock info" do
      {:ok, token} = ETS.lock(["dir"], :exclusive, :write, :infinity, nil, 1800)
      [info] = ETS.get_locks(["dir"])
      assert info.depth == :infinity
      assert info.token == token
    end
  end

  describe "lock/6 with Depth:infinity" do
    test "depth:infinity lock on ancestor conflicts with new descendant lock" do
      {:ok, _} = ETS.lock(["dir"], :exclusive, :write, :infinity, nil, 1800)
      assert {:error, :conflict} = ETS.lock(["dir", "file.txt"], :exclusive, :write, 0, nil, 1800)
    end

    test "depth:0 lock on ancestor does not conflict with descendant lock" do
      {:ok, _} = ETS.lock(["dir"], :exclusive, :write, 0, nil, 1800)
      assert {:ok, _} = ETS.lock(["dir", "file.txt"], :exclusive, :write, 0, nil, 1800)
    end

    test "new depth:infinity lock conflicts with existing descendant lock" do
      {:ok, _} = ETS.lock(["dir", "file.txt"], :exclusive, :write, 0, nil, 1800)
      assert {:error, :conflict} = ETS.lock(["dir"], :exclusive, :write, :infinity, nil, 1800)
    end

    test "new depth:0 lock does not conflict with existing descendant lock" do
      {:ok, _} = ETS.lock(["dir", "file.txt"], :exclusive, :write, 0, nil, 1800)
      assert {:ok, _} = ETS.lock(["dir"], :exclusive, :write, 0, nil, 1800)
    end

    test "shared depth:infinity does not conflict with shared descendant" do
      {:ok, _} = ETS.lock(["dir"], :shared, :write, :infinity, nil, 1800)
      assert {:ok, _} = ETS.lock(["dir", "file.txt"], :shared, :write, 0, nil, 1800)
    end

    test "siblings of a depth:infinity path are not blocked" do
      {:ok, _} = ETS.lock(["dir", "a"], :exclusive, :write, :infinity, nil, 1800)
      assert {:ok, _} = ETS.lock(["dir", "b"], :exclusive, :write, 0, nil, 1800)
    end
  end

  describe "unlock/1" do
    test "removes a lock" do
      {:ok, token} = ETS.lock(["file.txt"], :exclusive, :write, 0, nil, 1800)
      assert :ok = ETS.unlock(token)
      assert ETS.get_locks(["file.txt"]) == []
    end

    test "returns error for unknown token" do
      assert {:error, :not_found} = ETS.unlock("nonexistent")
    end
  end

  describe "refresh/2" do
    test "extends lock timeout" do
      {:ok, token} = ETS.lock(["file.txt"], :exclusive, :write, 0, nil, 100)
      {:ok, updated} = ETS.refresh(token, 3600)
      assert updated.timeout == 3600
    end

    test "returns error for unknown token" do
      assert {:error, :not_found} = ETS.refresh("nonexistent", 3600)
    end
  end

  describe "get_locks/1" do
    test "returns active locks for a path" do
      {:ok, _} = ETS.lock(["file.txt"], :exclusive, :write, 0, "owner", 1800)
      locks = ETS.get_locks(["file.txt"])
      assert length(locks) == 1
      assert hd(locks).owner == "owner"
    end

    test "returns empty list for unlocked path" do
      assert ETS.get_locks(["file.txt"]) == []
    end

    test "does not return ancestor depth:infinity locks" do
      {:ok, _} = ETS.lock(["dir"], :exclusive, :write, :infinity, nil, 1800)
      assert ETS.get_locks(["dir", "file.txt"]) == []
    end
  end

  describe "get_locks_covering/1" do
    test "returns direct locks on the path" do
      {:ok, token} = ETS.lock(["file.txt"], :exclusive, :write, 0, nil, 1800)
      [info] = ETS.get_locks_covering(["file.txt"])
      assert info.token == token
    end

    test "returns ancestor depth:infinity locks" do
      {:ok, token} = ETS.lock(["dir"], :exclusive, :write, :infinity, nil, 1800)
      [info] = ETS.get_locks_covering(["dir", "nested", "file.txt"])
      assert info.token == token
    end

    test "excludes ancestor depth:0 locks" do
      {:ok, _} = ETS.lock(["dir"], :exclusive, :write, 0, nil, 1800)
      assert ETS.get_locks_covering(["dir", "file.txt"]) == []
    end

    test "excludes sibling locks" do
      {:ok, _} = ETS.lock(["dir", "other.txt"], :exclusive, :write, 0, nil, 1800)
      assert ETS.get_locks_covering(["dir", "file.txt"]) == []
    end

    test "combines direct and ancestor locks" do
      {:ok, _} = ETS.lock(["dir"], :shared, :write, :infinity, "user-a", 1800)
      {:ok, _} = ETS.lock(["dir", "file.txt"], :shared, :write, 0, "user-b", 1800)
      locks = ETS.get_locks_covering(["dir", "file.txt"])
      assert length(locks) == 2
    end
  end

  describe "get_descendant_locks/1" do
    test "returns locks on descendants" do
      {:ok, a_token} = ETS.lock(["dir", "a.txt"], :exclusive, :write, 0, nil, 1800)
      {:ok, b_token} = ETS.lock(["dir", "sub", "b.txt"], :exclusive, :write, 0, nil, 1800)

      tokens = ETS.get_descendant_locks(["dir"]) |> Enum.map(& &1.token) |> Enum.sort()
      assert tokens == Enum.sort([a_token, b_token])
    end

    test "excludes the target path itself" do
      {:ok, _} = ETS.lock(["dir"], :exclusive, :write, 0, nil, 1800)
      assert ETS.get_descendant_locks(["dir"]) == []
    end

    test "excludes sibling paths" do
      {:ok, _} = ETS.lock(["other", "file.txt"], :exclusive, :write, 0, nil, 1800)
      assert ETS.get_descendant_locks(["dir"]) == []
    end

    test "excludes ancestor locks" do
      {:ok, _} = ETS.lock(["dir"], :exclusive, :write, :infinity, nil, 1800)
      assert ETS.get_descendant_locks(["dir", "sub"]) == []
    end

    test "returns empty for unlocked collection" do
      assert ETS.get_descendant_locks(["nothing"]) == []
    end
  end

  describe "check_token/2" do
    test "validates matching token and path" do
      {:ok, token} = ETS.lock(["file.txt"], :exclusive, :write, 0, nil, 1800)
      assert :ok = ETS.check_token(["file.txt"], token)
    end

    test "rejects token for different path" do
      {:ok, token} = ETS.lock(["file.txt"], :exclusive, :write, 0, nil, 1800)
      assert {:error, :invalid_token} = ETS.check_token(["other.txt"], token)
    end

    test "rejects unknown token" do
      assert {:error, :invalid_token} = ETS.check_token(["file.txt"], "unknown")
    end

    test "accepts token for descendant when lock is depth:infinity" do
      {:ok, token} = ETS.lock(["dir"], :exclusive, :write, :infinity, nil, 1800)
      assert :ok = ETS.check_token(["dir", "file.txt"], token)
      assert :ok = ETS.check_token(["dir", "sub", "deep.txt"], token)
    end

    test "rejects token for descendant when lock is depth:0" do
      {:ok, token} = ETS.lock(["dir"], :exclusive, :write, 0, nil, 1800)
      assert {:error, :invalid_token} = ETS.check_token(["dir", "file.txt"], token)
    end

    test "rejects token for ancestor or sibling even with depth:infinity" do
      {:ok, token} = ETS.lock(["dir", "sub"], :exclusive, :write, :infinity, nil, 1800)
      assert {:error, :invalid_token} = ETS.check_token(["dir"], token)
      assert {:error, :invalid_token} = ETS.check_token(["dir", "other"], token)
    end
  end
end
