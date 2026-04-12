defmodule WebdavServer.LockStore.ETSTest do
  use ExUnit.Case, async: false

  alias WebdavServer.LockStore.ETS

  setup do
    ETS.reset()
    :ok
  end

  describe "lock/5" do
    test "creates a lock and returns a token" do
      assert {:ok, token} = ETS.lock(["file.txt"], :exclusive, :write, "owner", 1800)
      assert is_binary(token)
      assert byte_size(token) > 0
    end

    test "exclusive lock conflicts with existing exclusive lock" do
      {:ok, _} = ETS.lock(["file.txt"], :exclusive, :write, nil, 1800)
      assert {:error, :conflict} = ETS.lock(["file.txt"], :exclusive, :write, nil, 1800)
    end

    test "exclusive lock conflicts with existing shared lock" do
      {:ok, _} = ETS.lock(["file.txt"], :shared, :write, nil, 1800)
      assert {:error, :conflict} = ETS.lock(["file.txt"], :exclusive, :write, nil, 1800)
    end

    test "shared lock conflicts with existing exclusive lock" do
      {:ok, _} = ETS.lock(["file.txt"], :exclusive, :write, nil, 1800)
      assert {:error, :conflict} = ETS.lock(["file.txt"], :shared, :write, nil, 1800)
    end

    test "multiple shared locks are allowed" do
      {:ok, _} = ETS.lock(["file.txt"], :shared, :write, "user1", 1800)
      {:ok, _} = ETS.lock(["file.txt"], :shared, :write, "user2", 1800)
      assert length(ETS.get_locks(["file.txt"])) == 2
    end

    test "locks on different paths do not conflict" do
      {:ok, _} = ETS.lock(["a.txt"], :exclusive, :write, nil, 1800)
      {:ok, _} = ETS.lock(["b.txt"], :exclusive, :write, nil, 1800)
    end
  end

  describe "unlock/1" do
    test "removes a lock" do
      {:ok, token} = ETS.lock(["file.txt"], :exclusive, :write, nil, 1800)
      assert :ok = ETS.unlock(token)
      assert ETS.get_locks(["file.txt"]) == []
    end

    test "returns error for unknown token" do
      assert {:error, :not_found} = ETS.unlock("nonexistent")
    end
  end

  describe "refresh/2" do
    test "extends lock timeout" do
      {:ok, token} = ETS.lock(["file.txt"], :exclusive, :write, nil, 100)
      {:ok, updated} = ETS.refresh(token, 3600)
      assert updated.timeout == 3600
    end

    test "returns error for unknown token" do
      assert {:error, :not_found} = ETS.refresh("nonexistent", 3600)
    end
  end

  describe "get_locks/1" do
    test "returns active locks for a path" do
      {:ok, _} = ETS.lock(["file.txt"], :exclusive, :write, "owner", 1800)
      locks = ETS.get_locks(["file.txt"])
      assert length(locks) == 1
      assert hd(locks).owner == "owner"
    end

    test "returns empty list for unlocked path" do
      assert ETS.get_locks(["file.txt"]) == []
    end
  end

  describe "check_token/2" do
    test "validates matching token and path" do
      {:ok, token} = ETS.lock(["file.txt"], :exclusive, :write, nil, 1800)
      assert :ok = ETS.check_token(["file.txt"], token)
    end

    test "rejects token for different path" do
      {:ok, token} = ETS.lock(["file.txt"], :exclusive, :write, nil, 1800)
      assert {:error, :invalid_token} = ETS.check_token(["other.txt"], token)
    end

    test "rejects unknown token" do
      assert {:error, :invalid_token} = ETS.check_token(["file.txt"], "unknown")
    end
  end
end
