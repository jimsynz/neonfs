defmodule NeonFS.Core.KVStoreTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.KVStore

  setup do
    start_kv_store()
    :ok
  end

  describe "put/2 and get/1" do
    test "round-trips a value" do
      assert :ok = KVStore.put("alice", %{name: "Alice"})
      assert {:ok, %{name: "Alice"}} = KVStore.get("alice")
    end

    test "replaces an existing value" do
      :ok = KVStore.put("bob", %{version: 1})
      :ok = KVStore.put("bob", %{version: 2})
      assert {:ok, %{version: 2}} = KVStore.get("bob")
    end

    test "stores arbitrary terms, not just maps" do
      :ok = KVStore.put("count", 42)
      :ok = KVStore.put("names", ["alice", "bob"])

      assert {:ok, 42} = KVStore.get("count")
      assert {:ok, ["alice", "bob"]} = KVStore.get("names")
    end

    test "rejects non-binary keys at the function clause" do
      assert_raise FunctionClauseError, fn -> KVStore.put(:atom_key, %{}) end
    end
  end

  describe "get/1" do
    test "returns :not_found for a missing key" do
      assert {:error, :not_found} = KVStore.get("ghost")
    end
  end

  describe "delete/1" do
    test "removes a key" do
      :ok = KVStore.put("temp", %{})
      assert {:ok, _} = KVStore.get("temp")

      assert :ok = KVStore.delete("temp")
      assert {:error, :not_found} = KVStore.get("temp")
    end

    test "is idempotent on unknown keys" do
      assert :ok = KVStore.delete("never-existed")
    end
  end

  describe "list/0 and list_prefix/1" do
    test "list/0 returns every record" do
      :ok = KVStore.put("a", 1)
      :ok = KVStore.put("b", 2)

      entries = KVStore.list() |> Enum.sort()
      assert [{"a", 1}, {"b", 2}] = entries
    end

    test "list_prefix/1 filters by key prefix" do
      :ok = KVStore.put("iam_user:1", %{name: "Alice"})
      :ok = KVStore.put("iam_user:2", %{name: "Bob"})
      :ok = KVStore.put("iam_group:1", %{name: "admins"})

      users = KVStore.list_prefix("iam_user:") |> Enum.sort()
      assert [{"iam_user:1", %{name: "Alice"}}, {"iam_user:2", %{name: "Bob"}}] = users

      assert [{"iam_group:1", %{name: "admins"}}] = KVStore.list_prefix("iam_group:")
      assert [] = KVStore.list_prefix("no_match:")
    end
  end

  describe "ETS read path" do
    test "non-GenServer callers can read without blocking on the store" do
      :ok = KVStore.put("direct", %{hello: "world"})

      assert [{"direct", %{hello: "world"}}] = :ets.lookup(:neonfs_kv, "direct")
    end
  end
end
