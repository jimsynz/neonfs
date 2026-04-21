defmodule NeonFS.Core.KVStoreTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{KVStore, RaServer}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()

    on_exit(fn -> cleanup_test_dirs() end)

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
    test "list/0 returns records written through the store" do
      # Use unique keys so the test is robust to any Ra state left over
      # from earlier tests in this module (Ra persists to disk and can
      # leak across a module's tests on some runs).
      :ok = KVStore.put("list_0_test/a", 1)
      :ok = KVStore.put("list_0_test/b", 2)

      entries = KVStore.list()
      assert {"list_0_test/a", 1} in entries
      assert {"list_0_test/b", 2} in entries
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

  describe "bootstrap — Ra unavailable" do
    setup do
      # This describe block tests the bootstrap path where Ra is NOT
      # running — KVStore is a stateless module so there's nothing to
      # restart; we just tear down the Ra supervisor started by the
      # module-level setup.
      stop_ra()
      :ok
    end

    test "writes return {:error, :ra_not_available}" do
      assert {:error, :ra_not_available} = KVStore.put("during-boot", %{})
      assert {:error, :ra_not_available} = KVStore.delete("during-boot")
    end

    test "reads return {:error, :not_found} when the state is unreachable" do
      # local_query surfaces a :noproc exit when Ra is not started; the
      # get/list_prefix/list functions translate that to :not_found /
      # empty so callers don't need to special-case pre-cluster reads.
      assert {:error, :not_found} = KVStore.get("during-boot")
      assert [] = KVStore.list()
      assert [] = KVStore.list_prefix("anything:")
    end
  end
end
