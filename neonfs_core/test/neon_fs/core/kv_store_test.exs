defmodule NeonFS.Core.KVStoreTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{KVStore, RaServer}
  alias NeonFS.Error.{KeyExists, KeyNotFound, Unavailable}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  describe "put/3 and get/2" do
    test "round-trips a value" do
      assert :ok = KVStore.put("alice", %{name: "Alice"})
      assert {:ok, %{name: "Alice"}} = KVStore.get("alice")
    end

    test "replaces an existing value (overwrite? defaults to true)" do
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

    test "accepts arbitrary terms as keys" do
      :ok = KVStore.put({:user, 1}, %{name: "Alice"})
      :ok = KVStore.put(:atom_key, "atom value")
      :ok = KVStore.put(42, "integer value")
      :ok = KVStore.put({:nested, {:deeply, [1, 2, 3]}}, "deep")

      assert {:ok, %{name: "Alice"}} = KVStore.get({:user, 1})
      assert {:ok, "atom value"} = KVStore.get(:atom_key)
      assert {:ok, "integer value"} = KVStore.get(42)
      assert {:ok, "deep"} = KVStore.get({:nested, {:deeply, [1, 2, 3]}})
    end

    test "overwrite?: false rejects an existing key with KeyExists" do
      :ok = KVStore.put("conflict", :first)

      assert {:error, %KeyExists{key: "conflict"}} =
               KVStore.put("conflict", :second, overwrite?: false)

      assert {:ok, :first} = KVStore.get("conflict")
    end

    test "overwrite?: false succeeds for an absent key" do
      assert :ok = KVStore.put("fresh", :v, overwrite?: false)
      assert {:ok, :v} = KVStore.get("fresh")
    end

    test "honours :consistency :strong on get" do
      :ok = KVStore.put("strong", :value)
      assert {:ok, :value} = KVStore.get("strong", consistency: :strong)
    end
  end

  describe "get/2" do
    test "returns KeyNotFound for a missing key" do
      assert {:error, %KeyNotFound{key: "ghost"}} = KVStore.get("ghost")
    end
  end

  describe "delete/2" do
    test "removes a key" do
      :ok = KVStore.put("temp", %{})
      assert {:ok, _} = KVStore.get("temp")

      assert :ok = KVStore.delete("temp")
      assert {:error, %KeyNotFound{}} = KVStore.get("temp")
    end

    test "is idempotent on unknown keys" do
      assert :ok = KVStore.delete("never-existed")
    end
  end

  describe "update/3" do
    test "applies the function to the existing value" do
      :ok = KVStore.put("counter", 1)
      assert {:ok, 2} = KVStore.update("counter", &(&1 + 1))
      assert {:ok, 2} = KVStore.get("counter")
    end

    test "returns KeyNotFound when key is absent and no default given" do
      assert {:error, %KeyNotFound{key: "missing"}} =
               KVStore.update("missing", &Function.identity/1)
    end

    test "applies the function to the default when key is absent" do
      assert {:ok, [1]} = KVStore.update("list", &[1 | &1], default: [])
      assert {:ok, [1]} = KVStore.get("list")
    end

    test "default of nil is honoured (distinct from no-default)" do
      assert {:ok, :wrapped} = KVStore.update("nilable", fn nil -> :wrapped end, default: nil)
    end
  end

  describe "list/1 and query/2" do
    test "list/1 returns records written through the store" do
      :ok = KVStore.put("list_test/a", 1)
      :ok = KVStore.put("list_test/b", 2)

      {:ok, entries} = KVStore.list()
      assert {"list_test/a", 1} in entries
      assert {"list_test/b", 2} in entries
    end

    test "query/2 filters on key + value via a server-side predicate" do
      :ok = KVStore.put({:user, 1}, %{name: "Alice"})
      :ok = KVStore.put({:user, 2}, %{name: "Bob"})
      :ok = KVStore.put({:group, 1}, %{name: "admins"})

      {:ok, users} =
        KVStore.query(fn
          {{:user, _}, _} -> true
          _ -> false
        end)

      assert Enum.sort(users) == [
               {{:user, 1}, %{name: "Alice"}},
               {{:user, 2}, %{name: "Bob"}}
             ]
    end

    test "query/2 can filter on the value" do
      :ok = KVStore.put("a", %{active: true})
      :ok = KVStore.put("b", %{active: false})
      :ok = KVStore.put("c", %{active: true})

      {:ok, active} =
        KVStore.query(fn {_k, v} -> match?(%{active: true}, v) end)

      assert Enum.sort(active) == [
               {"a", %{active: true}},
               {"c", %{active: true}}
             ]
    end

    test "query/2 honours :consistency :strong" do
      :ok = KVStore.put("strong_q", 1)
      {:ok, entries} = KVStore.query(fn {k, _} -> k == "strong_q" end, consistency: :strong)
      assert entries == [{"strong_q", 1}]
    end
  end

  describe "bootstrap — Ra unavailable" do
    setup do
      stop_ra()
      :ok
    end

    test "writes return Unavailable" do
      assert {:error, %Unavailable{}} = KVStore.put("during-boot", %{})
      assert {:error, %Unavailable{}} = KVStore.delete("during-boot")

      assert {:error, %Unavailable{}} =
               KVStore.update("during-boot", &Function.identity/1, default: nil)
    end

    test "reads return Unavailable" do
      assert {:error, %Unavailable{}} = KVStore.get("during-boot")
      assert {:error, %Unavailable{}} = KVStore.list()
      assert {:error, %Unavailable{}} = KVStore.query(fn _ -> true end)
    end
  end
end
