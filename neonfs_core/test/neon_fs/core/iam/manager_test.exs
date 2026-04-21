defmodule NeonFS.Core.IAM.ManagerTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.IAM.Manager
  alias NeonFS.Core.Persistence

  setup do
    start_iam_manager()
    :ok
  end

  describe "put/3 and get/2 across all four categories" do
    test "users round-trip" do
      assert :ok = Manager.put(:iam_users, "u-1", %{name: "alice"})
      assert {:ok, %{name: "alice"}} = Manager.get(:iam_users, "u-1")
    end

    test "groups round-trip" do
      assert :ok = Manager.put(:iam_groups, "g-1", %{name: "admins"})
      assert {:ok, %{name: "admins"}} = Manager.get(:iam_groups, "g-1")
    end

    test "policies round-trip" do
      assert :ok = Manager.put(:iam_policies, "p-1", %{effect: :allow})
      assert {:ok, %{effect: :allow}} = Manager.get(:iam_policies, "p-1")
    end

    test "identity mappings round-trip" do
      assert :ok = Manager.put(:iam_identity_mappings, {:uid, 1000}, %{user_id: "u-1"})
      assert {:ok, %{user_id: "u-1"}} = Manager.get(:iam_identity_mappings, {:uid, 1000})
    end
  end

  describe "get/2" do
    test "returns :not_found for missing key" do
      assert {:error, :not_found} = Manager.get(:iam_users, "missing")
    end
  end

  describe "put/3" do
    test "replaces an existing record" do
      :ok = Manager.put(:iam_users, "u-2", %{name: "bob", version: 1})
      :ok = Manager.put(:iam_users, "u-2", %{name: "bob", version: 2})

      assert {:ok, %{version: 2}} = Manager.get(:iam_users, "u-2")
    end

    test "rejects unknown categories at the FunctionClause boundary" do
      assert_raise FunctionClauseError, fn ->
        Manager.put(:bogus, "k", %{})
      end
    end
  end

  describe "delete/2" do
    test "removes a record from ETS" do
      :ok = Manager.put(:iam_groups, "g-delete", %{name: "temp"})
      assert {:ok, _} = Manager.get(:iam_groups, "g-delete")

      assert :ok = Manager.delete(:iam_groups, "g-delete")
      assert {:error, :not_found} = Manager.get(:iam_groups, "g-delete")
    end

    test "is idempotent on unknown keys" do
      assert :ok = Manager.delete(:iam_users, "never-existed")
    end
  end

  describe "list/1" do
    test "returns all records for a category" do
      :ok = Manager.put(:iam_policies, "p-a", %{rank: 1})
      :ok = Manager.put(:iam_policies, "p-b", %{rank: 2})

      entries = Manager.list(:iam_policies) |> Enum.sort()
      assert [{"p-a", %{rank: 1}}, {"p-b", %{rank: 2}}] = entries
    end

    test "returns an empty list for an empty category" do
      assert [] = Manager.list(:iam_identity_mappings)
    end
  end

  describe "ETS read path" do
    test "non-GenServer callers can read without blocking on the manager" do
      :ok = Manager.put(:iam_users, "u-direct", %{name: "carol"})

      # Reads go straight to ETS and succeed without a GenServer call.
      assert [{"u-direct", %{name: "carol"}}] = :ets.lookup(:iam_users, "u-direct")
    end
  end

  describe "terminate/2 snapshot" do
    test "writes a DETS file per category on shutdown" do
      # Pin the meta directory for the duration of the test — other
      # async tests manipulate this env key and the Manager's
      # `terminate/2` reads it.
      original = Application.get_env(:neonfs_core, :meta_dir)

      tmp_dir =
        Path.join(System.tmp_dir!(), "iam_manager_test_#{System.unique_integer([:positive])}")

      File.mkdir_p!(tmp_dir)
      Application.put_env(:neonfs_core, :meta_dir, tmp_dir)

      on_exit(fn ->
        if original do
          Application.put_env(:neonfs_core, :meta_dir, original)
        else
          Application.delete_env(:neonfs_core, :meta_dir)
        end

        File.rm_rf!(tmp_dir)
      end)

      :ok = Manager.put(:iam_users, "u-snap", %{name: "eve"})

      dets_path = Path.join(Persistence.meta_dir(), "iam_users.dets")
      File.rm_rf!(dets_path)

      stop_supervised!(NeonFS.Core.IAM.Manager)

      assert File.exists?(dets_path)
    end
  end
end
