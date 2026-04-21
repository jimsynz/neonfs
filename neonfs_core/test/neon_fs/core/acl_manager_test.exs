defmodule NeonFS.Core.ACLManagerTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{ACLManager, MetadataStateMachine, RaServer, RaSupervisor, VolumeACL}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()

    start_audit_log()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  defp make_acl(volume_id, opts \\ []) do
    VolumeACL.new(
      volume_id: volume_id,
      owner_uid: Keyword.get(opts, :owner_uid, 1000),
      owner_gid: Keyword.get(opts, :owner_gid, 1000),
      entries: Keyword.get(opts, :entries, [])
    )
  end

  describe "set_volume_acl/2 and get_volume_acl/1" do
    test "creates and retrieves an ACL" do
      acl = make_acl("vol-1")
      assert :ok = ACLManager.set_volume_acl("vol-1", acl)
      assert {:ok, ^acl} = ACLManager.get_volume_acl("vol-1")
    end

    test "replaces an existing ACL" do
      acl1 = make_acl("vol-replace", owner_uid: 1000)
      acl2 = make_acl("vol-replace", owner_uid: 2000)

      :ok = ACLManager.set_volume_acl("vol-replace", acl1)
      :ok = ACLManager.set_volume_acl("vol-replace", acl2)

      {:ok, stored} = ACLManager.get_volume_acl("vol-replace")
      assert stored.owner_uid == 2000
    end

    test "returns not_found for missing ACL" do
      assert {:error, :not_found} = ACLManager.get_volume_acl("non-existent")
    end
  end

  describe "grant/3" do
    test "adds a new entry" do
      acl = make_acl("vol-grant")
      :ok = ACLManager.set_volume_acl("vol-grant", acl)

      :ok = ACLManager.grant("vol-grant", {:uid, 2000}, [:read, :write])

      {:ok, updated} = ACLManager.get_volume_acl("vol-grant")
      assert length(updated.entries) == 1

      entry = hd(updated.entries)
      assert entry.principal == {:uid, 2000}
      assert MapSet.equal?(entry.permissions, MapSet.new([:read, :write]))
    end

    test "replaces existing entry for same principal" do
      acl = make_acl("vol-grant-replace")
      :ok = ACLManager.set_volume_acl("vol-grant-replace", acl)

      :ok = ACLManager.grant("vol-grant-replace", {:uid, 2000}, [:read])
      :ok = ACLManager.grant("vol-grant-replace", {:uid, 2000}, [:read, :write, :admin])

      {:ok, updated} = ACLManager.get_volume_acl("vol-grant-replace")
      assert length(updated.entries) == 1

      entry = hd(updated.entries)
      assert MapSet.equal?(entry.permissions, MapSet.new([:read, :write, :admin]))
    end

    test "returns not_found when volume ACL doesn't exist" do
      assert {:error, :not_found} = ACLManager.grant("no-such-vol", {:uid, 1}, [:read])
    end
  end

  describe "revoke/2" do
    test "removes an entry by principal" do
      acl =
        make_acl("vol-revoke",
          entries: [
            %{principal: {:uid, 2000}, permissions: MapSet.new([:read])},
            %{principal: {:uid, 3000}, permissions: MapSet.new([:write])}
          ]
        )

      :ok = ACLManager.set_volume_acl("vol-revoke", acl)
      :ok = ACLManager.revoke("vol-revoke", {:uid, 2000})

      {:ok, updated} = ACLManager.get_volume_acl("vol-revoke")
      assert length(updated.entries) == 1
      assert hd(updated.entries).principal == {:uid, 3000}
    end

    test "no-op when principal not in entries" do
      acl = make_acl("vol-revoke-noop")
      :ok = ACLManager.set_volume_acl("vol-revoke-noop", acl)

      :ok = ACLManager.revoke("vol-revoke-noop", {:uid, 9999})

      {:ok, updated} = ACLManager.get_volume_acl("vol-revoke-noop")
      assert updated.entries == []
    end

    test "returns not_found when volume ACL doesn't exist" do
      assert {:error, :not_found} = ACLManager.revoke("no-such-vol", {:uid, 1})
    end
  end

  describe "delete_volume_acl/1" do
    test "removes ACL and the delete replicates via Ra" do
      acl = make_acl("vol-delete")
      :ok = ACLManager.set_volume_acl("vol-delete", acl)
      assert {:ok, _} = ACLManager.get_volume_acl("vol-delete")

      # Ra reflects the ACL before the delete.
      assert {:ok, acl_map} =
               RaSupervisor.local_query(&MetadataStateMachine.get_volume_acl(&1, "vol-delete"))

      assert is_map(acl_map)

      :ok = ACLManager.delete_volume_acl("vol-delete")
      assert {:error, :not_found} = ACLManager.get_volume_acl("vol-delete")

      # Regression: before this migration, delete_volume_acl/1 only cleared
      # the local ETS cache and never issued a Ra command — the ACL
      # resurrected on other nodes. Assert the Ra state itself changes.
      assert {:ok, nil} =
               RaSupervisor.local_query(&MetadataStateMachine.get_volume_acl(&1, "vol-delete"))
    end

    test "is a no-op when the ACL doesn't exist" do
      assert :ok = ACLManager.delete_volume_acl("never-existed")
    end
  end

  describe "GID entries" do
    test "grant and revoke GID entries" do
      acl = make_acl("vol-gid")
      :ok = ACLManager.set_volume_acl("vol-gid", acl)

      :ok = ACLManager.grant("vol-gid", {:gid, 100}, [:read])

      {:ok, updated} = ACLManager.get_volume_acl("vol-gid")
      assert length(updated.entries) == 1
      assert hd(updated.entries).principal == {:gid, 100}

      :ok = ACLManager.revoke("vol-gid", {:gid, 100})

      {:ok, updated2} = ACLManager.get_volume_acl("vol-gid")
      assert updated2.entries == []
    end
  end
end
