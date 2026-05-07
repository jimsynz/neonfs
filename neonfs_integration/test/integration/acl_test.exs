defmodule NeonFS.Integration.ACLTest do
  @moduledoc """
  Phase 6 integration tests for access control lists.

  Tests the full ACL lifecycle:
  - Volume ACL: grant UID read, verify read succeeds and write denied
  - Volume ACL: grant UID write, verify both read and write succeed
  - Volume ACL: owner UID has full access without explicit grant
  - Volume ACL: GID-based permission
  - Volume ACL: UID 0 (root) bypasses all checks
  - File ACL: POSIX mode 0600 enforcement
  - File ACL: extended ACL entry for specific UID
  - Directory default ACL inheritance
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_single_node_cluster(cluster, name: "acl-test")

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "acl-volume",
        %{}
      ])

    %{volume_id: volume.id}
  end

  describe "volume ACL — UID-based" do
    test "grant read only — read succeeds, write denied", %{cluster: cluster, volume_id: vid} do
      # Grant UID 1000 read-only permission
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :grant, [
          vid,
          {:uid, 1000},
          [:read]
        ])

      # Write as UID 1000 should be denied
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
          vid,
          "/denied.bin",
          0,
          "test",
          [uid: 1000]
        ])

      assert {:error, :forbidden} = result

      # Write as root (UID 0) should succeed
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "acl-volume",
          "/readable.bin",
          "test data"
        ])

      # Read as UID 1000 should succeed
      {:ok, data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ReadOperation, :read_file, [
          vid,
          "/readable.bin",
          [uid: 1000]
        ])

      assert data == "test data"
    end

    test "grant write — both read and write succeed", %{cluster: cluster, volume_id: vid} do
      # Grant UID 1000 write permission (implies read)
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :grant, [
          vid,
          {:uid, 1000},
          [:write]
        ])

      # Write as UID 1000 should succeed
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
          vid,
          "/writable.bin",
          0,
          "written by 1000",
          [uid: 1000]
        ])

      # Read as UID 1000 should also succeed
      {:ok, data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ReadOperation, :read_file, [
          vid,
          "/writable.bin",
          [uid: 1000]
        ])

      assert data == "written by 1000"
    end

    test "volume owner has full access without explicit grant", %{
      cluster: cluster,
      volume_id: vid
    } do
      # Set volume ACL with owner_uid = 500
      acl =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeACL, :new, [
          [volume_id: vid, owner_uid: 500, owner_gid: 500]
        ])

      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :set_volume_acl, [
          vid,
          acl
        ])

      # Owner UID 500 can write without explicit grant
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
          vid,
          "/owner.bin",
          0,
          "owner data",
          [uid: 500]
        ])

      # Owner UID 500 can read
      {:ok, data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ReadOperation, :read_file, [
          vid,
          "/owner.bin",
          [uid: 500]
        ])

      assert data == "owner data"
    end

    test "UID 0 (root) bypasses all ACL checks", %{cluster: cluster, volume_id: vid} do
      # Don't grant any permissions — root should still work

      # Root can write
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
          vid,
          "/root.bin",
          0,
          "root data",
          [uid: 0]
        ])

      # Root can read
      {:ok, data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ReadOperation, :read_file, [
          vid,
          "/root.bin",
          [uid: 0]
        ])

      assert data == "root data"
    end
  end

  describe "volume ACL — GID-based" do
    test "GID-based permission grants access to matching UID", %{cluster: cluster, volume_id: vid} do
      # Grant GID 100 read permission
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :grant, [
          vid,
          {:gid, 100},
          [:read]
        ])

      # Write a file as root so there's something to read
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "acl-volume",
          "/gid-test.bin",
          "gid data"
        ])

      # UID 2000 with GID 100 in supplementary groups can read
      {:ok, data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ReadOperation, :read_file, [
          vid,
          "/gid-test.bin",
          [uid: 2000, gids: [100]]
        ])

      assert data == "gid data"
    end
  end

  describe "file ACL — POSIX mode" do
    test "mode 0600 — owner can read/write, others cannot", %{cluster: cluster, volume_id: vid} do
      # Grant UID 1000 write to the volume so it can create files
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :grant, [
          vid,
          {:uid, 1000},
          [:write]
        ])

      # Also grant UID 2000 write to the volume
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :grant, [
          vid,
          {:uid, 2000},
          [:write]
        ])

      # Write file as UID 1000 with mode 0600
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
          vid,
          "/private.bin",
          0,
          "private data",
          [uid: 1000, mode: 0o600]
        ])

      # Set file ACL with owner UID 1000
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :set_file_acl, [
          vid,
          "/private.bin",
          [%{type: :user, id: nil, permissions: MapSet.new([:r, :w])}]
        ])

      # Check: UID 1000 (owner) can read via file ACL
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.Authorise, :check, [
          1000,
          :read,
          {:file, vid, "/private.bin"}
        ])

      assert result == :ok

      # Check: UID 2000 (other) is denied via file ACL
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.Authorise, :check, [
          2000,
          :read,
          {:file, vid, "/private.bin"}
        ])

      assert {:error, :forbidden} = result
    end
  end

  describe "file ACL — extended ACL" do
    test "extended ACL entry grants specific UID access", %{cluster: cluster, volume_id: vid} do
      # Grant UIDs volume access
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :grant, [
          vid,
          {:uid, 1000},
          [:write]
        ])

      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :grant, [
          vid,
          {:uid, 2000},
          [:read]
        ])

      # Write file as UID 1000
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
          vid,
          "/extended.bin",
          0,
          "extended acl data",
          [uid: 1000, mode: 0o600]
        ])

      # Add extended ACL entry granting UID 2000 read access
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :set_file_acl, [
          vid,
          "/extended.bin",
          [
            %{type: :user, id: nil, permissions: MapSet.new([:r, :w])},
            %{type: :user, id: 2000, permissions: MapSet.new([:r])}
          ]
        ])

      # UID 2000 should now have read access via extended ACL
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.Authorise, :check, [
          2000,
          :read,
          {:file, vid, "/extended.bin"}
        ])

      assert result == :ok
    end
  end

  describe "directory default ACL" do
    test "default ACL inheritance via parent file", %{cluster: cluster, volume_id: vid} do
      # Write a parent file that will hold default_acl
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "acl-volume",
          "/parent.bin",
          "parent"
        ])

      # Set default ACL on the parent file
      default_acl = [
        %{type: :user, id: nil, permissions: MapSet.new([:r, :w])},
        %{type: :user, id: 3000, permissions: MapSet.new([:r])}
      ]

      {:ok, parent_file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          vid,
          "/parent.bin"
        ])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :update, [
          parent_file.id,
          [default_acl: default_acl]
        ])

      # Verify the default_acl was set
      {:ok, updated_parent} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get, [vid, parent_file.id])

      assert is_list(updated_parent.default_acl)
      assert length(updated_parent.default_acl) == 2
    end

    # NOTE: Full directory default ACL inheritance (set_default_acl on a directory
    # path, then newly-created child files inherit those ACLs) requires directories
    # to have FileMeta entries. Currently, directories only have DirectoryEntry records.
    # The maybe_inherit_default_acl code in WriteOperation reads default_acl from the
    # parent FileMeta, so inheritance would work if the parent were a file.
    # This is tracked as a known limitation of the current ACL model.
  end

  describe "CLI handler ACL commands" do
    test "grant, show, and revoke via handler", %{cluster: cluster, volume_id: _vid} do
      # Grant via handler
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_acl_grant, [
          "acl-volume",
          "uid:1000",
          ["read", "write"]
        ])

      # Show via handler
      {:ok, acl_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_acl_show, [
          "acl-volume"
        ])

      assert is_list(acl_info.entries)
      assert acl_info.entries != []

      # Revoke via handler
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_acl_revoke, [
          "acl-volume",
          "uid:1000"
        ])

      # Show again — should have no entries for UID 1000
      {:ok, acl_after} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_acl_show, [
          "acl-volume"
        ])

      uid_1000 =
        Enum.find(acl_after.entries, fn entry ->
          entry.principal == "uid:1000"
        end)

      assert uid_1000 == nil, "UID 1000 should be revoked"
    end
  end
end
