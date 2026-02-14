defmodule NeonFS.Core.VolumeACLTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.VolumeACL

  describe "new/1" do
    test "creates ACL with required fields" do
      acl = VolumeACL.new(volume_id: "vol-1", owner_uid: 1000, owner_gid: 1000)

      assert acl.volume_id == "vol-1"
      assert acl.owner_uid == 1000
      assert acl.owner_gid == 1000
      assert acl.entries == []
    end

    test "creates ACL with entries" do
      entries = [
        %{principal: {:uid, 1001}, permissions: MapSet.new([:read, :write])},
        %{principal: {:gid, 100}, permissions: MapSet.new([:read])}
      ]

      acl = VolumeACL.new(volume_id: "vol-1", owner_uid: 1000, owner_gid: 1000, entries: entries)

      assert length(acl.entries) == 2
    end
  end

  describe "has_permission?/3" do
    test "owner always has all permissions" do
      acl = VolumeACL.new(volume_id: "vol-1", owner_uid: 1000, owner_gid: 1000)

      assert VolumeACL.has_permission?(acl, 1000, :read)
      assert VolumeACL.has_permission?(acl, 1000, :write)
      assert VolumeACL.has_permission?(acl, 1000, :admin)
    end

    test "non-owner without entries has no permissions" do
      acl = VolumeACL.new(volume_id: "vol-1", owner_uid: 1000, owner_gid: 1000)

      refute VolumeACL.has_permission?(acl, 1001, :read)
      refute VolumeACL.has_permission?(acl, 1001, :write)
      refute VolumeACL.has_permission?(acl, 1001, :admin)
    end

    test "direct UID entry grants permission" do
      acl =
        VolumeACL.new(
          volume_id: "vol-1",
          owner_uid: 1000,
          owner_gid: 1000,
          entries: [
            %{principal: {:uid, 1001}, permissions: MapSet.new([:read, :write])}
          ]
        )

      assert VolumeACL.has_permission?(acl, 1001, :read)
      assert VolumeACL.has_permission?(acl, 1001, :write)
      refute VolumeACL.has_permission?(acl, 1001, :admin)
    end

    test "UID entry does not grant to other UIDs" do
      acl =
        VolumeACL.new(
          volume_id: "vol-1",
          owner_uid: 1000,
          owner_gid: 1000,
          entries: [
            %{principal: {:uid, 1001}, permissions: MapSet.new([:read])}
          ]
        )

      refute VolumeACL.has_permission?(acl, 1002, :read)
    end
  end

  describe "has_permission?/4 with supplementary GIDs" do
    test "GID entry grants permission via supplementary groups" do
      acl =
        VolumeACL.new(
          volume_id: "vol-1",
          owner_uid: 1000,
          owner_gid: 1000,
          entries: [
            %{principal: {:gid, 200}, permissions: MapSet.new([:read])}
          ]
        )

      assert VolumeACL.has_permission?(acl, 1001, [200, 300], :read)
      refute VolumeACL.has_permission?(acl, 1001, [300, 400], :read)
    end

    test "GID entry does not match UIDs" do
      acl =
        VolumeACL.new(
          volume_id: "vol-1",
          owner_uid: 1000,
          owner_gid: 1000,
          entries: [
            %{principal: {:gid, 1001}, permissions: MapSet.new([:read])}
          ]
        )

      # UID 1001 does not match GID 1001 without supplementary groups
      refute VolumeACL.has_permission?(acl, 1001, :read)
    end

    test "multiple entries combine permissions" do
      acl =
        VolumeACL.new(
          volume_id: "vol-1",
          owner_uid: 1000,
          owner_gid: 1000,
          entries: [
            %{principal: {:uid, 1001}, permissions: MapSet.new([:read])},
            %{principal: {:gid, 200}, permissions: MapSet.new([:write])}
          ]
        )

      # UID 1001 in group 200 gets read (from uid entry) + write (from gid entry)
      assert VolumeACL.has_permission?(acl, 1001, [200], :read)
      assert VolumeACL.has_permission?(acl, 1001, [200], :write)
      refute VolumeACL.has_permission?(acl, 1001, [200], :admin)
    end
  end

  describe "permission inheritance" do
    test "admin implies write" do
      acl =
        VolumeACL.new(
          volume_id: "vol-1",
          owner_uid: 1000,
          owner_gid: 1000,
          entries: [
            %{principal: {:uid, 1001}, permissions: MapSet.new([:admin])}
          ]
        )

      assert VolumeACL.has_permission?(acl, 1001, :write)
    end

    test "admin implies read" do
      acl =
        VolumeACL.new(
          volume_id: "vol-1",
          owner_uid: 1000,
          owner_gid: 1000,
          entries: [
            %{principal: {:uid, 1001}, permissions: MapSet.new([:admin])}
          ]
        )

      assert VolumeACL.has_permission?(acl, 1001, :read)
    end

    test "write implies read" do
      acl =
        VolumeACL.new(
          volume_id: "vol-1",
          owner_uid: 1000,
          owner_gid: 1000,
          entries: [
            %{principal: {:uid, 1001}, permissions: MapSet.new([:write])}
          ]
        )

      assert VolumeACL.has_permission?(acl, 1001, :read)
    end

    test "read does not imply write" do
      acl =
        VolumeACL.new(
          volume_id: "vol-1",
          owner_uid: 1000,
          owner_gid: 1000,
          entries: [
            %{principal: {:uid, 1001}, permissions: MapSet.new([:read])}
          ]
        )

      refute VolumeACL.has_permission?(acl, 1001, :write)
    end

    test "write does not imply admin" do
      acl =
        VolumeACL.new(
          volume_id: "vol-1",
          owner_uid: 1000,
          owner_gid: 1000,
          entries: [
            %{principal: {:uid, 1001}, permissions: MapSet.new([:write])}
          ]
        )

      refute VolumeACL.has_permission?(acl, 1001, :admin)
    end

    test "permission inheritance holds for various UIDs" do
      for uid <- [1001, 2000, 5000, 9999, 65_534] do
        acl_admin =
          VolumeACL.new(
            volume_id: "vol-1",
            owner_uid: 1000,
            owner_gid: 1000,
            entries: [%{principal: {:uid, uid}, permissions: MapSet.new([:admin])}]
          )

        acl_write =
          VolumeACL.new(
            volume_id: "vol-1",
            owner_uid: 1000,
            owner_gid: 1000,
            entries: [%{principal: {:uid, uid}, permissions: MapSet.new([:write])}]
          )

        acl_read =
          VolumeACL.new(
            volume_id: "vol-1",
            owner_uid: 1000,
            owner_gid: 1000,
            entries: [%{principal: {:uid, uid}, permissions: MapSet.new([:read])}]
          )

        # admin grants all three
        assert VolumeACL.has_permission?(acl_admin, uid, :admin)
        assert VolumeACL.has_permission?(acl_admin, uid, :write)
        assert VolumeACL.has_permission?(acl_admin, uid, :read)

        # write grants write + read but not admin
        refute VolumeACL.has_permission?(acl_write, uid, :admin)
        assert VolumeACL.has_permission?(acl_write, uid, :write)
        assert VolumeACL.has_permission?(acl_write, uid, :read)

        # read grants read only
        refute VolumeACL.has_permission?(acl_read, uid, :admin)
        refute VolumeACL.has_permission?(acl_read, uid, :write)
        assert VolumeACL.has_permission?(acl_read, uid, :read)
      end
    end
  end

  describe "validate/1" do
    test "accepts a valid ACL" do
      acl =
        VolumeACL.new(
          volume_id: "vol-1",
          owner_uid: 1000,
          owner_gid: 1000,
          entries: [
            %{principal: {:uid, 1001}, permissions: MapSet.new([:read])}
          ]
        )

      assert :ok = VolumeACL.validate(acl)
    end

    test "accepts ACL with no entries" do
      acl = VolumeACL.new(volume_id: "vol-1", owner_uid: 0, owner_gid: 0)
      assert :ok = VolumeACL.validate(acl)
    end

    test "rejects empty volume_id" do
      acl = %{VolumeACL.new(volume_id: "x", owner_uid: 0, owner_gid: 0) | volume_id: ""}
      assert {:error, "volume_id must be a non-empty string"} = VolumeACL.validate(acl)
    end

    test "rejects negative owner_uid" do
      acl = %{VolumeACL.new(volume_id: "v", owner_uid: 0, owner_gid: 0) | owner_uid: -1}
      assert {:error, "owner_uid must be a non-negative integer"} = VolumeACL.validate(acl)
    end

    test "rejects negative owner_gid" do
      acl = %{VolumeACL.new(volume_id: "v", owner_uid: 0, owner_gid: 0) | owner_gid: -1}
      assert {:error, "owner_gid must be a non-negative integer"} = VolumeACL.validate(acl)
    end

    test "rejects entry with invalid principal" do
      acl = %{
        VolumeACL.new(volume_id: "v", owner_uid: 0, owner_gid: 0)
        | entries: [%{principal: {:user, "alice"}, permissions: MapSet.new([:read])}]
      }

      assert {:error, "entry 0: invalid principal:" <> _} = VolumeACL.validate(acl)
    end

    test "rejects entry with invalid permissions" do
      acl = %{
        VolumeACL.new(volume_id: "v", owner_uid: 0, owner_gid: 0)
        | entries: [%{principal: {:uid, 1}, permissions: MapSet.new([:delete])}]
      }

      assert {:error, "entry 0: invalid permissions:" <> _} = VolumeACL.validate(acl)
    end

    test "rejects entry with non-MapSet permissions" do
      acl = %{
        VolumeACL.new(volume_id: "v", owner_uid: 0, owner_gid: 0)
        | entries: [%{principal: {:uid, 1}, permissions: [:read]}]
      }

      assert {:error, "entry 0: permissions must be a MapSet"} = VolumeACL.validate(acl)
    end
  end
end
