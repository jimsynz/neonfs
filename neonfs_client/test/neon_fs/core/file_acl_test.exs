defmodule NeonFS.Core.FileACLTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.FileACL

  describe "new/1" do
    test "creates FileACL with required fields" do
      acl = FileACL.new(mode: 0o755, uid: 1000, gid: 1000)

      assert acl.mode == 0o755
      assert acl.uid == 1000
      assert acl.gid == 1000
      assert acl.acl_entries == []
    end

    test "creates FileACL with extended ACL entries" do
      entries = [
        %{type: :user, id: 1001, permissions: MapSet.new([:r, :w])},
        %{type: :mask, id: nil, permissions: MapSet.new([:r, :w])}
      ]

      acl = FileACL.new(mode: 0o644, uid: 1000, gid: 1000, acl_entries: entries)

      assert length(acl.acl_entries) == 2
    end
  end

  describe "check_access/4 - POSIX mode bits only" do
    test "owner can read with read bit set" do
      acl = FileACL.new(mode: 0o400, uid: 1000, gid: 1000)
      assert :ok = FileACL.check_access(acl, 1000, 1000, :r)
    end

    test "owner can write with write bit set" do
      acl = FileACL.new(mode: 0o200, uid: 1000, gid: 1000)
      assert :ok = FileACL.check_access(acl, 1000, 1000, :w)
    end

    test "owner can execute with execute bit set" do
      acl = FileACL.new(mode: 0o100, uid: 1000, gid: 1000)
      assert :ok = FileACL.check_access(acl, 1000, 1000, :x)
    end

    test "owner denied without matching bit" do
      acl = FileACL.new(mode: 0o077, uid: 1000, gid: 1000)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1000, 1000, :r)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1000, 1000, :w)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1000, 1000, :x)
    end

    test "group member can read with group read bit" do
      acl = FileACL.new(mode: 0o040, uid: 1000, gid: 100)
      assert :ok = FileACL.check_access(acl, 1001, 100, :r)
    end

    test "group member can write with group write bit" do
      acl = FileACL.new(mode: 0o020, uid: 1000, gid: 100)
      assert :ok = FileACL.check_access(acl, 1001, 100, :w)
    end

    test "group member can execute with group execute bit" do
      acl = FileACL.new(mode: 0o010, uid: 1000, gid: 100)
      assert :ok = FileACL.check_access(acl, 1001, 100, :x)
    end

    test "group member denied without matching group bit" do
      acl = FileACL.new(mode: 0o707, uid: 1000, gid: 100)
      # UID 1001 in group 100 — group bits are 0
      assert {:error, :forbidden} = FileACL.check_access(acl, 1001, 100, :r)
    end

    test "other user can read with other read bit" do
      acl = FileACL.new(mode: 0o004, uid: 1000, gid: 100)
      assert :ok = FileACL.check_access(acl, 2000, 200, :r)
    end

    test "other user can write with other write bit" do
      acl = FileACL.new(mode: 0o002, uid: 1000, gid: 100)
      assert :ok = FileACL.check_access(acl, 2000, 200, :w)
    end

    test "other user can execute with other execute bit" do
      acl = FileACL.new(mode: 0o001, uid: 1000, gid: 100)
      assert :ok = FileACL.check_access(acl, 2000, 200, :x)
    end

    test "other user denied without matching other bit" do
      acl = FileACL.new(mode: 0o770, uid: 1000, gid: 100)
      assert {:error, :forbidden} = FileACL.check_access(acl, 2000, 200, :r)
    end

    test "classic 755 permissions" do
      acl = FileACL.new(mode: 0o755, uid: 1000, gid: 100)

      # Owner: rwx
      assert :ok = FileACL.check_access(acl, 1000, 100, :r)
      assert :ok = FileACL.check_access(acl, 1000, 100, :w)
      assert :ok = FileACL.check_access(acl, 1000, 100, :x)

      # Group: r-x
      assert :ok = FileACL.check_access(acl, 1001, 100, :r)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1001, 100, :w)
      assert :ok = FileACL.check_access(acl, 1001, 100, :x)

      # Other: r-x
      assert :ok = FileACL.check_access(acl, 2000, 200, :r)
      assert {:error, :forbidden} = FileACL.check_access(acl, 2000, 200, :w)
      assert :ok = FileACL.check_access(acl, 2000, 200, :x)
    end

    test "classic 644 permissions" do
      acl = FileACL.new(mode: 0o644, uid: 1000, gid: 100)

      # Owner: rw-
      assert :ok = FileACL.check_access(acl, 1000, 100, :r)
      assert :ok = FileACL.check_access(acl, 1000, 100, :w)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1000, 100, :x)

      # Group: r--
      assert :ok = FileACL.check_access(acl, 1001, 100, :r)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1001, 100, :w)

      # Other: r--
      assert :ok = FileACL.check_access(acl, 2000, 200, :r)
      assert {:error, :forbidden} = FileACL.check_access(acl, 2000, 200, :w)
    end
  end

  describe "check_access/5 - supplementary GIDs" do
    test "supplementary GID matches owning group" do
      acl = FileACL.new(mode: 0o040, uid: 1000, gid: 100)
      # Primary GID 200, supplementary 100 (matches owning group)
      assert :ok = FileACL.check_access(acl, 1001, 200, [100], :r)
    end

    test "supplementary GID does not match" do
      acl = FileACL.new(mode: 0o040, uid: 1000, gid: 100)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1001, 200, [300], :r)
    end
  end

  describe "check_access with extended ACL entries" do
    test "named user entry grants access" do
      acl =
        FileACL.new(
          mode: 0o600,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :user, id: 1001, permissions: MapSet.new([:r, :w])},
            %{type: :mask, id: nil, permissions: MapSet.new([:r, :w, :x])}
          ]
        )

      assert :ok = FileACL.check_access(acl, 1001, 200, :r)
      assert :ok = FileACL.check_access(acl, 1001, 200, :w)
    end

    test "mask limits named user effective permissions" do
      acl =
        FileACL.new(
          mode: 0o600,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :user, id: 1001, permissions: MapSet.new([:r, :w, :x])},
            %{type: :mask, id: nil, permissions: MapSet.new([:r])}
          ]
        )

      # User 1001 has rwx in entry but mask is r only
      assert :ok = FileACL.check_access(acl, 1001, 200, :r)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1001, 200, :w)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1001, 200, :x)
    end

    test "mask does not affect owner permissions" do
      acl =
        FileACL.new(
          mode: 0o700,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :mask, id: nil, permissions: MapSet.new([:r])}
          ]
        )

      # Owner still has full access based on mode bits (mask doesn't apply)
      assert :ok = FileACL.check_access(acl, 1000, 100, :r)
      assert :ok = FileACL.check_access(acl, 1000, 100, :w)
      assert :ok = FileACL.check_access(acl, 1000, 100, :x)
    end

    test "mask limits owning group permissions when extended ACLs exist" do
      acl =
        FileACL.new(
          mode: 0o070,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :mask, id: nil, permissions: MapSet.new([:r])}
          ]
        )

      # Group mode is rwx but mask limits to r only
      assert :ok = FileACL.check_access(acl, 1001, 100, :r)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1001, 100, :w)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1001, 100, :x)
    end

    test "named group entry grants access through mask" do
      acl =
        FileACL.new(
          mode: 0o600,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :group, id: 200, permissions: MapSet.new([:r, :w])},
            %{type: :mask, id: nil, permissions: MapSet.new([:r, :w])}
          ]
        )

      assert :ok = FileACL.check_access(acl, 2000, 200, :r)
      assert :ok = FileACL.check_access(acl, 2000, 200, :w)
    end

    test "mask limits named group effective permissions" do
      acl =
        FileACL.new(
          mode: 0o600,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :group, id: 200, permissions: MapSet.new([:r, :w, :x])},
            %{type: :mask, id: nil, permissions: MapSet.new([:r])}
          ]
        )

      assert :ok = FileACL.check_access(acl, 2000, 200, :r)
      assert {:error, :forbidden} = FileACL.check_access(acl, 2000, 200, :w)
    end

    test "named group matches via supplementary GIDs" do
      acl =
        FileACL.new(
          mode: 0o600,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :group, id: 300, permissions: MapSet.new([:r])},
            %{type: :mask, id: nil, permissions: MapSet.new([:r, :w, :x])}
          ]
        )

      # Primary GID 200, supplementary includes 300
      assert :ok = FileACL.check_access(acl, 2000, 200, [300], :r)
    end

    test "no named user/group match falls through to other" do
      acl =
        FileACL.new(
          mode: 0o604,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :user, id: 1001, permissions: MapSet.new([:r, :w])},
            %{type: :mask, id: nil, permissions: MapSet.new([:r, :w])}
          ]
        )

      # UID 2000, GID 200 — no match on named user or any group → falls to other (r--)
      assert :ok = FileACL.check_access(acl, 2000, 200, :r)
      assert {:error, :forbidden} = FileACL.check_access(acl, 2000, 200, :w)
    end

    test "mask does not affect other permissions" do
      acl =
        FileACL.new(
          mode: 0o007,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :mask, id: nil, permissions: MapSet.new()}
          ]
        )

      # Other has rwx, mask is empty but should not affect other
      assert :ok = FileACL.check_access(acl, 2000, 200, :r)
      assert :ok = FileACL.check_access(acl, 2000, 200, :w)
      assert :ok = FileACL.check_access(acl, 2000, 200, :x)
    end
  end

  describe "POSIX ACL evaluation order" do
    test "owner match takes precedence over named user entry" do
      acl =
        FileACL.new(
          mode: 0o000,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :user, id: 1000, permissions: MapSet.new([:r, :w, :x])},
            %{type: :mask, id: nil, permissions: MapSet.new([:r, :w, :x])}
          ]
        )

      # Owner mode bits are 000 — owner check uses mode, not named user entry
      assert {:error, :forbidden} = FileACL.check_access(acl, 1000, 100, :r)
    end

    test "named user takes precedence over group" do
      acl =
        FileACL.new(
          mode: 0o070,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :user, id: 1001, permissions: MapSet.new()},
            %{type: :mask, id: nil, permissions: MapSet.new([:r, :w, :x])}
          ]
        )

      # UID 1001 is in owning group 100 and has a named user entry with no perms
      # Named user entry (empty) takes precedence over group (rwx)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1001, 100, :r)
    end

    test "group match takes precedence over other" do
      acl = FileACL.new(mode: 0o007, uid: 1000, gid: 100)

      # UID 1001 in group 100 — group bits are 000, other bits are rwx
      # Group match should be used (deny), not other (allow)
      assert {:error, :forbidden} = FileACL.check_access(acl, 1001, 100, :r)
    end
  end

  describe "validate/1" do
    test "accepts a valid FileACL" do
      acl = FileACL.new(mode: 0o755, uid: 1000, gid: 1000)
      assert :ok = FileACL.validate(acl)
    end

    test "accepts FileACL with extended entries" do
      acl =
        FileACL.new(
          mode: 0o644,
          uid: 1000,
          gid: 100,
          acl_entries: [
            %{type: :user, id: 1001, permissions: MapSet.new([:r, :w])},
            %{type: :group, id: 200, permissions: MapSet.new([:r])},
            %{type: :mask, id: nil, permissions: MapSet.new([:r, :w])},
            %{type: :other, id: nil, permissions: MapSet.new([:r])}
          ]
        )

      assert :ok = FileACL.validate(acl)
    end

    test "rejects mode out of range" do
      acl = %{FileACL.new(mode: 0o755, uid: 0, gid: 0) | mode: 0o10000}
      assert {:error, "mode must be an integer between 0 and 0o7777"} = FileACL.validate(acl)
    end

    test "rejects negative mode" do
      acl = %{FileACL.new(mode: 0o755, uid: 0, gid: 0) | mode: -1}
      assert {:error, "mode must be an integer between 0 and 0o7777"} = FileACL.validate(acl)
    end

    test "rejects negative uid" do
      acl = %{FileACL.new(mode: 0o755, uid: 0, gid: 0) | uid: -1}
      assert {:error, "uid must be a non-negative integer"} = FileACL.validate(acl)
    end

    test "rejects negative gid" do
      acl = %{FileACL.new(mode: 0o755, uid: 0, gid: 0) | gid: -1}
      assert {:error, "gid must be a non-negative integer"} = FileACL.validate(acl)
    end

    test "rejects mask entry with non-nil id" do
      acl = %{
        FileACL.new(mode: 0o755, uid: 0, gid: 0)
        | acl_entries: [%{type: :mask, id: 1, permissions: MapSet.new([:r])}]
      }

      assert {:error, "acl_entry 0: mask entry must have nil id"} = FileACL.validate(acl)
    end

    test "rejects other entry with non-nil id" do
      acl = %{
        FileACL.new(mode: 0o755, uid: 0, gid: 0)
        | acl_entries: [%{type: :other, id: 1, permissions: MapSet.new([:r])}]
      }

      assert {:error, "acl_entry 0: other entry must have nil id"} = FileACL.validate(acl)
    end

    test "rejects user entry with nil id" do
      acl = %{
        FileACL.new(mode: 0o755, uid: 0, gid: 0)
        | acl_entries: [%{type: :user, id: nil, permissions: MapSet.new([:r])}]
      }

      assert {:error, "acl_entry 0: user entry requires a non-nil id"} = FileACL.validate(acl)
    end

    test "rejects group entry with nil id" do
      acl = %{
        FileACL.new(mode: 0o755, uid: 0, gid: 0)
        | acl_entries: [%{type: :group, id: nil, permissions: MapSet.new([:r])}]
      }

      assert {:error, "acl_entry 0: group entry requires a non-nil id"} = FileACL.validate(acl)
    end

    test "rejects invalid ACL type" do
      acl = %{
        FileACL.new(mode: 0o755, uid: 0, gid: 0)
        | acl_entries: [%{type: :unknown, id: nil, permissions: MapSet.new()}]
      }

      assert {:error, "acl_entry 0: invalid ACL type:" <> _} = FileACL.validate(acl)
    end

    test "rejects invalid permissions in ACL entry" do
      acl = %{
        FileACL.new(mode: 0o755, uid: 0, gid: 0)
        | acl_entries: [%{type: :user, id: 1, permissions: MapSet.new([:read])}]
      }

      assert {:error, "acl_entry 0: invalid permissions:" <> _} = FileACL.validate(acl)
    end

    test "accepts UID and GID of zero (root)" do
      acl = FileACL.new(mode: 0o755, uid: 0, gid: 0)
      assert :ok = FileACL.validate(acl)
    end
  end
end
