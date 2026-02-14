defmodule NeonFS.Core.AuthoriseTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{ACLManager, Authorise, VolumeACL}

  setup do
    start_acl_manager()
    :ok
  end

  defp setup_acl(volume_id, owner_uid, owner_gid, entries \\ []) do
    acl =
      VolumeACL.new(
        volume_id: volume_id,
        owner_uid: owner_uid,
        owner_gid: owner_gid,
        entries: entries
      )

    :ok = ACLManager.set_volume_acl(volume_id, acl)
    acl
  end

  describe "check/3 root bypass" do
    test "UID 0 always returns :ok regardless of ACL" do
      assert :ok = Authorise.check(0, :read, {:volume, "no-such-volume"})
      assert :ok = Authorise.check(0, :write, {:volume, "no-such-volume"})
      assert :ok = Authorise.check(0, :admin, {:volume, "no-such-volume"})
      assert :ok = Authorise.check(0, :mount, {:volume, "no-such-volume"})
    end
  end

  describe "check/3 owner bypass" do
    test "owner UID always has all permissions" do
      setup_acl("vol-owner", 1000, 1000)

      assert :ok = Authorise.check(1000, :read, {:volume, "vol-owner"})
      assert :ok = Authorise.check(1000, :write, {:volume, "vol-owner"})
      assert :ok = Authorise.check(1000, :admin, {:volume, "vol-owner"})
      assert :ok = Authorise.check(1000, :mount, {:volume, "vol-owner"})
    end
  end

  describe "check/3 no ACL" do
    test "denies non-root users when no ACL exists" do
      assert {:error, :forbidden} = Authorise.check(1000, :read, {:volume, "no-acl-volume"})
    end
  end

  describe "check/3 direct UID matching" do
    test "grants access when UID has matching permission" do
      setup_acl("vol-uid", 500, 500, [
        %{principal: {:uid, 1000}, permissions: MapSet.new([:read])}
      ])

      assert :ok = Authorise.check(1000, :read, {:volume, "vol-uid"})
    end

    test "denies access when UID lacks permission" do
      setup_acl("vol-uid-deny", 500, 500, [
        %{principal: {:uid, 1000}, permissions: MapSet.new([:read])}
      ])

      assert {:error, :forbidden} = Authorise.check(1000, :write, {:volume, "vol-uid-deny"})
    end

    test "denies access when UID not in ACL entries" do
      setup_acl("vol-uid-missing", 500, 500, [
        %{principal: {:uid, 1000}, permissions: MapSet.new([:read])}
      ])

      assert {:error, :forbidden} = Authorise.check(2000, :read, {:volume, "vol-uid-missing"})
    end
  end

  describe "check/4 GID matching" do
    test "grants access via supplementary GID" do
      setup_acl("vol-gid", 500, 500, [
        %{principal: {:gid, 100}, permissions: MapSet.new([:write])}
      ])

      assert :ok = Authorise.check(1000, [100, 200], :write, {:volume, "vol-gid"})
    end

    test "denies when no supplementary GID matches" do
      setup_acl("vol-gid-deny", 500, 500, [
        %{principal: {:gid, 100}, permissions: MapSet.new([:write])}
      ])

      assert {:error, :forbidden} =
               Authorise.check(1000, [200, 300], :write, {:volume, "vol-gid-deny"})
    end
  end

  describe "permission inheritance" do
    test "admin implies write implies read" do
      setup_acl("vol-inherit", 500, 500, [
        %{principal: {:uid, 1000}, permissions: MapSet.new([:admin])}
      ])

      assert :ok = Authorise.check(1000, :admin, {:volume, "vol-inherit"})
      assert :ok = Authorise.check(1000, :write, {:volume, "vol-inherit"})
      assert :ok = Authorise.check(1000, :read, {:volume, "vol-inherit"})
    end

    test "write implies read but not admin" do
      setup_acl("vol-inherit-w", 500, 500, [
        %{principal: {:uid, 1000}, permissions: MapSet.new([:write])}
      ])

      assert :ok = Authorise.check(1000, :write, {:volume, "vol-inherit-w"})
      assert :ok = Authorise.check(1000, :read, {:volume, "vol-inherit-w"})
      assert {:error, :forbidden} = Authorise.check(1000, :admin, {:volume, "vol-inherit-w"})
    end

    test "read does not imply write or admin" do
      setup_acl("vol-inherit-r", 500, 500, [
        %{principal: {:uid, 1000}, permissions: MapSet.new([:read])}
      ])

      assert :ok = Authorise.check(1000, :read, {:volume, "vol-inherit-r"})
      assert {:error, :forbidden} = Authorise.check(1000, :write, {:volume, "vol-inherit-r"})
      assert {:error, :forbidden} = Authorise.check(1000, :admin, {:volume, "vol-inherit-r"})
    end
  end

  describe "mount action" do
    test "mount requires read permission" do
      setup_acl("vol-mount", 500, 500, [
        %{principal: {:uid, 1000}, permissions: MapSet.new([:read])}
      ])

      assert :ok = Authorise.check(1000, :mount, {:volume, "vol-mount"})
    end

    test "mount denied without read permission" do
      setup_acl("vol-mount-deny", 500, 500)

      assert {:error, :forbidden} = Authorise.check(1000, :mount, {:volume, "vol-mount-deny"})
    end
  end

  describe "telemetry events" do
    test "emits :granted event on success" do
      setup_acl("vol-tele-ok", 1000, 1000)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :authorise, :granted]
        ])

      Authorise.check(1000, :read, {:volume, "vol-tele-ok"})

      assert_received {[:neonfs, :authorise, :granted], ^ref, %{},
                       %{uid: 1000, action: :read, resource: {:volume, "vol-tele-ok"}}}
    end

    test "emits :denied event on failure" do
      setup_acl("vol-tele-deny", 500, 500)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :authorise, :denied]
        ])

      Authorise.check(1000, :read, {:volume, "vol-tele-deny"})

      assert_received {[:neonfs, :authorise, :denied], ^ref, %{},
                       %{uid: 1000, action: :read, resource: {:volume, "vol-tele-deny"}}}
    end
  end
end
