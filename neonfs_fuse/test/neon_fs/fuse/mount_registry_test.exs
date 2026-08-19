defmodule NeonFS.FUSE.MountRegistryTest do
  use ExUnit.Case, async: false

  alias NeonFS.FUSE.{MountInfo, MountRegistry}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "fuse_mounts.json")
    Application.put_env(:neonfs_fuse, :mount_registry_path, path)

    on_exit(fn ->
      Application.put_env(
        :neonfs_fuse,
        :mount_registry_path,
        Path.join(System.tmp_dir!(), "neonfs_fuse_test_mounts.json")
      )
    end)

    {:ok, path: path}
  end

  test "a host that has never mounted anything has an empty registry, not an error" do
    assert {:ok, []} = MountRegistry.load()
  end

  test "an entry round-trips with the options a remount needs" do
    entry =
      MountRegistry.entry(
        MountInfo.new(
          id: "mount_abc123",
          volume_name: "vol-a",
          mount_point: "/mnt/vol-a",
          started_at: ~U[2026-08-19 03:00:00Z],
          mount_session: nil,
          opts: [ro: true, allow_other: true, atime_mode: :relatime]
        )
      )

    assert :ok = MountRegistry.save([entry])
    assert {:ok, [loaded]} = MountRegistry.load()

    assert loaded.id == "mount_abc123"
    assert loaded.volume_name == "vol-a"
    assert loaded.mount_point == "/mnt/vol-a"
    assert loaded.mounted_at == ~U[2026-08-19 03:00:00Z]
    assert Enum.sort(loaded.opts) == [allow_other: true, atime_mode: :relatime, ro: true]
  end

  # A caller's incidental options are not the mount's identity, and carrying
  # them forward would replay them into a remount made on a later daemon
  # version that may mean something different by them.
  test "options that do not describe the filesystem are not recorded" do
    entry =
      MountRegistry.entry(
        MountInfo.new(
          id: "mount_abc123",
          volume_name: "vol-a",
          mount_point: "/mnt/vol-a",
          started_at: DateTime.utc_now(),
          mount_session: nil,
          opts: [ro: true, request_id: "req-1", caller: self()]
        )
      )

    assert entry.opts == [ro: true]
  end

  # The registry is host state an operator can edit, so its keys reach
  # `decode` as untrusted input. Turning them into atoms would leak the atom
  # table to anyone who can write the file.
  test "an unrecognised option key is dropped rather than made into an atom", %{path: path} do
    File.write!(path, ~s({"mounts":[{"id":"m","volume_name":"v","mount_point":"/mnt/v",) <>
      ~s("opts":{"ro":true,"totally_made_up_key_9f3a":1},"mounted_at":"2026-08-19T03:00:00Z"}]}))

    assert {:ok, [loaded]} = MountRegistry.load()
    assert loaded.opts == [ro: true]

    assert_raise ArgumentError, fn -> String.to_existing_atom("totally_made_up_key_9f3a") end
  end

  # Silently reading a truncated file as "no mounts" is the same outcome as an
  # operator having unmounted everything, and the caller has to be able to tell
  # those apart before it decides to recover nothing.
  test "a malformed registry is reported rather than read as empty", %{path: path} do
    File.write!(path, "{\"mounts\": [")

    assert {:error, _reason} = MountRegistry.load()
  end

  test "a well-formed file that is not a registry is reported", %{path: path} do
    File.write!(path, ~s({"something": "else"}))

    assert {:error, :invalid_registry} = MountRegistry.load()
  end

  test "saving replaces the previous contents", %{path: path} do
    entry = fn id ->
      MountRegistry.entry(
        MountInfo.new(
          id: id,
          volume_name: "vol-#{id}",
          mount_point: "/mnt/#{id}",
          started_at: DateTime.utc_now(),
          mount_session: nil
        )
      )
    end

    assert :ok = MountRegistry.save([entry.("a"), entry.("b")])
    assert :ok = MountRegistry.save([entry.("c")])

    assert {:ok, [%{id: "c"}]} = MountRegistry.load()
    refute File.exists?(path <> ".tmp")
  end
end
