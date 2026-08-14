defmodule NeonFS.CIFS.HandlerTest do
  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.CIFS.{ConnectionHandler, Handler, HandleRegistry}
  alias NeonFS.Client.ChunkReader
  alias NeonFS.Error.{AlreadyExists, FileNotFound}

  @file_id "019dc5d8-4000-7000-8000-000000000001"
  @volume_id "019dc5d8-3fcf-7d13-b4fa-832c4390b0a0"

  setup :verify_on_exit!

  # File handles live in the node-wide registry now, so it has to be running
  # and Mimic has to reach the calls it makes on the caller's behalf.
  setup do
    start_supervised!(HandleRegistry)
    Mimic.allow(NeonFS.Client, self(), Process.whereis(HandleRegistry))
    :ok
  end

  # Taken from the production initial state rather than restated. Restating
  # it is what hid the `:disconnect` defect: this literal had drifted the
  # same way that one had, so a test asserting the reset state equalled it
  # asserted the two agreed on being wrong.
  defp blank_state do
    {:continue, state} = ConnectionHandler.handle_connection(nil, nil)
    state
  end

  defp connected do
    {{:ok, _}, state} = Handler.handle({:connect, %{"volume" => "vol-a"}}, blank_state())
    state
  end

  defp file_meta(path, attrs \\ []) do
    defaults = %{
      id: if(path == "/", do: nil, else: "object:" <> path),
      volume_id: @volume_id,
      path: path,
      size: 0,
      mode: 0o100644
    }

    Map.merge(defaults, Map.new(attrs))
  end

  defp not_found(path), do: FileNotFound.exception(file_path: path, volume_id: "vol-a")

  # Opening the share root, the way smbd's pathref open does: the path
  # already exists, so there is no create, and `get_file_meta` answers a
  # directory whose id is nil.
  defp open_root(state) do
    expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/"] ->
      {:ok, file_meta("/", mode: 0o40755)}
    end)

    expect(NeonFS.Client, :core_call, fn NeonFS.Core, :pin_file, ["vol-a", "/", _holder] ->
      {:ok, %{claim_id: "claim:/", file_id: nil}}
    end)

    {{:ok, %{handle: handle}}, state} =
      Handler.handle({:openat, %{"path" => "/", "flags" => 0, "mode" => 0}}, state)

    {handle, state}
  end

  # Opening an existing directory, the way smbd does after `mkdirat`.
  defp open_directory(state, path) do
    expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", ^path] ->
      {:ok, file_meta(path, id: "object:" <> path, mode: 0o40755)}
    end)

    expect(NeonFS.Client, :core_call, fn NeonFS.Core, :pin_file, ["vol-a", ^path, _holder] ->
      {:ok, %{claim_id: "claim:" <> path, file_id: "object:" <> path}}
    end)

    {{:ok, %{handle: handle}}, state} =
      Handler.handle({:openat, %{"path" => path, "flags" => 0, "mode" => 0}}, state)

    {handle, state}
  end

  defp open_file(state, path, flags \\ 0o100) do
    expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", ^path] ->
      {:error, not_found(path)}
    end)

    expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                         :write_file_at,
                                         ["vol-a", ^path, 0, <<>>, [mode: 420]] ->
      {:ok, file_meta(path)}
    end)

    # `openat` takes the identity pin before registering the handle.
    expect(NeonFS.Client, :core_call, fn NeonFS.Core, :pin_file, ["vol-a", ^path, _holder] ->
      {:ok, %{claim_id: "claim:" <> path, file_id: "object:" <> path}}
    end)

    {{:ok, %{handle: handle}}, state} =
      Handler.handle({:openat, %{"path" => path, "flags" => flags, "mode" => 0o644}}, state)

    {handle, state}
  end

  defp open_dir(state, path) do
    stub(NeonFS.Client, :core_call, fn
      NeonFS.Core, :get_file_meta, ["vol-a", ^path] -> {:ok, file_meta(path, mode: 0o040755)}
      NeonFS.Core, :list_dir, ["vol-a", ^path] -> {:ok, []}
    end)

    {{:ok, %{handle: handle}}, state} = Handler.handle({:fdopendir, %{"path" => path}}, state)
    {handle, state}
  end

  describe "lifecycle" do
    test "connect binds the volume to the connection state" do
      state = blank_state()
      {reply, new} = Handler.handle({:connect, %{"volume" => "vol-a"}}, state)
      assert {:ok, %{}} == reply
      assert new.volume == "vol-a"
    end

    test "ops before connect return :enotconn" do
      {reply, _} = Handler.handle({:stat, %{"path" => "/foo"}}, blank_state())
      assert {:error, :enotconn} == reply
    end

    test "disconnect resets the connection state" do
      state = connected()
      {reply, new} = Handler.handle({:disconnect, %{}}, state)
      assert {:ok, %{}} == reply
      assert new == blank_state()
    end

    # `handle_close/2` is the sweep that releases handles the C shim left
    # open. It reads `state.files`, so a reset state missing that key made
    # it raise on its first line — skipping the sweep, and doing it inside
    # `terminate/2` where a normal teardown then reads as a crash.
    test "the state a disconnect leaves behind can still be swept on close" do
      {_reply, reset} = Handler.handle({:disconnect, %{}}, connected())

      assert :ok = ConnectionHandler.handle_close(nil, reset)
    end
  end

  describe "metadata" do
    test "stat fetches via core_call and translates to the Samba shape" do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/foo"] ->
        {:ok,
         file_meta("/foo",
           id: @file_id,
           size: 13,
           uid: 1000,
           gid: 1001,
           accessed_at: 100,
           modified_at: 200,
           changed_at: 300
         )}
      end)

      {reply, _} = Handler.handle({:stat, %{"path" => "/foo"}}, connected())

      assert {:ok,
              %{
                stat: %{
                  dev: 0x6AD05F36A5D262B7,
                  ino: 0x957C881D9661B59D,
                  size: 13,
                  mode: 0o100644,
                  uid: 1000,
                  gid: 1001,
                  atime: 100,
                  mtime: 200,
                  ctime: 300,
                  kind: :file
                }
              }} = reply
    end

    # smbd runs its own access check against the stat before dispatching a
    # create to any VFS hook, so ownership omitted here denies every non-root
    # session at `smb2_create` — which reads as the module never being
    # consulted, when in fact it answered and said root.
    test "stat reports the file's real ownership rather than root" do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/foo"] ->
        {:ok, file_meta("/foo", id: @file_id, uid: 65_534, gid: 65_533)}
      end)

      {{:ok, %{stat: stat}}, _} = Handler.handle({:stat, %{"path" => "/foo"}}, connected())

      assert stat.uid == 65_534
      assert stat.gid == 65_533
    end

    # A `FileMeta` predating ownership on the wire has no uid to report, and
    # root is the only defensible default — but it is a default, not a fact,
    # so it must not be mistaken for ownership the volume actually recorded.
    test "stat falls back to root when the metadata carries no ownership" do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/foo"] ->
        {:ok, %{id: @file_id, volume_id: @volume_id, path: "/foo", size: 0, mode: 0o100644}}
      end)

      {{:ok, %{stat: stat}}, _} = Handler.handle({:stat, %{"path" => "/foo"}}, connected())

      assert stat.uid == 0
      assert stat.gid == 0
    end

    test "stat rejects metadata without stable object identity" do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/foo"] ->
        {:ok, %{path: "/foo", size: 0, mode: 0o100644}}
      end)

      {reply, _} = Handler.handle({:stat, %{"path" => "/foo"}}, connected())
      assert {:error, :eio} == reply
    end

    test "stat identity follows the object rather than its path" do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", path] ->
        id = if path == "/replacement", do: "replacement-id", else: @file_id
        {:ok, file_meta(path, id: id)}
      end)

      {{:ok, %{stat: original}}, _} =
        Handler.handle({:stat, %{"path" => "/original"}}, connected())

      {{:ok, %{stat: renamed}}, _} = Handler.handle({:stat, %{"path" => "/renamed"}}, connected())

      {{:ok, %{stat: replacement}}, _} =
        Handler.handle({:stat, %{"path" => "/replacement"}}, connected())

      assert {original.dev, original.ino} == {renamed.dev, renamed.ino}
      assert original.dev == replacement.dev
      refute original.ino == replacement.ino
    end

    test "stat ENOENT maps a not_found-class error to :enoent" do
      stub(NeonFS.Client, :core_call, fn _, _, _ -> {:error, not_found("/missing")} end)
      {reply, _} = Handler.handle({:stat, %{"path" => "/missing"}}, connected())
      assert {:error, :enoent} == reply
    end

    test "lstat falls through to stat for now" do
      stub(NeonFS.Client, :core_call, fn _, _, _ ->
        {:ok, file_meta("/x", modified_at: 1, changed_at: 1, accessed_at: 1)}
      end)

      {reply1, _} = Handler.handle({:stat, %{"path" => "/x"}}, connected())
      {reply2, _} = Handler.handle({:lstat, %{"path" => "/x"}}, connected())
      assert reply1 == reply2
    end

    test "fstat resolves through the open-files table" do
      {handle, state} = open_file(connected(), "/foo")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :get_file_meta_by_id,
                                           ["vol-a", "object:/foo"] ->
        {:ok, file_meta("/foo", size: 99)}
      end)

      {reply, _} = Handler.handle({:fstat, %{"handle" => handle}}, state)
      assert match?({:ok, %{stat: %{size: 99}}}, reply)
    end

    # smbd opens a pathref on the share root and stats it *through that
    # handle* on every tree connect (`openat_pathref_fsp_dot` →
    # `vfs_stat_fsp` → `fstat_fn`). The root's `FileMeta` carries
    # `id: nil`, so a by-id lookup has nothing to look up: it answered an
    # error that reached smbd as EIO, and every SMB2 create then failed
    # with OBJECT_PATH_NOT_FOUND. Nothing could be done on any share.
    test "fstat on the share root resolves by name, since the root has no id" do
      {handle, state} = open_root(connected())

      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/"] ->
        {:ok, file_meta("/", mode: 0o40755)}
      end)

      {reply, _} = Handler.handle({:fstat, %{"handle" => handle}}, state)

      assert match?({:ok, %{stat: %{kind: :directory}}}, reply)
    end

    test "fchmod on the share root goes by name too" do
      {handle, state} = open_root(connected())

      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :update_file_meta,
                                           ["vol-a", "/", [mode: 0o40700]] ->
        {:ok, %{}}
      end)

      {reply, _} = Handler.handle({:fchmod, %{"handle" => handle, "mode" => 0o40700}}, state)

      assert {:ok, %{}} == reply
    end

    test "a handle that does have an id still resolves by identity" do
      {handle, state} = open_file(connected(), "/keeps-identity.txt")

      # Not by path: resolving by name is what a rename breaks, and is why
      # handles are identity-keyed in the first place. The root is the
      # exception, not the rule.
      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :get_file_meta_by_id,
                                           ["vol-a", "object:/keeps-identity.txt"] ->
        {:ok, file_meta("/keeps-identity.txt", size: 7)}
      end)

      {reply, _} = Handler.handle({:fstat, %{"handle" => handle}}, state)
      assert match?({:ok, %{stat: %{size: 7}}}, reply)
    end

    # A directory has an id, and it is not one `get_file_meta_by_id` can
    # resolve — directories live as dirents and only regular files are in
    # the by-id index. Keying a directory handle by that id mints a handle
    # whose every later op answers ENOENT, which is what smbd hit creating
    # a directory: it opens what it just made and stats it through that
    # handle before renaming it into place.
    test "a directory handle resolves by name even though the directory has an id" do
      {handle, state} = open_directory(connected(), "/adir")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/adir"] ->
        {:ok, file_meta("/adir", id: "object:/adir", mode: 0o40755)}
      end)

      {reply, _} = Handler.handle({:fstat, %{"handle" => handle}}, state)

      assert match?({:ok, %{stat: %{kind: :directory}}}, reply)
    end

    test "fstat on an unknown handle is :ebadf" do
      {reply, _} = Handler.handle({:fstat, %{"handle" => 9999}}, connected())
      assert {:error, :ebadf} == reply
    end

    test "fchmod updates the mode via update_file_meta" do
      {handle, state} = open_file(connected(), "/p")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :update_file_meta_by_id,
                                           ["vol-a", "object:/p", [mode: 0o600]] ->
        {:ok, %{}}
      end)

      {reply, _} =
        Handler.handle({:fchmod, %{"handle" => handle, "mode" => 0o600}}, state)

      assert {:ok, %{}} == reply
    end

    # `fchmod` used to resolve only through the file-handle registry, so a
    # directory handle — minted separately by `fdopendir` — missed it and
    # returned `:ebadf` before any core call. A directory's mode lives in a
    # path-keyed record, so the handle has to carry its path.
    test "fchmod on a directory handle updates by path" do
      {handle, state} = open_dir(connected(), "/d")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :update_file_meta,
                                           ["vol-a", "/d", [mode: 0o750]] ->
        {:ok, %{}}
      end)

      {reply, _} = Handler.handle({:fchmod, %{"handle" => handle, "mode" => 0o750}}, state)

      assert {:ok, %{}} == reply
    end

    test "fntimes on a directory handle updates by path" do
      {handle, state} = open_dir(connected(), "/d")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :update_file_meta, ["vol-a", "/d", ups] ->
        assert Keyword.has_key?(ups, :accessed_at)
        assert Keyword.has_key?(ups, :modified_at)
        {:ok, %{}}
      end)

      {reply, _} =
        Handler.handle(
          {:fntimes, %{"handle" => handle, "atime" => 1_600_000_000, "mtime" => 1_600_000_001}},
          state
        )

      assert {:ok, %{}} == reply
    end

    test "fstat on a directory handle resolves by path" do
      {handle, state} = open_dir(connected(), "/d")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/d"] ->
        {:ok, file_meta("/d", mode: 0o040755)}
      end)

      {reply, _} = Handler.handle({:fstat, %{"handle" => handle}}, state)

      assert {:ok, %{stat: _}} = reply
    end

    test "fchown is :enosys until the IAM bridge lands" do
      {handle, state} = open_file(connected(), "/p")

      {reply, _} =
        Handler.handle(
          {:fchown, %{"handle" => handle, "uid" => 1000, "gid" => 1000}},
          state
        )

      assert {:error, :enosys} == reply
    end

    test "fntimes updates atime+mtime via update_file_meta" do
      {handle, state} = open_file(connected(), "/p")

      stub(NeonFS.Client, :core_call, fn NeonFS.Core,
                                         :update_file_meta_by_id,
                                         ["vol-a", "object:/p", updates] ->
        send(self(), {:times, updates})
        {:ok, %{}}
      end)

      {reply, _} =
        Handler.handle({:fntimes, %{"handle" => handle, "atime" => 100, "mtime" => 200}}, state)

      assert {:ok, %{}} == reply
      assert_received {:times, updates}
      assert Keyword.get(updates, :accessed_at) == DateTime.from_unix!(100)
      assert Keyword.get(updates, :modified_at) == DateTime.from_unix!(200)
    end
  end

  describe "file I/O" do
    test "openat creates if missing and mints a handle" do
      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core, :get_file_meta, ["vol-a", "/new"] ->
          {:error, not_found("/new")}

        NeonFS.Core, :write_file_at, ["vol-a", "/new", 0, <<>>, [mode: 420]] ->
          {:ok, file_meta("/new")}

        NeonFS.Core, :pin_file, ["vol-a", "/new", _holder] ->
          {:ok, %{claim_id: "claim:/new"}}
      end)

      {reply, _state} =
        Handler.handle(
          {:openat, %{"path" => "/new", "flags" => 0o100, "mode" => 0o644}},
          connected()
        )

      assert {:ok, %{handle: handle}} = reply
      assert {:ok, %{volume: "vol-a", file_id: "object:/new"}} = HandleRegistry.fetch(handle)
    end

    test "openat with O_EXCL on an existing file is :eexist" do
      stub(NeonFS.Client, :core_call, fn _, _, _ ->
        {:ok, %{path: "/existing"}}
      end)

      {reply, _} =
        Handler.handle(
          {:openat, %{"path" => "/existing", "flags" => 0o300, "mode" => 0o644}},
          connected()
        )

      assert {:error, :eexist} == reply
    end

    # `O_EXCL | O_CREAT` (0o300) routes through `write_file_at` with
    # `create_only: true`. The interface-side
    # get_file_meta precheck only catches the trivial case where the
    # file is already on disk; concurrent creates on different CIFS
    # nodes are fenced by the `claim_create` primitive on the core
    # node, and the loser sees `{:error, :exists}` which this handler
    # maps to `:eexist`.
    test "openat with O_EXCL | O_CREAT on missing file forwards create_only: true" do
      test_pid = self()

      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core, :get_file_meta, ["vol-a", "/atomic"] ->
          {:error, not_found("/atomic")}

        NeonFS.Core, :write_file_at, ["vol-a", "/atomic", 0, <<>>, opts] ->
          send(test_pid, {:write_opts, opts})
          {:ok, file_meta("/atomic")}

        NeonFS.Core, :pin_file, ["vol-a", "/atomic", _holder] ->
          {:ok, %{claim_id: "claim:/atomic"}}
      end)

      {reply, _state} =
        Handler.handle(
          {:openat, %{"path" => "/atomic", "flags" => 0o300, "mode" => 0o644}},
          connected()
        )

      assert {:ok, %{handle: handle}} = reply
      assert {:ok, %{file_id: "object:/atomic"}} = HandleRegistry.fetch(handle)
      assert_receive {:write_opts, opts}, 500
      assert Keyword.get(opts, :create_only) == true
    end

    test "openat with O_EXCL | O_CREAT maps :exists from core to :eexist" do
      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core, :get_file_meta, ["vol-a", "/raced"] ->
          {:error, not_found("/raced")}

        NeonFS.Core, :write_file_at, ["vol-a", "/raced", 0, <<>>, opts] ->
          # The peer-cluster integration test for the underlying
          # primitive lives elsewhere; here we just verify the
          # interface-level translation.
          assert Keyword.get(opts, :create_only) == true
          {:error, AlreadyExists.from_reason(:exists)}
      end)

      {reply, _} =
        Handler.handle(
          {:openat, %{"path" => "/raced", "flags" => 0o300, "mode" => 0o644}},
          connected()
        )

      assert {:error, :eexist} == reply
    end

    test "openat with O_CREAT only (no O_EXCL) does not set create_only" do
      test_pid = self()

      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core, :get_file_meta, ["vol-a", "/plain"] ->
          {:error, not_found("/plain")}

        NeonFS.Core, :write_file_at, ["vol-a", "/plain", 0, <<>>, opts] ->
          send(test_pid, {:write_opts, opts})
          {:ok, file_meta("/plain")}

        NeonFS.Core, :pin_file, ["vol-a", "/plain", _holder] ->
          {:ok, %{claim_id: "claim:/plain"}}
      end)

      {reply, _} =
        Handler.handle(
          {:openat, %{"path" => "/plain", "flags" => 0o100, "mode" => 0o644}},
          connected()
        )

      assert {:ok, %{handle: _}} = reply
      assert_receive {:write_opts, opts}, 500
      refute Keyword.get(opts, :create_only)
    end

    test "close releases the handle" do
      {handle, state} = open_file(connected(), "/p")

      # Closing releases the identity pin exactly once.
      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :unpin_file, ["claim:/p"] -> :ok end)

      {reply, _state} = Handler.handle({:close, %{"handle" => handle}}, state)
      assert {:ok, %{}} == reply
      assert :error = HandleRegistry.fetch(handle)
    end

    test "pread routes through ChunkReader.read_file" do
      {handle, state} = open_file(connected(), "/p")

      expect(ChunkReader, :read_file_by_id, fn "vol-a", "object:/p", opts ->
        assert Keyword.get(opts, :offset) == 16
        assert Keyword.get(opts, :length) == 32
        {:ok, :binary.copy("x", 32)}
      end)

      {reply, _} =
        Handler.handle({:pread, %{"handle" => handle, "offset" => 16, "size" => 32}}, state)

      assert {:ok, %{data: data}} = reply
      assert byte_size(data) == 32
    end

    test "pwrite forwards bytes verbatim and reports written count" do
      {handle, state} = open_file(connected(), "/p")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :write_file_at_by_id,
                                           ["vol-a", "object:/p", 0, "hello"] ->
        {:ok, %{path: "/p", size: 5}}
      end)

      {reply, _} =
        Handler.handle({:pwrite, %{"handle" => handle, "offset" => 0, "data" => "hello"}}, state)

      assert {:ok, %{written: 5}} == reply
    end

    test "ftruncate routes through truncate_file" do
      {handle, state} = open_file(connected(), "/p")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :truncate_file_by_id,
                                           ["vol-a", "object:/p", 0] ->
        {:ok, %{}}
      end)

      {reply, _} = Handler.handle({:ftruncate, %{"handle" => handle, "size" => 0}}, state)
      assert {:ok, %{}} == reply
    end

    test "fsync drives the shared sync_file barrier for the open handle" do
      {handle, state} = open_file(connected(), "/p")

      expect(NeonFS.Client, :sync_file_by_id, fn "vol-a", "object:/p" -> :ok end)

      {reply, _} = Handler.handle({:fsync, %{"handle" => handle}}, state)
      assert {:ok, %{}} == reply
    end

    test "fsync maps a barrier failure to an errno" do
      {handle, state} = open_file(connected(), "/p")

      expect(NeonFS.Client, :sync_file_by_id, fn "vol-a", "object:/p" -> {:error, :io_error} end)

      {reply, _} = Handler.handle({:fsync, %{"handle" => handle}}, state)
      assert {:error, :eio} == reply
    end

    test "fsync on an unknown handle is :ebadf" do
      {reply, _} = Handler.handle({:fsync, %{"handle" => 9999}}, connected())
      assert {:error, :ebadf} == reply
    end
  end

  describe "directories" do
    test "fdopendir snapshots the listing; readdir + closedir consume it one entry per call" do
      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/dir"] ->
        {:ok, %{path: "/dir", mode: 0o040755}}
      end)

      # Exactly one list_dir per fdopendir: readdir steps pop the
      # snapshot rather than re-fetching from core.
      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :list_dir, ["vol-a", "/dir"] ->
        {:ok,
         [
           %{path: "/dir/b.txt", mode: 0o100644},
           %{path: "/dir/a.txt", mode: 0o100644}
         ]}
      end)

      {{:ok, %{handle: handle}}, state} =
        Handler.handle({:fdopendir, %{"path" => "/dir"}}, connected())

      {{:ok, %{entry: e1, eof: false}}, state} =
        Handler.handle({:readdir, %{"handle" => handle}}, state)

      assert e1.name == "a.txt"

      {{:ok, %{entry: e2, eof: false}}, state} =
        Handler.handle({:readdir, %{"handle" => handle}}, state)

      assert e2.name == "b.txt"

      {{:ok, %{eof: true}}, state} =
        Handler.handle({:readdir, %{"handle" => handle}}, state)

      {{:ok, %{}}, state} = Handler.handle({:closedir, %{"handle" => handle}}, state)

      refute Map.has_key?(state.dirs, handle)
    end

    test "fdopendir surfaces a list_dir failure instead of snapshotting" do
      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core, :get_file_meta, ["vol-a", "/dir"] -> {:ok, %{path: "/dir", mode: 0o040755}}
        NeonFS.Core, :list_dir, ["vol-a", "/dir"] -> {:error, :io_error}
      end)

      {reply, _} = Handler.handle({:fdopendir, %{"path" => "/dir"}}, connected())
      assert {:error, :eio} == reply
    end

    test "mkdirat creates a directory via mkdir with the plain mode bits" do
      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :mkdir, ["vol-a", "/d", [mode: 0o755]] ->
        {:ok, %{}}
      end)

      {reply, _} = Handler.handle({:mkdirat, %{"path" => "/d", "mode" => 0o755}}, connected())
      assert {:ok, %{}} == reply
    end
  end

  describe "mutations" do
    test "unlinkat deletes via delete_file" do
      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :delete_file, ["vol-a", "/x"] -> :ok end)

      {reply, _} = Handler.handle({:unlinkat, %{"path" => "/x"}}, connected())
      assert {:ok, %{}} == reply
    end

    test "renameat forwards both paths to rename_file" do
      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :rename_file,
                                           ["vol-a", "/dir/a", "/dir/b"] ->
        :ok
      end)

      {reply, _} =
        Handler.handle(
          {:renameat, %{"old_path" => "/dir/a", "new_path" => "/dir/b"}},
          connected()
        )

      assert {:ok, %{}} == reply
    end

    # smbd's atomic mkdir opens the tmp-named directory, renames it to
    # the final name, then fstats the still-open handle
    # (open.c mkdir_internal → open_directory's vfs_stat_fsp). The
    # handle's stored path must follow the rename.
    # The handle carries the file's identity, so a rename needs no
    # bookkeeping on the handler's side at all: the fd keeps addressing the
    # same object under its new name, which is what POSIX and SMB both
    # require.
    test "a rename does not disturb an open handle" do
      {handle, state} = open_file(connected(), "/old.txt")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :rename_file,
                                           ["vol-a", "/old.txt", "/new.txt"] ->
        :ok
      end)

      {reply, state} =
        Handler.handle({:renameat, %{"old_path" => "/old.txt", "new_path" => "/new.txt"}}, state)

      assert {:ok, %{}} == reply

      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :get_file_meta_by_id,
                                           ["vol-a", "object:/old.txt"] ->
        {:ok, file_meta("/new.txt", size: 7)}
      end)

      {reply, _} = Handler.handle({:fstat, %{"handle" => handle}}, state)
      assert {:ok, %{stat: %{size: 7}}} = reply
    end

    test "a rename beneath an open handle's directory does not disturb it" do
      {handle, state} = open_file(connected(), "/dir/sub/f.txt")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :rename_file,
                                           ["vol-a", "/dir", "/moved"] ->
        :ok
      end)

      {reply, state} =
        Handler.handle({:renameat, %{"old_path" => "/dir", "new_path" => "/moved"}}, state)

      assert {:ok, %{}} == reply

      assert {:ok, %{file_id: "object:/dir/sub/f.txt"}} = HandleRegistry.fetch(handle)

      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :get_file_meta_by_id,
                                           ["vol-a", "object:/dir/sub/f.txt"] ->
        {:ok, file_meta("/moved/sub/f.txt", size: 3)}
      end)

      {reply, _} = Handler.handle({:fstat, %{"handle" => handle}}, state)
      assert {:ok, %{stat: %{size: 3}}} = reply
    end

    test "a handle opened on one connection works on another, across rename and unlink" do
      {handle, state_a} = open_file(connected(), "/shared.txt")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :rename_file,
                                           ["vol-a", "/shared.txt", "/renamed.txt"] ->
        :ok
      end)

      {{:ok, %{}}, state_a} =
        Handler.handle(
          {:renameat, %{"old_path" => "/shared.txt", "new_path" => "/renamed.txt"}},
          state_a
        )

      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :delete_file, ["vol-a", "/renamed.txt"] ->
        :ok
      end)

      {{:ok, %{}}, _state_a} =
        Handler.handle({:unlinkat, %{"path" => "/renamed.txt", "flags" => 0}}, state_a)

      # A second connection, which never saw the open, reaches the same
      # bytes through the same handle. The pin keeps the unlinked object
      # readable.
      state_b = connected()

      expect(ChunkReader, :read_file_by_id, fn "vol-a", "object:/shared.txt", _opts ->
        {:ok, "still here"}
      end)

      {reply, _} =
        Handler.handle(
          {:pread, %{"handle" => handle, "offset" => 0, "size" => 9}},
          state_b
        )

      assert {:ok, %{data: "still here"}} = reply
    end

    test "a dead connection releases its own pins and no others" do
      {mine, _state} = open_file(connected(), "/mine.txt")

      test_pid = self()

      # A second connection process opens its own file, then dies.
      other =
        spawn(fn ->
          receive do
            {:open, from} ->
              {:ok, handle} =
                HandleRegistry.open(
                  "vol-a",
                  "object:/theirs.txt",
                  "/theirs.txt",
                  0,
                  "claim:/theirs.txt"
                )

              send(from, {:opened, handle})
              receive do: (:die -> :ok)
          end
        end)

      Mimic.allow(NeonFS.Client, self(), other)
      send(other, {:open, self()})
      assert_receive {:opened, theirs}, 500

      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :unpin_file, ["claim:/theirs.txt"] ->
        send(test_pid, :released)
        :ok
      end)

      ref = Process.monitor(other)
      send(other, :die)
      assert_receive {:DOWN, ^ref, :process, ^other, _}, 500
      assert_receive :released, 500

      assert :error = HandleRegistry.fetch(theirs)
      assert {:ok, %{file_id: "object:/mine.txt"}} = HandleRegistry.fetch(mine)
    end
  end

  describe "filesystem" do
    test "disk_free reports a synthetic (non-zero) capacity" do
      {reply, _} = Handler.handle({:disk_free, %{}}, connected())

      assert {:ok, %{total_bytes: total, free_bytes: free, available_bytes: avail}} = reply
      assert total > 0 and free > 0 and avail > 0
    end

    test "fstatvfs reports the same synthetic capacity" do
      {reply, _} = Handler.handle({:fstatvfs, %{}}, connected())
      assert {:ok, %{total_bytes: total}} = reply
      assert total > 0
    end
  end

  describe "path normalisation" do
    test "the share root '.' maps to the volume root '/'" do
      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/"] ->
        {:ok, file_meta("/", mode: 0o40777, accessed_at: 1, modified_at: 1, changed_at: 1)}
      end)

      {reply, _} = Handler.handle({:stat, %{"path" => "."}}, connected())
      assert {:ok, %{stat: %{dev: 0x6AD05F36A5D262B7, ino: 1, kind: :directory}}} = reply
    end

    test "share-relative entries gain a leading slash" do
      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/d/a.txt"] ->
        {:ok, file_meta("/d/a.txt", size: 3, accessed_at: 1, modified_at: 1, changed_at: 1)}
      end)

      {reply, _} = Handler.handle({:stat, %{"path" => "d/a.txt"}}, connected())
      assert {:ok, %{stat: %{kind: :file}}} = reply
    end

    test "opendir on the root '.' lists the volume root" do
      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/"] ->
        {:ok, %{size: 0, mode: 0o40777}}
      end)

      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :list_dir, ["vol-a", "/"] ->
        {:ok, []}
      end)

      {reply, _} = Handler.handle({:fdopendir, %{"path" => "."}}, connected())
      assert {:ok, %{handle: _}} = reply
    end

    test "rename normalises both operands" do
      # "d/old.txt" → "/d/old.txt", likewise the destination.
      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :rename_file,
                                           ["vol-a", "/d/old.txt", "/d/new.txt"] ->
        :ok
      end)

      {reply, _} =
        Handler.handle(
          {:renameat, %{"old_path" => "d/old.txt", "new_path" => "d/new.txt"}},
          connected()
        )

      assert {:ok, %{}} == reply
    end

    test "already-absolute paths are left unchanged" do
      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/foo"] ->
        {:ok, file_meta("/foo")}
      end)

      {reply, _} = Handler.handle({:stat, %{"path" => "/foo"}}, connected())
      assert {:ok, %{stat: _}} = reply
    end

    # smbd stats the synthesised "." and ".." entries of a directory
    # listing by opening `<dir>/.` verbatim (smbd_dirptr_get_entry) —
    # the path arrives uncanonicalised.
    test "a trailing '.' segment resolves to the directory itself" do
      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/d"] ->
        {:ok, file_meta("/d", mode: 0o40755)}
      end)

      {reply, _} = Handler.handle({:stat, %{"path" => "d/."}}, connected())
      assert {:ok, %{stat: %{kind: :directory}}} = reply
    end

    test "a trailing '..' segment resolves to the parent directory" do
      expect(NeonFS.Client, :core_call, fn NeonFS.Core, :get_file_meta, ["vol-a", "/"] ->
        {:ok, file_meta("/", mode: 0o40777)}
      end)

      {reply, _} = Handler.handle({:stat, %{"path" => "d/.."}}, connected())
      assert {:ok, %{stat: %{kind: :directory}}} = reply
    end

    test "mid-path dot segments are resolved without touching other dots" do
      expect(NeonFS.Client, :core_call, fn NeonFS.Core,
                                           :get_file_meta,
                                           ["vol-a", "/b/.::TMPNAME:D:1%2:x"] ->
        {:ok, file_meta("/b/.::TMPNAME:D:1%2:x", mode: 0o40755)}
      end)

      {reply, _} =
        Handler.handle({:stat, %{"path" => "a/../b/./.::TMPNAME:D:1%2:x"}}, connected())

      assert {:ok, %{stat: %{kind: :directory}}} = reply
    end
  end

  describe "unknown operations" do
    test "unknown op returns :enosys" do
      {reply, _} = Handler.handle({:flock, %{}}, connected())
      assert {:error, :enosys} == reply
    end

    test "non-tuple request returns :einval" do
      {reply, _} = Handler.handle(:not_a_tuple, blank_state())
      assert {:error, :einval} == reply
    end
  end
end
