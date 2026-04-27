defmodule NeonFS.CIFS.HandlerTest do
  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.CIFS.Handler
  alias NeonFS.Client.ChunkReader

  setup :verify_on_exit!

  defp blank_state do
    %{volume: nil, next_handle: 1, files: %{}, dirs: %{}}
  end

  defp connected do
    {{:ok, _}, state} = Handler.handle({:connect, %{"volume" => "vol-a"}}, blank_state())
    state
  end

  defp open_file(state, path, flags \\ 0o100) do
    expect(NeonFS.Client, :core_call, fn NeonFS.Core.FileIndex, :get_by_path, ["vol-a", ^path] ->
      {:error, :not_found}
    end)

    expect(NeonFS.Client, :core_call, fn NeonFS.Core.WriteOperation,
                                         :write_file_at,
                                         ["vol-a", ^path, 0, <<>>, [mode: 420]] ->
      {:ok, %{path: path, size: 0, mode: 0o100644}}
    end)

    {{:ok, %{handle: handle}}, state} =
      Handler.handle({:openat, %{"path" => path, "flags" => flags, "mode" => 0o644}}, state)

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
  end

  describe "metadata" do
    test "stat fetches via core_call and translates to the Samba shape" do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core.FileIndex, :get_by_path, ["vol-a", "/foo"] ->
        {:ok, %{size: 13, mode: 0o100644, accessed_at: 100, modified_at: 200, changed_at: 300}}
      end)

      {reply, _} = Handler.handle({:stat, %{"path" => "/foo"}}, connected())

      assert {:ok,
              %{
                stat: %{size: 13, mode: 0o100644, atime: 100, mtime: 200, ctime: 300, kind: :file}
              }} = reply
    end

    test "stat ENOENT maps backend :not_found to :enoent" do
      stub(NeonFS.Client, :core_call, fn _, _, _ -> {:error, :not_found} end)
      {reply, _} = Handler.handle({:stat, %{"path" => "/missing"}}, connected())
      assert {:error, :enoent} == reply
    end

    test "lstat falls through to stat for now" do
      stub(NeonFS.Client, :core_call, fn _, _, _ ->
        {:ok, %{size: 0, mode: 0o100644, modified_at: 1, changed_at: 1, accessed_at: 1}}
      end)

      {reply1, _} = Handler.handle({:stat, %{"path" => "/x"}}, connected())
      {reply2, _} = Handler.handle({:lstat, %{"path" => "/x"}}, connected())
      assert reply1 == reply2
    end

    test "fstat resolves through the open-files table" do
      {handle, state} = open_file(connected(), "/foo")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core.FileIndex,
                                           :get_by_path,
                                           ["vol-a", "/foo"] ->
        {:ok, %{size: 99, mode: 0o100644}}
      end)

      {reply, _} = Handler.handle({:fstat, %{"handle" => handle}}, state)
      assert match?({:ok, %{stat: %{size: 99}}}, reply)
    end

    test "fstat on an unknown handle is :ebadf" do
      {reply, _} = Handler.handle({:fstat, %{"handle" => 9999}}, connected())
      assert {:error, :ebadf} == reply
    end

    test "fchmod routes through update_mode" do
      {handle, state} = open_file(connected(), "/p")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core.FileIndex,
                                           :update_mode,
                                           ["vol-a", "/p", 0o600] ->
        {:ok, %{}}
      end)

      {reply, _} =
        Handler.handle({:fchmod, %{"handle" => handle, "mode" => 0o600}}, state)

      assert {:ok, %{}} == reply
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

    test "fntimes routes atime+mtime through update_times" do
      {handle, state} = open_file(connected(), "/p")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core.FileIndex,
                                           :update_times,
                                           ["vol-a", "/p", 100, 200] ->
        {:ok, %{}}
      end)

      {reply, _} =
        Handler.handle({:fntimes, %{"handle" => handle, "atime" => 100, "mtime" => 200}}, state)

      assert {:ok, %{}} == reply
    end
  end

  describe "file I/O" do
    test "openat creates if missing and mints a handle" do
      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol-a", "/new"] ->
          {:error, :not_found}

        NeonFS.Core.WriteOperation, :write_file_at, ["vol-a", "/new", 0, <<>>, [mode: 420]] ->
          {:ok, %{path: "/new"}}
      end)

      {reply, state} =
        Handler.handle(
          {:openat, %{"path" => "/new", "flags" => 0o100, "mode" => 0o644}},
          connected()
        )

      assert {:ok, %{handle: 1}} = reply
      assert Map.has_key?(state.files, 1)
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

    # `O_EXCL | O_CREAT` (0o300) routes through `WriteOperation` with
    # `create_only: true` (sub-issue #595 of #303). The interface-side
    # FileIndex precheck only catches the trivial case where the file
    # is already on disk; concurrent creates on different CIFS nodes
    # are fenced by the `claim_create` primitive on the core node, and
    # the loser sees `{:error, :exists}` which this handler maps to
    # `:eexist`.
    test "openat with O_EXCL | O_CREAT on missing file forwards create_only: true" do
      test_pid = self()

      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol-a", "/atomic"] ->
          {:error, :not_found}

        NeonFS.Core.WriteOperation, :write_file_at, ["vol-a", "/atomic", 0, <<>>, opts] ->
          send(test_pid, {:write_opts, opts})
          {:ok, %{path: "/atomic"}}
      end)

      {reply, state} =
        Handler.handle(
          {:openat, %{"path" => "/atomic", "flags" => 0o300, "mode" => 0o644}},
          connected()
        )

      assert {:ok, %{handle: 1}} = reply
      assert Map.has_key?(state.files, 1)
      assert_receive {:write_opts, opts}, 500
      assert Keyword.get(opts, :create_only) == true
    end

    test "openat with O_EXCL | O_CREAT maps :exists from core to :eexist" do
      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol-a", "/raced"] ->
          {:error, :not_found}

        NeonFS.Core.WriteOperation, :write_file_at, ["vol-a", "/raced", 0, <<>>, opts] ->
          # The peer-cluster integration test for the underlying
          # primitive lives in #592; here we just verify the
          # interface-level translation.
          assert Keyword.get(opts, :create_only) == true
          {:error, :exists}
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
        NeonFS.Core.FileIndex, :get_by_path, ["vol-a", "/plain"] ->
          {:error, :not_found}

        NeonFS.Core.WriteOperation, :write_file_at, ["vol-a", "/plain", 0, <<>>, opts] ->
          send(test_pid, {:write_opts, opts})
          {:ok, %{path: "/plain"}}
      end)

      {reply, _} =
        Handler.handle(
          {:openat, %{"path" => "/plain", "flags" => 0o100, "mode" => 0o644}},
          connected()
        )

      assert {:ok, %{handle: 1}} = reply
      assert_receive {:write_opts, opts}, 500
      refute Keyword.get(opts, :create_only)
    end

    test "close releases the handle" do
      {handle, state} = open_file(connected(), "/p")
      {reply, state2} = Handler.handle({:close, %{"handle" => handle}}, state)
      assert {:ok, %{}} == reply
      refute Map.has_key?(state2.files, handle)
    end

    test "pread routes through ChunkReader.read_file" do
      {handle, state} = open_file(connected(), "/p")

      expect(ChunkReader, :read_file, fn "vol-a", "/p", opts ->
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

      expect(NeonFS.Client, :core_call, fn NeonFS.Core.WriteOperation,
                                           :write_file_at,
                                           ["vol-a", "/p", 0, "hello"] ->
        {:ok, %{path: "/p", size: 5}}
      end)

      {reply, _} =
        Handler.handle({:pwrite, %{"handle" => handle, "offset" => 0, "data" => "hello"}}, state)

      assert {:ok, %{written: 5}} == reply
    end

    test "ftruncate routes through FileIndex.truncate" do
      {handle, state} = open_file(connected(), "/p")

      expect(NeonFS.Client, :core_call, fn NeonFS.Core.FileIndex, :truncate, ["vol-a", "/p", 0] ->
        {:ok, %{}}
      end)

      {reply, _} = Handler.handle({:ftruncate, %{"handle" => handle, "size" => 0}}, state)
      assert {:ok, %{}} == reply
    end
  end

  describe "directories" do
    test "fdopendir + readdir + closedir paginates one entry per call" do
      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol-a", "/dir"] ->
          {:ok, %{path: "/dir", mode: 0o040755}}

        NeonFS.Core.FileIndex, :list_directory, ["vol-a", "/dir"] ->
          {:ok, [{"a.txt", "/dir/a.txt", 0o100644}, {"b.txt", "/dir/b.txt", 0o100644}]}
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

    test "mkdirat routes through create_directory" do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core.WriteOperation,
                                         :create_directory,
                                         ["vol-a", "/d", 0o755] ->
        {:ok, %{}}
      end)

      {reply, _} = Handler.handle({:mkdirat, %{"path" => "/d", "mode" => 0o755}}, connected())
      assert {:ok, %{}} == reply
    end
  end

  describe "mutations" do
    test "unlinkat routes through delete_file" do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core.WriteOperation,
                                         :delete_file,
                                         ["vol-a", "/x"] ->
        :ok
      end)

      {reply, _} = Handler.handle({:unlinkat, %{"path" => "/x"}}, connected())
      assert {:ok, %{}} == reply
    end

    test "renameat routes through rename" do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core.WriteOperation,
                                         :rename,
                                         ["vol-a", "/a", "/b"] ->
        :ok
      end)

      {reply, _} =
        Handler.handle({:renameat, %{"old_path" => "/a", "new_path" => "/b"}}, connected())

      assert {:ok, %{}} == reply
    end
  end

  describe "filesystem" do
    test "disk_free routes through Volume.stats" do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core.Volume, :stats, ["vol-a"] ->
        {:ok, %{total_bytes: 1_000, free_bytes: 600, available_bytes: 500}}
      end)

      {reply, _} = Handler.handle({:disk_free, %{}}, connected())
      assert {:ok, %{total_bytes: 1_000, free_bytes: 600, available_bytes: 500}} == reply
    end

    test "fstatvfs returns the same shape" do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core.Volume, :stats, ["vol-a"] ->
        {:ok, %{total_bytes: 8, free_bytes: 4, available_bytes: 4}}
      end)

      {reply, _} = Handler.handle({:fstatvfs, %{}}, connected())
      assert {:ok, %{total_bytes: 8}} = reply
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
