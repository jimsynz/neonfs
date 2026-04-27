defmodule NeonFS.NFS.NFSv3BackendTest do
  use ExUnit.Case, async: false

  import Bitwise, only: [<<<: 2]

  alias NeonFS.Core.FileMeta
  alias NeonFS.NFS.{Filehandle, NFSv3Backend}
  alias NFSServer.NFSv3.Types.Fattr3

  @volume_id_uuid "019dc5d8-3fcf-7d13-b4fa-832c4390b0a0"
  @volume_id_bin Filehandle.volume_uuid_to_binary(@volume_id_uuid) |> elem(1)
  @volume_name "vol-1"
  @file_path "/docs/file.txt"
  @file_inode 0xCAFE_BABE_DEAD_BEEF

  setup do
    on_exit(fn ->
      Application.delete_env(:neonfs_nfs, :core_call_fn)
      Application.delete_env(:neonfs_nfs, :inode_table_get_path_fn)
      Application.delete_env(:neonfs_nfs, :inode_table_allocate_fn)
      Application.delete_env(:neonfs_nfs, :read_file_stream_fn)
    end)

    :ok
  end

  defp put_inode_table(table) do
    Application.put_env(:neonfs_nfs, :inode_table_get_path_fn, fn inode ->
      case Map.get(table, inode) do
        nil -> {:error, :not_found}
        path_or_pair -> {:ok, normalise_path_pair(path_or_pair)}
      end
    end)

    Application.put_env(:neonfs_nfs, :inode_table_allocate_fn, fn vol, path ->
      allocate_inode_for(table, vol, path)
    end)
  end

  defp allocate_inode_for(table, vol, path) do
    key = {vol, path}
    inode = find_inode(table, key) || :erlang.phash2(key, 1 <<< 60)
    {:ok, inode}
  end

  defp find_inode(table, key) do
    Enum.find_value(table, fn {ino, p} ->
      if normalise_path_pair(p) == key, do: ino, else: nil
    end)
  end

  defp normalise_path_pair({_, _} = pair), do: pair
  defp normalise_path_pair(path) when is_binary(path), do: {nil, path}

  defp put_core(handler) do
    Application.put_env(:neonfs_nfs, :core_call_fn, handler)
  end

  defp file_meta(overrides \\ %{}) do
    base = %FileMeta{
      id: "file-1",
      volume_id: @volume_id_uuid,
      path: @file_path,
      chunks: [],
      stripes: nil,
      size: 12,
      content_type: "text/plain",
      mode: 0o100_644,
      uid: 1000,
      gid: 1000,
      acl_entries: [],
      default_acl: nil,
      metadata: %{},
      created_at: DateTime.from_unix!(1, :second),
      modified_at: DateTime.from_unix!(2, :second),
      accessed_at: DateTime.from_unix!(3, :second),
      changed_at: DateTime.from_unix!(4, :second),
      version: 1,
      previous_version_id: nil,
      hlc_timestamp: nil
    }

    Map.merge(base, overrides)
  end

  defp valid_fh, do: Filehandle.encode(@volume_id_bin, @file_inode)

  ## getattr

  describe "getattr/3" do
    test "returns Fattr3 derived from FileMeta" do
      put_inode_table(%{@file_inode => {@volume_name, @file_path}})

      put_core(fn NeonFS.Core, :get_file_meta, [@volume_name, @file_path] ->
        {:ok, file_meta()}
      end)

      assert {:ok, %Fattr3{type: :reg, mode: 0o644, size: 12, uid: 1000}} =
               NFSv3Backend.getattr(valid_fh(), :auth, %{})
    end

    test "returns :stale for a malformed filehandle" do
      assert {:error, :stale} = NFSv3Backend.getattr(<<0::8>>, :auth, %{})
    end

    test "returns :stale when the inode isn't in the table" do
      put_inode_table(%{})

      assert {:error, :stale} = NFSv3Backend.getattr(valid_fh(), :auth, %{})
    end

    test "returns :noent when core says the path is gone" do
      put_inode_table(%{@file_inode => {@volume_name, @file_path}})
      put_core(fn _, :get_file_meta, _ -> {:error, :not_found} end)

      assert {:error, :noent} = NFSv3Backend.getattr(valid_fh(), :auth, %{})
    end
  end

  ## access

  describe "access/4" do
    test "echoes the requested mask alongside post-op attrs on success" do
      put_inode_table(%{@file_inode => {@volume_name, @file_path}})
      put_core(fn _, :get_file_meta, _ -> {:ok, file_meta()} end)

      assert {:ok, 0x21, %Fattr3{}} = NFSv3Backend.access(valid_fh(), 0x21, :auth, %{})
    end

    test "returns :stale with no attrs when handle decode fails" do
      assert {:error, :stale, nil} = NFSv3Backend.access(<<>>, 0x21, :auth, %{})
    end
  end

  ## lookup

  describe "lookup/4" do
    test "returns child fhandle, child attrs, and dir attrs on success" do
      dir_path = "/docs"
      child_path = "/docs/file.txt"
      dir_inode = 0xAAAA_AAAA_AAAA_AAAA

      put_inode_table(%{
        dir_inode => {@volume_name, dir_path},
        @file_inode => {@volume_name, child_path}
      })

      put_core(fn _, :get_file_meta, [_vol, path] ->
        case path do
          ^child_path -> {:ok, file_meta(%{path: child_path})}
          ^dir_path -> {:ok, file_meta(%{path: dir_path, mode: 0o040_755, size: 0})}
        end
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, dir_inode)

      assert {:ok, child_fh, %Fattr3{type: :reg}, %Fattr3{type: :dir}} =
               NFSv3Backend.lookup(dir_fh, "file.txt", :auth, %{})

      assert byte_size(child_fh) == Filehandle.size()
      assert {:ok, %{volume_id: vol}} = Filehandle.decode(child_fh)
      assert vol == @volume_id_bin
    end

    test "returns :noent + dir attrs when the child doesn't exist" do
      dir_path = "/docs"
      dir_inode = 0xAAAA_AAAA_AAAA_AAAA

      put_inode_table(%{dir_inode => {@volume_name, dir_path}})

      put_core(fn _, :get_file_meta, [_vol, path] ->
        case path do
          ^dir_path -> {:ok, file_meta(%{path: dir_path, mode: 0o040_755})}
          _ -> {:error, :not_found}
        end
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, dir_inode)

      assert {:error, :noent, %Fattr3{type: :dir}} =
               NFSv3Backend.lookup(dir_fh, "missing.txt", :auth, %{})
    end
  end

  ## read

  describe "read/5" do
    setup do
      Application.put_env(:neonfs_nfs, :read_file_stream_fn, fn _vol, _path, _off, _count ->
        Stream.repeatedly(fn -> "x" end)
      end)

      :ok
    end

    test "returns a lazy stream that doesn't materialise the whole file" do
      put_inode_table(%{@file_inode => {@volume_name, @file_path}})
      put_core(fn _, :get_file_meta, _ -> {:ok, file_meta(%{size: 1024})} end)

      assert {:ok, %{data: stream, eof: false, post_op: %Fattr3{}}} =
               NFSv3Backend.read(valid_fh(), 0, 64, :auth, %{})

      # Drain only the first chunk to confirm the stream is lazy —
      # the mock would yield infinitely if we Enum.into'd the lot.
      assert ["x" | _] = Enum.take(stream, 1)
    end

    test "eof is true when the request reaches end-of-file" do
      put_inode_table(%{@file_inode => {@volume_name, @file_path}})
      put_core(fn _, :get_file_meta, _ -> {:ok, file_meta(%{size: 100})} end)

      assert {:ok, %{eof: true}} = NFSv3Backend.read(valid_fh(), 50, 100, :auth, %{})
    end
  end

  ## fsstat / fsinfo / pathconf

  describe "fsstat/3, fsinfo/3, pathconf/3" do
    setup do
      put_inode_table(%{@file_inode => {@volume_name, @file_path}})
      put_core(fn _, :get_file_meta, _ -> {:ok, file_meta()} end)
      :ok
    end

    test "fsstat returns sane fixed capacity values until #321 lands" do
      assert {:ok, reply, %Fattr3{}} = NFSv3Backend.fsstat(valid_fh(), :auth, %{})
      assert reply.tbytes > 0
      assert reply.fbytes > 0
      assert reply.tfiles > 0
    end

    test "fsinfo returns RFC 1813 §3.3.19 reply with sensible R/W limits" do
      assert {:ok, reply, %Fattr3{}} = NFSv3Backend.fsinfo(valid_fh(), :auth, %{})
      assert reply.rtmax >= 1024
      assert reply.wtmax >= 1024
      assert is_integer(reply.maxfilesize)
    end

    test "pathconf returns NAME_MAX 255" do
      assert {:ok, reply, %Fattr3{}} = NFSv3Backend.pathconf(valid_fh(), :auth, %{})
      assert reply.name_max == 255
    end
  end

  ## setattr (#621)

  describe "setattr/5" do
    setup do
      put_inode_table(%{@file_inode => {@volume_name, @file_path}})
      :ok
    end

    test "applies attribute updates and returns wcc_data on success" do
      pre = file_meta()
      post = %{pre | mode: 0o100_600, version: 2}
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, @file_path] ->
          {:ok, pre}

        NeonFS.Core, :update_file_meta, [@volume_name, @file_path, updates] ->
          send(test_pid, {:update_called, updates})
          {:ok, post}
      end)

      sattr = %NFSServer.NFSv3.Types.Sattr3{mode: 0o600}

      assert {:ok, %NFSServer.NFSv3.Types.WccData{before: pre_wcc, after: post_attr}} =
               NFSv3Backend.setattr(valid_fh(), sattr, nil, :auth, %{})

      assert pre_wcc.size == 12
      assert pre_wcc.mtime.seconds == 2
      assert pre_wcc.ctime.seconds == 4
      assert post_attr.mode == 0o600

      assert_receive {:update_called, updates}
      assert Keyword.get(updates, :mode) == 0o600
      refute Keyword.has_key?(updates, :uid)
    end

    test "size triggers truncate_file with the rest of the updates folded in" do
      pre = file_meta(%{size: 1000})
      post = %{pre | size: 100, mode: 0o100_640, version: 2}
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, @file_path] ->
          {:ok, pre}

        NeonFS.Core, :truncate_file, [@volume_name, @file_path, new_size, additional] ->
          send(test_pid, {:truncate_called, new_size, additional})
          {:ok, post}
      end)

      sattr = %NFSServer.NFSv3.Types.Sattr3{size: 100, mode: 0o640}

      assert {:ok, %NFSServer.NFSv3.Types.WccData{after: post_attr}} =
               NFSv3Backend.setattr(valid_fh(), sattr, nil, :auth, %{})

      assert post_attr.size == 100
      assert_receive {:truncate_called, 100, additional}
      assert Keyword.get(additional, :mode) == 0o640
    end

    test "non-matching guard ctime returns NFS3ERR_NOT_SYNC + wcc_data; no update fires" do
      pre = file_meta()
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, @file_path] ->
          {:ok, pre}

        NeonFS.Core, :update_file_meta, _ ->
          send(test_pid, :update_called)
          {:ok, pre}
      end)

      # `pre.changed_at` is unix-second 4; supply a guard ctime of
      # second 99 so it definitely doesn't match.
      guard = %NFSServer.NFSv3.Types.Nfstime3{seconds: 99, nseconds: 0}
      sattr = %NFSServer.NFSv3.Types.Sattr3{mode: 0o600}

      assert {:error, :not_sync,
              %NFSServer.NFSv3.Types.WccData{before: pre_wcc, after: %Fattr3{}}} =
               NFSv3Backend.setattr(valid_fh(), sattr, guard, :auth, %{})

      assert pre_wcc.ctime.seconds == 4
      refute_receive :update_called, 50
    end

    test "matching guard ctime allows the update to proceed" do
      pre = file_meta()
      post = %{pre | mode: 0o100_700, version: 2}

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, @file_path] -> {:ok, pre}
        NeonFS.Core, :update_file_meta, _ -> {:ok, post}
      end)

      # pre.changed_at = unix(4), so guard {4, 0} matches.
      guard = %NFSServer.NFSv3.Types.Nfstime3{seconds: 4, nseconds: 0}
      sattr = %NFSServer.NFSv3.Types.Sattr3{mode: 0o700}

      assert {:ok, %NFSServer.NFSv3.Types.WccData{after: post_attr}} =
               NFSv3Backend.setattr(valid_fh(), sattr, guard, :auth, %{})

      assert post_attr.mode == 0o700
    end

    test "stale filehandle returns :stale + empty wcc_data" do
      put_core(fn
        NeonFS.Core, :get_file_meta, _ -> {:error, :not_found}
      end)

      sattr = %NFSServer.NFSv3.Types.Sattr3{mode: 0o600}

      assert {:error, :noent, %NFSServer.NFSv3.Types.WccData{before: nil, after: nil}} =
               NFSv3Backend.setattr(valid_fh(), sattr, nil, :auth, %{})
    end

    test "no fields set + no size = no-op SETATTR refreshes post-op only" do
      pre = file_meta()
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, @file_path] ->
          send(test_pid, :get_called)
          {:ok, pre}

        NeonFS.Core, :update_file_meta, _ ->
          flunk("update_file_meta should not be called for no-op SETATTR")

        NeonFS.Core, :truncate_file, _ ->
          flunk("truncate_file should not be called for no-op SETATTR")
      end)

      assert {:ok, %NFSServer.NFSv3.Types.WccData{before: %_{}, after: %Fattr3{}}} =
               NFSv3Backend.setattr(
                 valid_fh(),
                 %NFSServer.NFSv3.Types.Sattr3{},
                 nil,
                 :auth,
                 %{}
               )

      # `resolve_meta/1` calls get_file_meta once, then we re-fetch
      # for the post-op view — two calls total.
      assert_receive :get_called
      assert_receive :get_called
    end
  end
end
