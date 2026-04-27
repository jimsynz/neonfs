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
    # `:erlang.phash2/2`'s upper bound is `2^32 - 1`. Test fixtures
    # don't care about the actual value — we just need a stable
    # 32-bit pseudo-allocation when the inode isn't pre-registered.
    inode = find_inode(table, key) || :erlang.phash2(key, 1 <<< 32)
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

  ## create (#622)

  describe "create/5 — UNCHECKED" do
    setup do
      put_inode_table(%{0xDEAD_DEAD_DEAD_DEAD => {@volume_name, "/docs"}})
      :ok
    end

    test "writes a new empty file and returns fhandle + attrs + wcc" do
      child_path = "/docs/new.txt"
      pre_dir = file_meta(%{path: "/docs", mode: 0o040_755, size: 0})
      child = file_meta(%{path: child_path, mode: 0o100_644, size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/docs"] -> {:ok, pre_dir}
        NeonFS.Core, :write_file_at, [@volume_name, ^child_path, 0, <<>>, _opts] -> {:ok, child}
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xDEAD_DEAD_DEAD_DEAD)

      sattr = %NFSServer.NFSv3.Types.Sattr3{mode: 0o644}

      assert {:ok, child_fh, %Fattr3{type: :reg}, %NFSServer.NFSv3.Types.WccData{before: pre_wcc}} =
               NFSv3Backend.create(dir_fh, "new.txt", {:unchecked, sattr}, :auth, %{})

      assert byte_size(child_fh) == Filehandle.size()
      assert pre_wcc.size == 0
    end

    test "forwards mode/uid/gid from sattr3 to write_file_at" do
      child_path = "/docs/owned.txt"
      pre_dir = file_meta(%{path: "/docs", mode: 0o040_755, size: 0})
      child = file_meta(%{path: child_path, uid: 42, gid: 100, mode: 0o100_600})
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/docs"] ->
          {:ok, pre_dir}

        NeonFS.Core, :write_file_at, [@volume_name, ^child_path, 0, <<>>, opts] ->
          send(test_pid, {:opts, opts})
          {:ok, child}
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xDEAD_DEAD_DEAD_DEAD)

      sattr = %NFSServer.NFSv3.Types.Sattr3{mode: 0o600, uid: 42, gid: 100}

      assert {:ok, _, _, _} =
               NFSv3Backend.create(dir_fh, "owned.txt", {:unchecked, sattr}, :auth, %{})

      assert_receive {:opts, opts}
      assert Keyword.get(opts, :mode) == 0o600
      assert Keyword.get(opts, :uid) == 42
      assert Keyword.get(opts, :gid) == 100
      refute Keyword.get(opts, :create_only)
    end
  end

  describe "create/5 — GUARDED" do
    setup do
      put_inode_table(%{0xCAFE_CAFE_CAFE_CAFE => {@volume_name, "/docs"}})
      :ok
    end

    test "fails with :exist when target already exists" do
      child_path = "/docs/already.txt"
      pre_dir = file_meta(%{path: "/docs", mode: 0o040_755, size: 0})
      existing = file_meta(%{path: child_path})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, ^child_path] -> {:ok, existing}
        NeonFS.Core, :get_file_meta, [@volume_name, "/docs"] -> {:ok, pre_dir}
        NeonFS.Core, :write_file_at, _ -> flunk("should not be called when GUARDED + exists")
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xCAFE_CAFE_CAFE_CAFE)

      assert {:error, :exist, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.create(
                 dir_fh,
                 "already.txt",
                 {:guarded, %NFSServer.NFSv3.Types.Sattr3{}},
                 :auth,
                 %{}
               )
    end

    test "creates if target doesn't exist" do
      child_path = "/docs/fresh.txt"
      pre_dir = file_meta(%{path: "/docs", mode: 0o040_755, size: 0})
      fresh = file_meta(%{path: child_path})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, ^child_path] -> {:error, :not_found}
        NeonFS.Core, :get_file_meta, [@volume_name, "/docs"] -> {:ok, pre_dir}
        NeonFS.Core, :write_file_at, [@volume_name, ^child_path, 0, <<>>, _opts] -> {:ok, fresh}
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xCAFE_CAFE_CAFE_CAFE)

      assert {:ok, _fh, %Fattr3{type: :reg}, _wcc} =
               NFSv3Backend.create(
                 dir_fh,
                 "fresh.txt",
                 {:guarded, %NFSServer.NFSv3.Types.Sattr3{}},
                 :auth,
                 %{}
               )
    end
  end

  describe "create/5 — EXCLUSIVE (idempotent retry)" do
    setup do
      put_inode_table(%{0xBEEF_BEEF_BEEF_BEEF => {@volume_name, "/docs"}})
      :ok
    end

    test "first call writes the file with the verf in metadata + create_only" do
      child_path = "/docs/excl.txt"
      verf = <<1, 2, 3, 4, 5, 6, 7, 8>>
      pre_dir = file_meta(%{path: "/docs", mode: 0o040_755, size: 0})
      created = file_meta(%{path: child_path, metadata: %{"nfs3_createverf" => verf}})
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, ^child_path] ->
          {:error, :not_found}

        NeonFS.Core, :get_file_meta, [@volume_name, "/docs"] ->
          {:ok, pre_dir}

        NeonFS.Core, :write_file_at, [@volume_name, ^child_path, 0, <<>>, opts] ->
          send(test_pid, {:opts, opts})
          {:ok, created}
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xBEEF_BEEF_BEEF_BEEF)

      assert {:ok, _fh, _attr, _wcc} =
               NFSv3Backend.create(dir_fh, "excl.txt", {:exclusive, verf}, :auth, %{})

      assert_receive {:opts, opts}
      assert Keyword.get(opts, :create_only) == true
      assert Keyword.fetch!(opts, :metadata)["nfs3_createverf"] == verf
    end

    test "retry with the same verf observes the existing file (no second write)" do
      child_path = "/docs/excl.txt"
      verf = <<1, 2, 3, 4, 5, 6, 7, 8>>
      pre_dir = file_meta(%{path: "/docs", mode: 0o040_755, size: 0})
      existing = file_meta(%{path: child_path, metadata: %{"nfs3_createverf" => verf}})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, ^child_path] -> {:ok, existing}
        NeonFS.Core, :get_file_meta, [@volume_name, "/docs"] -> {:ok, pre_dir}
        NeonFS.Core, :write_file_at, _ -> flunk("retry must not write")
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xBEEF_BEEF_BEEF_BEEF)

      assert {:ok, _fh, %Fattr3{type: :reg}, _wcc} =
               NFSv3Backend.create(dir_fh, "excl.txt", {:exclusive, verf}, :auth, %{})
    end

    test "different verf on an existing file returns :exist" do
      child_path = "/docs/excl.txt"
      original_verf = <<1, 2, 3, 4, 5, 6, 7, 8>>
      retry_verf = <<9, 9, 9, 9, 9, 9, 9, 9>>
      pre_dir = file_meta(%{path: "/docs", mode: 0o040_755, size: 0})
      existing = file_meta(%{path: child_path, metadata: %{"nfs3_createverf" => original_verf}})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, ^child_path] -> {:ok, existing}
        NeonFS.Core, :get_file_meta, [@volume_name, "/docs"] -> {:ok, pre_dir}
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xBEEF_BEEF_BEEF_BEEF)

      assert {:error, :exist, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.create(dir_fh, "excl.txt", {:exclusive, retry_verf}, :auth, %{})
    end
  end

  ## symlink / mknod / link (#626)

  describe "symlink/6" do
    setup do
      put_inode_table(%{0xFEED_FEED_FEED_FEED => {@volume_name, "/parent"}})
      :ok
    end

    test "stores the target string as file content with S_IFLNK mode bits" do
      child_path = "/parent/link.txt"
      target = "../target"
      pre_dir = file_meta(%{path: "/parent", mode: 0o040_755, size: 0})
      post = file_meta(%{path: child_path, mode: 0o120_777, size: byte_size(target)})
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/parent"] ->
          {:ok, pre_dir}

        NeonFS.Core, :write_file_at, [@volume_name, ^child_path, 0, ^target, opts] ->
          send(test_pid, {:write_called, opts})
          {:ok, post}
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xFEED_FEED_FEED_FEED)

      assert {:ok, _fh, %Fattr3{type: :lnk}, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.symlink(
                 dir_fh,
                 "link.txt",
                 %NFSServer.NFSv3.Types.Sattr3{mode: 0o777},
                 target,
                 :auth,
                 %{}
               )

      assert_receive {:write_called, opts}
      assert Bitwise.band(Keyword.fetch!(opts, :mode), 0o170_000) == 0o120_000
    end
  end

  describe "mknod/4" do
    setup do
      put_inode_table(%{0xFEED_FEED_FEED_FEED => {@volume_name, "/parent"}})
      :ok
    end

    test "returns NFS3ERR_NOTSUPP + parent wcc_data" do
      pre_dir = file_meta(%{path: "/parent", mode: 0o040_755, size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/parent"] -> {:ok, pre_dir}
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xFEED_FEED_FEED_FEED)

      assert {:error, :notsupp, %NFSServer.NFSv3.Types.WccData{before: %_{}}} =
               NFSv3Backend.mknod(dir_fh, "fifo", :auth, %{})
    end

    test "stale fhandle returns :stale + empty wcc_data" do
      assert {:error, :stale, %NFSServer.NFSv3.Types.WccData{before: nil, after: nil}} =
               NFSv3Backend.mknod(<<0::8>>, "x", :auth, %{})
    end
  end

  describe "link/5" do
    setup do
      put_inode_table(%{0xFEED_FEED_FEED_FEED => {@volume_name, "/parent"}})
      :ok
    end

    test "returns NFS3ERR_NOTSUPP + nil post_op + parent wcc_data" do
      pre_dir = file_meta(%{path: "/parent", mode: 0o040_755, size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/parent"] -> {:ok, pre_dir}
      end)

      file_fh = Filehandle.encode(@volume_id_bin, @file_inode)
      link_dir_fh = Filehandle.encode(@volume_id_bin, 0xFEED_FEED_FEED_FEED)

      assert {:error, :notsupp, nil, %NFSServer.NFSv3.Types.WccData{before: %_{}}} =
               NFSv3Backend.link(file_fh, link_dir_fh, "alias", :auth, %{})
    end

    test "stale link-dir fhandle returns :stale" do
      file_fh = Filehandle.encode(@volume_id_bin, @file_inode)

      assert {:error, :stale} =
               NFSv3Backend.link(file_fh, <<0::8>>, "alias", :auth, %{})
    end
  end

  ## rename (#625)

  describe "rename/6" do
    setup do
      put_inode_table(%{
        0xD00D_D00D_D00D_D00D => {@volume_name, "/from"},
        0xE00E_E00E_E00E_E00E => {@volume_name, "/to"}
      })

      :ok
    end

    test "same-directory rename succeeds with dual wcc_data" do
      pre_from = file_meta(%{path: "/from", mode: 0o040_755, size: 0})
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/from"] ->
          {:ok, pre_from}

        NeonFS.Core, :rename_file, [@volume_name, "/from/old.txt", "/from/new.txt"] ->
          send(test_pid, :rename_called)
          :ok
      end)

      from_fh = Filehandle.encode(@volume_id_bin, 0xD00D_D00D_D00D_D00D)

      assert {:ok, %NFSServer.NFSv3.Types.WccData{}, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.rename(from_fh, "old.txt", from_fh, "new.txt", :auth, %{})

      assert_received :rename_called
    end

    test "cross-directory rename calls Core.rename_file with the right paths" do
      pre_from = file_meta(%{path: "/from", mode: 0o040_755, size: 0})
      pre_to = file_meta(%{path: "/to", mode: 0o040_755, size: 0})
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/from"] ->
          {:ok, pre_from}

        NeonFS.Core, :get_file_meta, [@volume_name, "/to"] ->
          {:ok, pre_to}

        NeonFS.Core, :rename_file, [@volume_name, "/from/x.txt", "/to/y.txt"] ->
          send(test_pid, :rename_called)
          :ok
      end)

      from_fh = Filehandle.encode(@volume_id_bin, 0xD00D_D00D_D00D_D00D)
      to_fh = Filehandle.encode(@volume_id_bin, 0xE00E_E00E_E00E_E00E)

      assert {:ok, _, _} = NFSv3Backend.rename(from_fh, "x.txt", to_fh, "y.txt", :auth, %{})
      assert_received :rename_called
    end

    test "cycle (`/a` → `/a/b`) maps `:einval` to NFS3ERR_INVAL" do
      pre_from = file_meta(%{path: "/from", mode: 0o040_755, size: 0})
      pre_to = file_meta(%{path: "/from", mode: 0o040_755, size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, _ -> {:ok, pre_from}
        # `Core.rename_file` returns `{:error, :einval}` for cycles per
        # the namespace coordinator's `claim_rename` check (#304).
        NeonFS.Core, :rename_file, _ -> {:error, :einval}
        _, _, _ -> {:ok, pre_to}
      end)

      from_fh = Filehandle.encode(@volume_id_bin, 0xD00D_D00D_D00D_D00D)

      assert {:error, :inval, %NFSServer.NFSv3.Types.WccData{}, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.rename(from_fh, "a", from_fh, "a/b", :auth, %{})
    end

    test "missing source returns NFS3ERR_NOENT + dual wcc_data" do
      pre_from = file_meta(%{path: "/from", mode: 0o040_755, size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, _ -> {:ok, pre_from}
        NeonFS.Core, :rename_file, _ -> {:error, :not_found}
      end)

      from_fh = Filehandle.encode(@volume_id_bin, 0xD00D_D00D_D00D_D00D)

      assert {:error, :noent, %NFSServer.NFSv3.Types.WccData{}, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.rename(from_fh, "missing.txt", from_fh, "new.txt", :auth, %{})
    end

    test "stale fhandle returns :stale" do
      assert {:error, :stale} =
               NFSv3Backend.rename(<<0::8>>, "x", <<0::8>>, "y", :auth, %{})
    end
  end

  ## write / commit (#624)

  describe "write/6" do
    setup do
      put_inode_table(%{@file_inode => {@volume_name, @file_path}})
      :ok
    end

    test "writes the bytes and reports :file_sync regardless of the requested hint" do
      pre = file_meta(%{size: 0})
      post = file_meta(%{size: 11})
      data = "hello-write"
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, @file_path] ->
          {:ok, pre}

        NeonFS.Core, :write_file_at, [@volume_name, @file_path, 0, ^data, _opts] ->
          send(test_pid, :write_called)
          {:ok, post}
      end)

      assert {:ok, %{wcc: wcc, count: 11, committed: :file_sync, verf: verf}} =
               NFSv3Backend.write(valid_fh(), 0, data, :unstable, :auth, %{})

      assert byte_size(verf) == 8
      assert %NFSServer.NFSv3.Types.WccData{before: %_{size: 0}, after: %Fattr3{size: 11}} = wcc
      assert_received :write_called
    end

    test "returns NFS3ERR_NOSPC + parent wcc when core surfaces it" do
      pre = file_meta(%{size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, @file_path] -> {:ok, pre}
        NeonFS.Core, :write_file_at, _ -> {:error, :nospc}
      end)

      assert {:error, :nospc, %NFSServer.NFSv3.Types.WccData{before: %_{}}} =
               NFSv3Backend.write(valid_fh(), 0, "x", :file_sync, :auth, %{})
    end

    test "stale fhandle returns :stale + empty wcc" do
      put_core(fn _, :get_file_meta, _ -> {:error, :not_found} end)

      assert {:error, :noent, %NFSServer.NFSv3.Types.WccData{before: nil, after: nil}} =
               NFSv3Backend.write(valid_fh(), 0, "x", :file_sync, :auth, %{})
    end
  end

  describe "commit/4" do
    setup do
      put_inode_table(%{@file_inode => {@volume_name, @file_path}})
      :ok
    end

    test "returns wcc + per-instance verf without re-writing data" do
      meta = file_meta()

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, @file_path] -> {:ok, meta}
        NeonFS.Core, :write_file_at, _ -> flunk("commit must not write")
      end)

      assert {:ok, %{wcc: %NFSServer.NFSv3.Types.WccData{}, verf: verf}} =
               NFSv3Backend.commit(valid_fh(), 0, 0, :auth, %{})

      assert byte_size(verf) == 8
    end

    test "verf is stable across calls within the same VM" do
      meta = file_meta()

      put_core(fn _, :get_file_meta, _ -> {:ok, meta} end)

      {:ok, %{verf: v1}} = NFSv3Backend.commit(valid_fh(), 0, 0, :auth, %{})
      {:ok, %{verf: v2}} = NFSv3Backend.commit(valid_fh(), 0, 0, :auth, %{})

      assert v1 == v2
    end

    test "stale fhandle returns :stale + empty wcc" do
      put_core(fn _, :get_file_meta, _ -> {:error, :not_found} end)

      assert {:error, :noent, %NFSServer.NFSv3.Types.WccData{before: nil, after: nil}} =
               NFSv3Backend.commit(valid_fh(), 0, 0, :auth, %{})
    end
  end

  ## mkdir / remove / rmdir (#623)

  describe "mkdir/5" do
    setup do
      put_inode_table(%{0xA000_A000_A000_A000 => {@volume_name, "/parent"}})
      :ok
    end

    test "creates a new directory and returns fhandle + attrs + wcc" do
      child_path = "/parent/newdir"
      pre_dir = file_meta(%{path: "/parent", mode: 0o040_755, size: 0})
      child_dir = file_meta(%{path: child_path, mode: 0o040_755, size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/parent"] -> {:ok, pre_dir}
        NeonFS.Core, :mkdir, [@volume_name, ^child_path] -> {:ok, %{path: child_path}}
        NeonFS.Core, :get_file_meta, [@volume_name, ^child_path] -> {:ok, child_dir}
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xA000_A000_A000_A000)

      assert {:ok, child_fh, %Fattr3{type: :dir}, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.mkdir(
                 dir_fh,
                 "newdir",
                 %NFSServer.NFSv3.Types.Sattr3{mode: 0o755},
                 :auth,
                 %{}
               )

      assert byte_size(child_fh) == Filehandle.size()
    end

    test "returns NFS3ERR_EXIST when target already exists" do
      pre_dir = file_meta(%{path: "/parent", mode: 0o040_755, size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/parent"] -> {:ok, pre_dir}
        NeonFS.Core, :mkdir, [@volume_name, "/parent/exists"] -> {:error, :eexist}
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xA000_A000_A000_A000)

      assert {:error, :exist, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.mkdir(
                 dir_fh,
                 "exists",
                 %NFSServer.NFSv3.Types.Sattr3{},
                 :auth,
                 %{}
               )
    end
  end

  describe "remove/4" do
    setup do
      put_inode_table(%{0xB000_B000_B000_B000 => {@volume_name, "/parent"}})
      :ok
    end

    test "deletes the file and returns wcc_data" do
      pre_dir = file_meta(%{path: "/parent", mode: 0o040_755, size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/parent"] -> {:ok, pre_dir}
        NeonFS.Core, :delete_file, [@volume_name, "/parent/doomed.txt"] -> :ok
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xB000_B000_B000_B000)

      assert {:ok, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.remove(dir_fh, "doomed.txt", :auth, %{})
    end

    test "returns NFS3ERR_NOENT when target doesn't exist" do
      pre_dir = file_meta(%{path: "/parent", mode: 0o040_755, size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/parent"] -> {:ok, pre_dir}
        NeonFS.Core, :delete_file, [@volume_name, "/parent/missing.txt"] -> {:error, :not_found}
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xB000_B000_B000_B000)

      assert {:error, :noent, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.remove(dir_fh, "missing.txt", :auth, %{})
    end
  end

  describe "rmdir/4" do
    setup do
      put_inode_table(%{0xC000_C000_C000_C000 => {@volume_name, "/parent"}})
      :ok
    end

    test "removes an empty directory" do
      child_path = "/parent/emptydir"
      pre_dir = file_meta(%{path: "/parent", mode: 0o040_755, size: 0})
      target = file_meta(%{path: child_path, mode: 0o040_755, size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/parent"] -> {:ok, pre_dir}
        NeonFS.Core, :get_file_meta, [@volume_name, ^child_path] -> {:ok, target}
        NeonFS.Core, :list_dir, [@volume_name, ^child_path] -> {:ok, []}
        NeonFS.Core, :delete_file, [@volume_name, ^child_path] -> :ok
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xC000_C000_C000_C000)

      assert {:ok, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.rmdir(dir_fh, "emptydir", :auth, %{})
    end

    test "returns NFS3ERR_NOTEMPTY when target has children" do
      child_path = "/parent/full"
      pre_dir = file_meta(%{path: "/parent", mode: 0o040_755, size: 0})
      target = file_meta(%{path: child_path, mode: 0o040_755, size: 0})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/parent"] ->
          {:ok, pre_dir}

        NeonFS.Core, :get_file_meta, [@volume_name, ^child_path] ->
          {:ok, target}

        NeonFS.Core, :list_dir, [@volume_name, ^child_path] ->
          {:ok, [%{path: child_path <> "/file.txt"}]}

        NeonFS.Core, :delete_file, _ ->
          flunk("delete_file must not be called when the dir is non-empty")
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xC000_C000_C000_C000)

      assert {:error, :notempty, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.rmdir(dir_fh, "full", :auth, %{})
    end

    test "returns NFS3ERR_NOTDIR when target is a regular file" do
      child_path = "/parent/regular.txt"
      pre_dir = file_meta(%{path: "/parent", mode: 0o040_755, size: 0})
      target = file_meta(%{path: child_path, mode: 0o100_644, size: 12})

      put_core(fn
        NeonFS.Core, :get_file_meta, [@volume_name, "/parent"] -> {:ok, pre_dir}
        NeonFS.Core, :get_file_meta, [@volume_name, ^child_path] -> {:ok, target}
        NeonFS.Core, :list_dir, _ -> flunk("list_dir must not be called for non-dir")
        NeonFS.Core, :delete_file, _ -> flunk("delete_file must not be called for non-dir")
      end)

      dir_fh = Filehandle.encode(@volume_id_bin, 0xC000_C000_C000_C000)

      assert {:error, :notdir, %NFSServer.NFSv3.Types.WccData{}} =
               NFSv3Backend.rmdir(dir_fh, "regular.txt", :auth, %{})
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
