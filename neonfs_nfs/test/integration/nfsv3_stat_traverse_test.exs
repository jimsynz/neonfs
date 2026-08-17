defmodule NeonFS.Integration.NFSv3StatTraverseTest do
  @moduledoc """
  `ls -l` over NFSv3 on files the caller cannot read.

  POSIX `stat()` needs search on each directory component and no permission
  at all on the target — which is what makes `ls -l` work in a directory
  full of unreadable files. Core used to authorise `:read` on the file
  itself for `get_file_meta/3`, so a non-root AUTH_SYS identity got
  `NFS3ERR_ACCES` where local POSIX succeeds. This drives the real backend
  against a real cluster so the property is asserted where a client sees it,
  rather than through a mocked core.

  The counterpart still has to hold: a directory the caller cannot search
  hides the files under it.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.NFS.{Filehandle, InodeTable}
  alias NeonFS.TestSupport.PeerCluster
  alias Tahr.NFSv3.{Handler, Types}
  alias Tahr.RPC.Auth

  @moduletag timeout: 300_000
  @moduletag :integration
  @moduletag :nfs
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  # The identity the property is about: a mounted client that is not root
  # and owns none of the files it is listing.
  @client %Auth.Sys{uid: 1000, gid: 1000, gids: [1000]}

  @getattr 1
  @lookup 3

  setup_all %{cluster: cluster} do
    :ok = init_single_node_cluster(cluster, name: "nfsv3-stat-traverse")

    # node1 is itself a core node, so `core_call/3` goes through a local
    # apply rather than the Router/Discovery stack that non-core interface
    # peers use.
    :ok =
      PeerCluster.rpc(cluster, :node1, Application, :put_env, [
        :neonfs_nfs,
        :core_call_fn,
        &:erlang.apply/3
      ])

    :ok =
      PeerCluster.rpc(cluster, :node1, Application, :put_env, [
        :neonfs_nfs,
        :handler_stack,
        :beam
      ])

    on_exit(fn ->
      try do
        PeerCluster.rpc(cluster, :node1, Application, :delete_env, [:neonfs_nfs, :core_call_fn])
        PeerCluster.rpc(cluster, :node1, Application, :delete_env, [:neonfs_nfs, :handler_stack])
      catch
        _, _ -> :ok
      end
    end)

    volume = "nfsv3-stat-vol-#{System.unique_integer([:positive])}"

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume,
        %{"durability" => "replicate:1"}
      ])

    assert_eventually timeout: 10_000 do
      match?(
        {:ok, _},
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [volume])
      )
    end

    # Root-owned and unreadable by anyone else — the file `ls -l` has to be
    # able to describe without being able to open.
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core, :write_file_streamed, [
        volume,
        "/classified.txt",
        ["secret"],
        [uid: 0, mode: 0o600]
      ])

    # A directory with no search bit for others, holding a world-readable
    # file: the file's own mode must not be what decides.
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core, :mkdir, [
        volume,
        "/closed",
        [uid: 0, mode: 0o700]
      ])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core, :write_file_streamed, [
        volume,
        "/closed/inside.txt",
        ["public"],
        [uid: 0, mode: 0o644]
      ])

    case PeerCluster.rpc(cluster, :node1, GenServer, :start, [
           InodeTable,
           [],
           [name: InodeTable]
         ]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    {:ok, root_inode} =
      PeerCluster.rpc(cluster, :node1, InodeTable, :allocate_inode, [volume, "/"])

    {:ok, vol_struct} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [volume])

    {:ok, vol_id_bin} =
      PeerCluster.rpc(cluster, :node1, Filehandle, :volume_uuid_to_binary, [vol_struct.id])

    root_fh = PeerCluster.rpc(cluster, :node1, Filehandle, :encode, [vol_id_bin, root_inode])

    %{volume: volume, root_fh: root_fh}
  end

  test "LOOKUP and GETATTR describe a file the client cannot read",
       %{cluster: cluster, root_fh: root_fh} do
    {:ok, body} =
      call_proc(cluster, @lookup, Types.encode_diropargs3({root_fh, "classified.txt"}))

    assert {:ok, :ok, rest} = Types.decode_nfsstat3(IO.iodata_to_binary(body))
    assert {:ok, file_fh, _rest} = Types.decode_fhandle3(rest)

    {:ok, attr_body} = call_proc(cluster, @getattr, Types.encode_fhandle3(file_fh))

    assert {:ok, :ok, rest} = Types.decode_nfsstat3(IO.iodata_to_binary(attr_body))
    assert {:ok, attr, <<>>} = Types.decode_fattr3(rest)

    # The mode the client is shown is the one that would refuse it a read —
    # describing the file is not the same as opening it.
    assert attr.mode == 0o600
    assert attr.uid == 0
  end

  test "LOOKUP is refused through a directory the client cannot search",
       %{cluster: cluster, root_fh: root_fh} do
    {:ok, body} = call_proc(cluster, @lookup, Types.encode_diropargs3({root_fh, "closed"}))

    assert {:ok, :ok, rest} = Types.decode_nfsstat3(IO.iodata_to_binary(body))
    assert {:ok, dir_fh, _rest} = Types.decode_fhandle3(rest)

    {:ok, denied} = call_proc(cluster, @lookup, Types.encode_diropargs3({dir_fh, "inside.txt"}))

    assert {:ok, :acces, _rest} = Types.decode_nfsstat3(IO.iodata_to_binary(denied))
  end

  defp call_proc(cluster, proc, args) do
    ctx = %{call: nil, nfs_v3_backend: NeonFS.NFS.NFSv3Backend}
    PeerCluster.rpc(cluster, :node1, Handler, :handle_call, [proc, args, @client, ctx])
  end
end
