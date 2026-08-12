defmodule NeonFS.Integration.CIFSSmbdTest do
  @moduledoc """
  Proves the SMB stack end to end: a real `smbd` loads `vfs_neonfs.so`,
  the module reaches a `neonfs_cifs` bridge over its ETF socket, and the
  bridge serves files out of a NeonFS volume to an SMB2 client.

  ## What this covers that nothing else does

  `neonfs_cifs`'s own suite drives the wire protocol against a mock
  responder, and `packaging/verify-smbd-sidecar.sh` proves an `smbd`
  built against the right Samba loads the module. Both stop short of the
  same place: the sidecar's share points at a socket nothing is
  listening on, so its tree connect fails immediately after the module
  loads. Everything between "the module loaded" and "the client got its
  bytes" — the arguments Samba really passes each hook, the handle
  lifecycle across operations, the mapping from an SMB path to a NeonFS
  file — has only ever been exercised against a stub.

  ## Architecture

  - 2-peer mixed-role cluster, the shape `containerd_content_test.exs`
    established: `node1` runs `:neonfs_core`, `node2` runs
    `:neonfs_cifs` as an interface peer.
  - `PeerCluster` allocates a per-test UDS under the CIFS peer's data
    dir and configures the bridge to bind there at startup.
  - The host spawns an `smbd` whose share carries
    `vfs objects = neonfs`, `neonfs:socket` pointing at that UDS, and
    `neonfs:volume` naming the volume.
  - `smbclient` drives SMB2 against it, so the assertions are about what
    a client observes rather than what the bridge was asked.

  ## Why this is nightly rather than per-push

  The module has to be built inside the distro's Samba source package to
  ABI- and symbol-version-match the `smbd` that loads it, which is a
  ~35-minute build on a cold cache. That is why the job runs on a
  schedule and on `workflow_dispatch` rather than per push, and why it is
  deliberately not a `canary` dependency.
  """

  use ExUnit.Case, async: false

  alias NeonFS.TestSupport.{ClusterCase, PeerCluster, Smbd}

  @moduletag :requires_smbd
  @moduletag timeout: 300_000

  @volume_name "cifs-e2e"
  @volume_opts %{
    durability: %{type: :replicate, factor: 1, min_copies: 1},
    compression: %{algorithm: :none, level: 0, min_size: 0}
  }

  setup do
    cluster =
      PeerCluster.start_cluster!(2,
        roles: %{node1: [:neonfs_core], node2: [:neonfs_cifs]}
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)

    :ok =
      ClusterCase.init_mixed_role_cluster(cluster,
        name: "cifs-smbd-test",
        volumes: [{@volume_name, @volume_opts}]
      )

    cifs_peer = PeerCluster.get_node!(cluster, :node2)
    socket_path = cifs_peer.interface_ports.cifs

    :ok = wait_for_socket(socket_path, 30_000)

    dir = Path.join(cifs_peer.data_dir, "smbd")

    server = Smbd.start!(dir: dir, socket_path: socket_path, volume: @volume_name)
    on_exit(fn -> Smbd.stop(server) end)

    {:ok, cluster: cluster, server: server, socket_path: socket_path, dir: dir}
  end

  describe "file round trip" do
    test "a file written over SMB reads back byte-identical", %{server: server, dir: dir} do
      # Not random bytes: a mismatch in a readable payload says *where* it
      # went wrong — a truncation and a corrupted middle look the same in
      # a hash comparison.
      payload = for i <- 1..2048, into: "", do: "line #{i}\n"
      local = Path.join(dir, "upload.txt")
      File.write!(local, payload)

      assert {:ok, _} = Smbd.client(server, "put #{local} round-trip.txt"),
             Smbd.logs(server)

      fetched = Path.join(dir, "downloaded.txt")
      assert {:ok, _} = Smbd.client(server, "get round-trip.txt #{fetched}")

      assert File.read!(fetched) == payload
    end

    test "the file is visible to the cluster, not only to smbd", %{
      cluster: cluster,
      server: server,
      dir: dir
    } do
      local = Path.join(dir, "through.txt")
      File.write!(local, "written by smbclient")

      assert {:ok, _} = Smbd.client(server, "put #{local} through.txt")

      # The point of the whole stack: the bytes are in the volume, not in
      # smbd's share directory. Reading them back through core proves the
      # write went through the bridge rather than landing on local disk.
      assert {:ok, bytes} =
               PeerCluster.rpc(cluster, :node1, NeonFS.Core, :read_file, [
                 @volume_name,
                 "/through.txt"
               ])

      assert bytes == "written by smbclient"
    end

    test "a listing shows what was written and hides what was deleted", %{
      server: server,
      dir: dir
    } do
      local = Path.join(dir, "listed.txt")
      File.write!(local, "listed")

      assert {:ok, _} = Smbd.client(server, "put #{local} listed.txt")
      assert {:ok, listing} = Smbd.client(server, "ls")
      assert listing =~ "listed.txt"

      assert {:ok, _} = Smbd.client(server, "del listed.txt")
      assert {:ok, after_delete} = Smbd.client(server, "ls")
      refute after_delete =~ "listed.txt"
    end

    test "a rename moves the name and keeps the bytes", %{server: server, dir: dir} do
      local = Path.join(dir, "before.txt")
      File.write!(local, "same bytes either side")

      assert {:ok, _} = Smbd.client(server, "put #{local} before.txt")
      assert {:ok, _} = Smbd.client(server, "rename before.txt after.txt")

      assert {:ok, listing} = Smbd.client(server, "ls")
      assert listing =~ "after.txt"
      refute listing =~ "before.txt"

      fetched = Path.join(dir, "after.txt")
      assert {:ok, _} = Smbd.client(server, "get after.txt #{fetched}")
      assert File.read!(fetched) == "same bytes either side"
    end
  end

  describe "directories" do
    test "a directory can be created, listed into and removed", %{server: server, dir: dir} do
      local = Path.join(dir, "nested.txt")
      File.write!(local, "in a directory")

      assert {:ok, _} = Smbd.client(server, "mkdir sub")
      assert {:ok, _} = Smbd.client(server, "put #{local} sub\\nested.txt")

      assert {:ok, listing} = Smbd.client(server, "cd sub; ls")
      assert listing =~ "nested.txt"

      assert {:ok, _} = Smbd.client(server, "deltree sub")
      assert {:ok, after_delete} = Smbd.client(server, "ls")
      refute after_delete =~ "sub"
    end

    # `fchmod` / `fntimes` / `fstat` on a *directory* once returned EBADF,
    # because directory handles were never registered with a
    # `{volume, path}` and so missed at handle lookup. That has handler
    # coverage, but never against a real smbd — which is the layer that
    # decides whether an op arrives by handle or by path, so it is the
    # only place the distinction that broke can actually be observed.
    test "a directory's mode survives a change made through its handle", %{server: server} do
      assert {:ok, _} = Smbd.client(server, "mkdir attrs")

      # `setmode` on a directory is what drove the EBADF: smbd opens the
      # directory and issues the attribute change against that handle.
      assert {:ok, _} = Smbd.client(server, "setmode attrs +r")

      assert {:ok, listing} = Smbd.client(server, "ls attrs")
      refute listing =~ "NT_STATUS_", "attribute op on a directory failed:\n#{listing}"
    end

    test "allinfo on a directory answers through the open handle", %{server: server} do
      assert {:ok, _} = Smbd.client(server, "mkdir queried")

      assert {:ok, info} = Smbd.client(server, "allinfo queried")

      # `attributes: D` is the directory bit, and it only gets there by way
      # of an `fstat` on the handle smbclient opened.
      assert info =~ ~r/attributes:.*\bD\b/, "allinfo did not report a directory:\n#{info}"

      # smbclient also asks for VSS shadow-copy data, which the module does
      # not implement and NeonFS has no equivalent of, so its
      # INVALID_DEVICE_REQUEST is expected. Asserting no NT_STATUS appears
      # anywhere would fail on that rather than on anything this covers.
      refute info =~ "NT_STATUS_OBJECT", "path resolution failed:\n#{info}"
      refute info =~ "NT_STATUS_ACCESS", "access check failed:\n#{info}"
    end
  end

  describe "error paths" do
    test "a share naming a volume that does not exist refuses the tree connect", %{
      socket_path: socket_path,
      dir: dir
    } do
      absent =
        Smbd.start!(
          dir: Path.join(dir, "absent"),
          socket_path: socket_path,
          volume: "no-such-volume"
        )

      on_exit(fn -> Smbd.stop(absent) end)

      # The module loads — this is not a module fault — and then the
      # bridge refuses to resolve a volume it does not have.
      assert {:error, {_status, output}} = Smbd.client(absent, "ls")
      assert output =~ "NT_STATUS_"

      refute Smbd.logs(absent) =~ "error probing vfs module",
             "the module failed to load, so this proves nothing about the volume"
    end

    test "a read-only share refuses a write and still serves a read", %{
      server: server,
      socket_path: socket_path,
      dir: dir
    } do
      seeded = Path.join(dir, "seeded.txt")
      File.write!(seeded, "readable")
      assert {:ok, _} = Smbd.client(server, "put #{seeded} seeded.txt")

      ro =
        Smbd.start!(
          dir: Path.join(dir, "readonly"),
          socket_path: socket_path,
          volume: @volume_name,
          read_only: true
        )

      on_exit(fn -> Smbd.stop(ro) end)

      denied = Path.join(dir, "denied.txt")
      File.write!(denied, "should not land")

      assert {:error, {_status, output}} = Smbd.client(ro, "put #{denied} denied.txt")
      assert output =~ "NT_STATUS_"

      fetched = Path.join(dir, "from-readonly.txt")
      assert {:ok, _} = Smbd.client(ro, "get seeded.txt #{fetched}")
      assert File.read!(fetched) == "readable"
    end
  end

  # The rest of the suite runs one core peer and one interface peer, which
  # has no minority to be in. This block builds its own topology: three
  # core peers so a majority exists to be cut off from, and the bridge peer
  # isolated alongside the one core node it can still reach.
  describe "minority partition" do
    @describetag timeout: 420_000

    test "a write through a share served by a minority node is refused" do
      cluster =
        PeerCluster.start_cluster!(4,
          roles: %{
            node1: [:neonfs_core],
            node2: [:neonfs_core],
            node3: [:neonfs_core],
            node4: [:neonfs_cifs]
          }
        )

      on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

      PeerCluster.connect_nodes(cluster)

      :ok =
        ClusterCase.init_mixed_role_cluster(cluster,
          name: "cifs-partition-test",
          volumes: [{@volume_name, @volume_opts}]
        )

      cifs_peer = PeerCluster.get_node!(cluster, :node4)
      :ok = wait_for_socket(cifs_peer.interface_ports.cifs, 30_000)

      dir = Path.join(cifs_peer.data_dir, "smbd")

      server =
        Smbd.start!(dir: dir, socket_path: cifs_peer.interface_ports.cifs, volume: @volume_name)

      on_exit(fn -> Smbd.stop(server) end)

      local = Path.join(dir, "partitioned.txt")
      File.write!(local, "written while whole")

      # Establish that the share works before the partition, so a refusal
      # afterwards is attributable to the partition rather than to a share
      # that never worked.
      assert {:ok, _} = Smbd.client(server, "put #{local} before-partition.txt"),
             Smbd.logs(server)

      # node1 and the bridge on one side, the majority on the other. The
      # bridge can still reach a core node; that node just cannot commit.
      :ok = PeerCluster.partition_cluster(cluster, [[:node1, :node4], [:node2, :node3]])

      assert {:error, {_status, output}} =
               Smbd.client(server, "put #{local} during-partition.txt")

      # The architecture specifies write protection for this case. What
      # matters first is that the write is refused rather than acked — a
      # block-level ack for something no quorum accepted is the failure
      # this is really guarding against.
      assert output =~ "NT_STATUS_",
             "a write in the minority was not refused:\n#{output}"

      :ok = PeerCluster.heal_partition(cluster)

      # And that the refusal was the partition talking, not a wedged
      # cluster: the same write succeeds once the majority is reachable.
      assert eventually_writes?(server, local),
             "the share never recovered after the partition healed:\n#{Smbd.logs(server)}"
    end
  end

  # Recovery is not instantaneous — the bridge's client has to notice the
  # core node is reachable again — so this polls rather than asserting on
  # the first attempt after the heal.
  defp eventually_writes?(server, local, attempts \\ 30) do
    case Smbd.client(server, "put #{local} after-partition.txt") do
      {:ok, _} ->
        true

      {:error, _} when attempts > 1 ->
        Process.sleep(1_000)
        eventually_writes?(server, local, attempts - 1)

      {:error, _} ->
        false
    end
  end

  defp wait_for_socket(path, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_socket(path, deadline)
  end

  defp do_wait_for_socket(path, deadline) do
    cond do
      socket_listening?(path) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(100)
        do_wait_for_socket(path, deadline)
    end
  end

  defp socket_listening?(path) do
    case File.stat(path) do
      {:ok, %{type: :other}} ->
        case :gen_tcp.connect({:local, path}, 0, [:binary, active: false], 250) do
          {:ok, sock} ->
            :gen_tcp.close(sock)
            true

          {:error, _} ->
            false
        end

      _ ->
        false
    end
  end
end
