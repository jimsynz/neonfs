defmodule NeonFS.CSI.CsiSanityTest do
  @moduledoc """
  Runs the upstream [csi-sanity](https://github.com/kubernetes-csi/csi-test)
  conformance suite against the real `neonfs_csi` gRPC endpoint.

  This is the CI-runnable slice of #319: it exercises the Identity and
  Controller services over a real Unix-socket gRPC connection, against
  the actual `NeonFS.CSI.Endpoint`, with the NeonFS core RPCs served by
  an in-memory stateful stub (`NeonFS.CSI.TestSupport.CoreStub`). It
  proves the driver's wire behaviour conforms to the CSI spec —
  request/response shapes, error codes, idempotency, pagination — which
  the in-process unit tests don't check.

  Two groups of specs are skipped, and both are tracked, not hidden:

    * **Node service** — needs a real FUSE mount on a kubelet host, the
      job of the full kind end-to-end test still outstanding on
      #319 / #995.
    * **One Controller spec (#1458)** — capacity-mismatch idempotency,
      which needs a volume capacity model (blocked on #1462 / #1463).
      Skipped by the precise description pattern below and tracked in
      #1458; everything else gates conformance today.

  Tagged `:requires_csi_sanity`; `test/test_helper.exs` excludes the tag
  unless a `csi-sanity` binary is found (on `PATH` or via the
  `CSI_SANITY` env var), so the suite skips gracefully where the tool is
  absent and runs where CI installs it.
  """

  use ExUnit.Case, async: false

  alias NeonFS.CSI.{NodeServer, VolumeHealth}
  alias NeonFS.CSI.TestSupport.CoreStub

  @moduletag :requires_csi_sanity
  @moduletag timeout: 300_000

  # Ginkgo `--skip` regexes matched against each spec's full description.
  # Node service is out of scope for a stubbed-core run; the rest are the
  # driver conformance gaps tracked in #1458. Each pattern is anchored to
  # its service so it can't over-match a passing spec.
  @skip_specs [
    "Node Service",
    "CreateVolume.*already existing name and different capacity"
  ]

  setup do
    NodeServer.init_state_tables()
    VolumeHealth.init_table()

    {:ok, agent} = CoreStub.start_link()
    Application.put_env(:neonfs_csi, :core_call_fn, CoreStub.handler(agent))

    socket_path =
      Path.join(System.tmp_dir!(), "neonfs_csi_sanity_#{System.unique_integer([:positive])}.sock")

    File.rm(socket_path)

    {:ok, server} =
      GRPC.Server.Supervisor.start_link(
        endpoint: NeonFS.CSI.Endpoint,
        port: 0,
        start_server: true,
        adapter_opts: [ip: {:local, socket_path}]
      )

    # csi-sanity creates (and removes) these leaf dirs itself, once per
    # spec, with `os.Mkdir` — which errors if the dir already exists. So
    # hand it paths whose parent (tmp_dir) exists but whose leaf does not.
    staging_dir = tmp_path("staging")
    mount_dir = tmp_path("mount")

    on_exit(fn ->
      # `server` is linked to this setup's process, so it may already be
      # gone by the time on_exit runs — stop it defensively.
      try do
        Supervisor.stop(server)
      catch
        :exit, _ -> :ok
      end

      Application.delete_env(:neonfs_csi, :core_call_fn)
      File.rm(socket_path)
      File.rm_rf(staging_dir)
      File.rm_rf(mount_dir)
    end)

    %{socket_path: socket_path, staging_dir: staging_dir, mount_dir: mount_dir}
  end

  test "the driver passes csi-sanity Identity + Controller conformance", ctx do
    endpoint = "unix://" <> ctx.socket_path

    {output, exit_code} =
      System.cmd(
        csi_sanity_bin(),
        [
          "-csi.endpoint",
          endpoint,
          "-csi.stagingdir",
          ctx.staging_dir,
          "-csi.mountdir",
          ctx.mount_dir,
          "--ginkgo.skip",
          Enum.join(@skip_specs, "|")
        ],
        stderr_to_stdout: true
      )

    assert exit_code == 0, """
    csi-sanity reported conformance failures (Node-service specs skipped):

    #{output}
    """
  end

  defp csi_sanity_bin do
    System.get_env("CSI_SANITY") || System.find_executable("csi-sanity") ||
      flunk("csi-sanity not found (set CSI_SANITY or add it to PATH)")
  end

  defp tmp_path(name) do
    Path.join(
      System.tmp_dir!(),
      "neonfs_csi_sanity_#{name}_#{System.unique_integer([:positive])}"
    )
  end
end
