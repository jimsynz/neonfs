defmodule NeonFS.CSI.NodeServerTest do
  use ExUnit.Case, async: false

  alias Csi.V1.{
    NodeGetCapabilitiesRequest,
    NodeGetInfoRequest,
    NodeGetVolumeStatsRequest,
    NodePublishVolumeRequest,
    NodeServiceCapability,
    NodeStageVolumeRequest,
    NodeStageVolumeResponse,
    NodeUnpublishVolumeRequest,
    NodeUnstageVolumeRequest,
    VolumeCapability
  }

  alias NeonFS.Core.Volume
  alias NeonFS.CSI.{NodeServer, VolumeHealth}

  @rw_capability %VolumeCapability{
    access_mode: %VolumeCapability.AccessMode{mode: :SINGLE_NODE_WRITER}
  }

  @ro_capability %VolumeCapability{
    access_mode: %VolumeCapability.AccessMode{mode: :MULTI_NODE_READER_ONLY}
  }

  @block_capability %VolumeCapability{
    access_type: {:block, %VolumeCapability.BlockVolume{}},
    access_mode: %VolumeCapability.AccessMode{mode: :SINGLE_NODE_WRITER}
  }

  setup do
    NodeServer.reset_state_tables()
    VolumeHealth.reset_table()

    test_pid = self()

    Application.put_env(:neonfs_csi, :fuse_mount_fn, fn vol, path ->
      send(test_pid, {:fuse_mount, vol, path})
      {:ok, {:mock_mount, vol}}
    end)

    Application.put_env(:neonfs_csi, :fuse_unmount_fn, fn mount_id ->
      send(test_pid, {:fuse_unmount, mount_id})
      :ok
    end)

    Application.put_env(:neonfs_csi, :bind_mount_fn, fn src, dst, ro? ->
      send(test_pid, {:bind_mount, src, dst, ro?})
      :ok
    end)

    Application.put_env(:neonfs_csi, :bind_unmount_fn, fn target ->
      send(test_pid, {:bind_unmount, target})
      :ok
    end)

    Application.put_env(:neonfs_csi, :block_attach_fn, fn vol ->
      send(test_pid, {:block_attach, vol})
      {:ok, "/dev/nbd0"}
    end)

    Application.put_env(:neonfs_csi, :block_detach_fn, fn device ->
      send(test_pid, {:block_detach, device})
      :ok
    end)

    on_exit(fn ->
      Application.delete_env(:neonfs_csi, :fuse_mount_fn)
      Application.delete_env(:neonfs_csi, :fuse_unmount_fn)
      Application.delete_env(:neonfs_csi, :bind_mount_fn)
      Application.delete_env(:neonfs_csi, :bind_unmount_fn)
      Application.delete_env(:neonfs_csi, :block_attach_fn)
      Application.delete_env(:neonfs_csi, :block_detach_fn)
      Application.delete_env(:neonfs_csi, :node_id)
      Application.delete_env(:neonfs_csi, :core_call_fn)
      NodeServer.reset_state_tables()
      VolumeHealth.reset_table()
    end)

    staging_root =
      Path.join(System.tmp_dir!(), "csi_node_test_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(staging_root) end)

    {:ok, staging_root: staging_root}
  end

  describe "NodeGetCapabilities" do
    test "advertises STAGE_UNSTAGE_VOLUME, GET_VOLUME_STATS, VOLUME_CONDITION" do
      reply = NodeServer.node_get_capabilities(%NodeGetCapabilitiesRequest{}, nil)

      types =
        Enum.map(reply.capabilities, fn %NodeServiceCapability{type: {:rpc, rpc}} -> rpc.type end)

      assert :STAGE_UNSTAGE_VOLUME in types
      assert :GET_VOLUME_STATS in types
      assert :VOLUME_CONDITION in types
    end
  end

  describe "NodeGetInfo" do
    test "returns the configured node_id" do
      Application.put_env(:neonfs_csi, :node_id, "k8s-worker-7")
      reply = NodeServer.node_get_info(%NodeGetInfoRequest{}, nil)
      assert reply.node_id == "k8s-worker-7"
    end

    test "falls back to Node.self when nothing configured" do
      Application.delete_env(:neonfs_csi, :node_id)
      reply = NodeServer.node_get_info(%NodeGetInfoRequest{}, nil)
      assert reply.node_id == to_string(Node.self())
    end
  end

  describe "NodeStageVolume" do
    test "mounts the volume via the FUSE mount fn", %{staging_root: root} do
      staging = Path.join(root, "stage-1")

      reply =
        NodeServer.node_stage_volume(
          %NodeStageVolumeRequest{
            volume_id: "vol-1",
            staging_target_path: staging,
            volume_capability: @rw_capability
          },
          nil
        )

      assert %_{} = reply
      assert_received {:fuse_mount, "vol-1", ^staging}
      assert File.dir?(staging)

      assert [{"vol-1", %{staging_path: ^staging, mount_id: {:mock_mount, "vol-1"}}}] =
               :ets.lookup(:csi_node_staged, "vol-1")
    end

    test "is idempotent for the same (volume, staging_path)", %{staging_root: root} do
      staging = Path.join(root, "stage-2")

      req = %NodeStageVolumeRequest{
        volume_id: "vol-2",
        staging_target_path: staging,
        volume_capability: @rw_capability
      }

      NodeServer.node_stage_volume(req, nil)
      assert_received {:fuse_mount, "vol-2", ^staging}

      NodeServer.node_stage_volume(req, nil)
      refute_received {:fuse_mount, _, _}
    end

    test "rejects re-stage at a different path", %{staging_root: root} do
      staging1 = Path.join(root, "stage-3a")
      staging2 = Path.join(root, "stage-3b")

      NodeServer.node_stage_volume(
        %NodeStageVolumeRequest{
          volume_id: "vol-3",
          staging_target_path: staging1,
          volume_capability: @rw_capability
        },
        nil
      )

      assert_raise GRPC.RPCError, ~r/already staged/, fn ->
        NodeServer.node_stage_volume(
          %NodeStageVolumeRequest{
            volume_id: "vol-3",
            staging_target_path: staging2,
            volume_capability: @rw_capability
          },
          nil
        )
      end
    end

    test "raises invalid_argument with empty volume_id", %{staging_root: root} do
      assert_raise GRPC.RPCError, ~r/volume_id is required/, fn ->
        NodeServer.node_stage_volume(
          %NodeStageVolumeRequest{
            volume_id: "",
            staging_target_path: Path.join(root, "x"),
            volume_capability: @rw_capability
          },
          nil
        )
      end
    end

    test "raises invalid_argument with empty staging_target_path" do
      assert_raise GRPC.RPCError, ~r/staging_target_path is required/, fn ->
        NodeServer.node_stage_volume(
          %NodeStageVolumeRequest{
            volume_id: "vol-x",
            staging_target_path: "",
            volume_capability: @rw_capability
          },
          nil
        )
      end
    end

    # A stage without the controller's attach is either a CO that skipped
    # it or something driving the plugin directly, and neither should be
    # handed a device another node may hold.
    test "refuses a block stage that did not go through the controller", %{staging_root: root} do
      assert_raise GRPC.RPCError, ~r/was not attached through the controller/, fn ->
        NodeServer.node_stage_volume(
          %NodeStageVolumeRequest{
            volume_id: "vol-block",
            staging_target_path: Path.join(root, "unattached"),
            volume_capability: @block_capability
          },
          nil
        )
      end
    end

    test "stages a mount volume without any attach context", %{staging_root: root} do
      assert %NodeStageVolumeResponse{} =
               NodeServer.node_stage_volume(
                 %NodeStageVolumeRequest{
                   volume_id: "vol-mount",
                   staging_target_path: Path.join(root, "mount"),
                   volume_capability: @rw_capability
                 },
                 nil
               )
    end

    test "stages a block volume by attaching its device", %{staging_root: root} do
      staging = Path.join(root, "block")

      assert %NodeStageVolumeResponse{} =
               NodeServer.node_stage_volume(
                 %NodeStageVolumeRequest{
                   volume_id: "vol-block",
                   staging_target_path: staging,
                   volume_capability: @block_capability,
                   publish_context: %{"neonfs.attached_node" => "worker-a"}
                 },
                 nil
               )

      assert_received {:block_attach, "vol-block"}
      refute_received {:fuse_mount, _, _}

      # The staging path is a directory even for a block volume — the spec
      # says so regardless of access type — but nothing is mounted on it.
      assert File.dir?(staging)
    end

    # `/dev/nbd0` and `/dev/ublkb0` look alike to everything above them and
    # CSI has no response field for it, so if this is not emitted there is
    # nothing an operator can read to tell which frontend served a volume.
    test "reports the frontend the device came up on", %{staging_root: root} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [[:neonfs, :csi, :block, :staged]])

      on_exit(fn -> :telemetry.detach(ref) end)

      assert %NodeStageVolumeResponse{} =
               NodeServer.node_stage_volume(
                 %NodeStageVolumeRequest{
                   volume_id: "vol-reported",
                   staging_target_path: Path.join(root, "reported"),
                   volume_capability: @block_capability,
                   publish_context: %{"neonfs.attached_node" => "worker-a"}
                 },
                 nil
               )

      assert_received {[:neonfs, :csi, :block, :staged], ^ref, %{count: 1}, metadata}
      assert metadata.volume_id == "vol-reported"
      assert metadata.frontend == :nbd
      assert metadata.device_path == "/dev/nbd0"
    end

    test "surfaces fuse mount errors as INTERNAL", %{staging_root: root} do
      Application.put_env(:neonfs_csi, :fuse_mount_fn, fn _, _ -> {:error, :enoent} end)

      assert_raise GRPC.RPCError, ~r/fuse mount failed/, fn ->
        NodeServer.node_stage_volume(
          %NodeStageVolumeRequest{
            volume_id: "vol-err",
            staging_target_path: Path.join(root, "err"),
            volume_capability: @rw_capability
          },
          nil
        )
      end
    end
  end

  describe "NodeUnstageVolume" do
    setup %{staging_root: root} do
      staging = Path.join(root, "stage-unstage")

      NodeServer.node_stage_volume(
        %NodeStageVolumeRequest{
          volume_id: "vol-u",
          staging_target_path: staging,
          volume_capability: @rw_capability
        },
        nil
      )

      assert_receive {:fuse_mount, _, _}
      {:ok, staging: staging}
    end

    test "unmounts and clears state", %{staging: staging} do
      reply =
        NodeServer.node_unstage_volume(
          %NodeUnstageVolumeRequest{volume_id: "vol-u", staging_target_path: staging},
          nil
        )

      assert %_{} = reply
      assert_received {:fuse_unmount, {:mock_mount, "vol-u"}}
      assert :ets.lookup(:csi_node_staged, "vol-u") == []
    end

    test "is idempotent for unknown volumes", %{staging_root: root} do
      reply =
        NodeServer.node_unstage_volume(
          %NodeUnstageVolumeRequest{
            volume_id: "ghost",
            staging_target_path: Path.join(root, "ghost")
          },
          nil
        )

      assert %_{} = reply
      refute_received {:fuse_unmount, _}
    end

    test "refuses while publishes are outstanding", %{staging: staging, staging_root: root} do
      target = Path.join(root, "target-pub")

      NodeServer.node_publish_volume(
        %NodePublishVolumeRequest{
          volume_id: "vol-u",
          staging_target_path: staging,
          target_path: target,
          volume_capability: @rw_capability,
          readonly: false
        },
        nil
      )

      assert_receive {:bind_mount, ^staging, ^target, false}

      assert_raise GRPC.RPCError, ~r/active publishes/, fn ->
        NodeServer.node_unstage_volume(
          %NodeUnstageVolumeRequest{volume_id: "vol-u", staging_target_path: staging},
          nil
        )
      end
    end

    test "rejects mismatched staging_target_path", %{staging_root: root} do
      assert_raise GRPC.RPCError, ~r/staged at/, fn ->
        NodeServer.node_unstage_volume(
          %NodeUnstageVolumeRequest{
            volume_id: "vol-u",
            staging_target_path: Path.join(root, "wrong-path")
          },
          nil
        )
      end
    end
  end

  describe "NodePublishVolume" do
    setup %{staging_root: root} do
      staging = Path.join(root, "stage-pub")

      NodeServer.node_stage_volume(
        %NodeStageVolumeRequest{
          volume_id: "vol-p",
          staging_target_path: staging,
          volume_capability: @rw_capability
        },
        nil
      )

      assert_receive {:fuse_mount, _, _}
      {:ok, staging: staging}
    end

    test "bind-mounts staging into target", %{staging: staging, staging_root: root} do
      target = Path.join(root, "pod-target")

      reply =
        NodeServer.node_publish_volume(
          %NodePublishVolumeRequest{
            volume_id: "vol-p",
            staging_target_path: staging,
            target_path: target,
            volume_capability: @rw_capability,
            readonly: false
          },
          nil
        )

      assert %_{} = reply
      assert_received {:bind_mount, ^staging, ^target, false}
      assert File.dir?(target)
    end

    test "passes the readonly flag through", %{staging: staging, staging_root: root} do
      target = Path.join(root, "pod-ro")

      NodeServer.node_publish_volume(
        %NodePublishVolumeRequest{
          volume_id: "vol-p",
          staging_target_path: staging,
          target_path: target,
          volume_capability: @ro_capability,
          readonly: true
        },
        nil
      )

      assert_received {:bind_mount, ^staging, ^target, true}
    end

    test "is idempotent for the same target + mode", %{staging: staging, staging_root: root} do
      target = Path.join(root, "pod-idem")

      req = %NodePublishVolumeRequest{
        volume_id: "vol-p",
        staging_target_path: staging,
        target_path: target,
        volume_capability: @rw_capability,
        readonly: false
      }

      NodeServer.node_publish_volume(req, nil)
      assert_receive {:bind_mount, ^staging, ^target, false}

      NodeServer.node_publish_volume(req, nil)
      refute_received {:bind_mount, _, _, _}
    end

    test "rejects re-publish with different readonly", %{staging: staging, staging_root: root} do
      target = Path.join(root, "pod-mismatch")

      NodeServer.node_publish_volume(
        %NodePublishVolumeRequest{
          volume_id: "vol-p",
          staging_target_path: staging,
          target_path: target,
          volume_capability: @rw_capability,
          readonly: false
        },
        nil
      )

      assert_receive {:bind_mount, _, _, _}

      assert_raise GRPC.RPCError, ~r/already published/, fn ->
        NodeServer.node_publish_volume(
          %NodePublishVolumeRequest{
            volume_id: "vol-p",
            staging_target_path: staging,
            target_path: target,
            volume_capability: @ro_capability,
            readonly: true
          },
          nil
        )
      end
    end

    test "refuses publish for unstaged volume", %{staging_root: root} do
      assert_raise GRPC.RPCError, ~r/not staged/, fn ->
        NodeServer.node_publish_volume(
          %NodePublishVolumeRequest{
            volume_id: "ghost",
            staging_target_path: Path.join(root, "ghost-stage"),
            target_path: Path.join(root, "ghost-target"),
            volume_capability: @rw_capability,
            readonly: false
          },
          nil
        )
      end
    end

    test "raises invalid_argument with empty staging_target_path", %{staging_root: root} do
      assert_raise GRPC.RPCError, ~r/staging_target_path is required/, fn ->
        NodeServer.node_publish_volume(
          %NodePublishVolumeRequest{
            volume_id: "vol-p",
            staging_target_path: "",
            target_path: Path.join(root, "x"),
            volume_capability: @rw_capability,
            readonly: false
          },
          nil
        )
      end
    end
  end

  describe "NodeUnpublishVolume" do
    setup %{staging_root: root} do
      staging = Path.join(root, "stage-unp")
      target = Path.join(root, "target-unp")

      NodeServer.node_stage_volume(
        %NodeStageVolumeRequest{
          volume_id: "vol-up",
          staging_target_path: staging,
          volume_capability: @rw_capability
        },
        nil
      )

      assert_receive {:fuse_mount, _, _}

      NodeServer.node_publish_volume(
        %NodePublishVolumeRequest{
          volume_id: "vol-up",
          staging_target_path: staging,
          target_path: target,
          volume_capability: @rw_capability,
          readonly: false
        },
        nil
      )

      assert_receive {:bind_mount, _, _, _}

      {:ok, staging: staging, target: target}
    end

    test "unmounts the bind mount and clears state", %{target: target} do
      reply =
        NodeServer.node_unpublish_volume(
          %NodeUnpublishVolumeRequest{volume_id: "vol-up", target_path: target},
          nil
        )

      assert %_{} = reply
      assert_received {:bind_unmount, ^target}
      assert :ets.lookup(:csi_node_published, {"vol-up", target}) == []
    end

    test "is idempotent for unknown targets", %{staging_root: root} do
      reply =
        NodeServer.node_unpublish_volume(
          %NodeUnpublishVolumeRequest{
            volume_id: "vol-up",
            target_path: Path.join(root, "ghost")
          },
          nil
        )

      assert %_{} = reply
      refute_received {:bind_unmount, _}
    end

    test "allows unstage after unpublish", %{staging: staging, target: target} do
      NodeServer.node_unpublish_volume(
        %NodeUnpublishVolumeRequest{volume_id: "vol-up", target_path: target},
        nil
      )

      reply =
        NodeServer.node_unstage_volume(
          %NodeUnstageVolumeRequest{volume_id: "vol-up", staging_target_path: staging},
          nil
        )

      assert %_{} = reply
      assert_received {:fuse_unmount, _}
    end
  end

  describe "NodeGetVolumeStats" do
    setup %{staging_root: root} do
      staging = Path.join(root, "stage-stats")
      target = Path.join(root, "publish-stats")
      File.mkdir_p!(staging)
      File.mkdir_p!(target)

      NodeServer.node_stage_volume(
        %NodeStageVolumeRequest{
          volume_id: "stats-vol",
          staging_target_path: staging,
          volume_capability: @rw_capability
        },
        nil
      )

      Application.put_env(:neonfs_csi, :core_call_fn, fn
        NeonFS.Core, :get_volume, ["stats-vol"] ->
          {:ok,
           %Volume{
             id: "vid-stats",
             name: "stats-vol",
             durability: %{type: :replicate, factor: 1, min_copies: 1},
             logical_size: 1_024,
             physical_size: 1_024,
             chunk_count: 0,
             created_at: DateTime.from_unix!(0),
             updated_at: DateTime.from_unix!(0)
           }}
      end)

      {:ok, staging: staging, target: target}
    end

    test "rejects empty volume_id" do
      assert_raise GRPC.RPCError, ~r/required/, fn ->
        NodeServer.node_get_volume_stats(
          %NodeGetVolumeStatsRequest{volume_id: "", volume_path: "/p"},
          nil
        )
      end
    end

    test "rejects empty volume_path" do
      assert_raise GRPC.RPCError, ~r/required/, fn ->
        NodeServer.node_get_volume_stats(
          %NodeGetVolumeStatsRequest{volume_id: "stats-vol", volume_path: ""},
          nil
        )
      end
    end

    test "reports usage and abnormal=false for a healthy mount", %{target: target} do
      reply =
        NodeServer.node_get_volume_stats(
          %NodeGetVolumeStatsRequest{volume_id: "stats-vol", volume_path: target},
          nil
        )

      assert reply.volume_condition.abnormal == false

      # Uncapped volume: both dimensions report total = used (available 0).
      assert [
               %Csi.V1.VolumeUsage{used: 1_024, total: 1_024, available: 0, unit: :BYTES},
               %Csi.V1.VolumeUsage{used: 0, total: 0, available: 0, unit: :INODES}
             ] = reply.usage
    end

    test "reports the quota as total for a capped volume", %{target: target} do
      Application.put_env(:neonfs_csi, :core_call_fn, fn
        NeonFS.Core, :get_volume, ["stats-vol"] ->
          {:ok,
           %Volume{
             id: "vid-stats",
             name: "stats-vol",
             durability: %{type: :replicate, factor: 1, min_copies: 1},
             logical_size: 400,
             max_size: 1_000,
             file_count: 3,
             max_files: 10,
             physical_size: 400,
             chunk_count: 0,
             created_at: DateTime.from_unix!(0),
             updated_at: DateTime.from_unix!(0)
           }}
      end)

      reply =
        NodeServer.node_get_volume_stats(
          %NodeGetVolumeStatsRequest{volume_id: "stats-vol", volume_path: target},
          nil
        )

      assert [
               %Csi.V1.VolumeUsage{used: 400, total: 1_000, available: 600, unit: :BYTES},
               %Csi.V1.VolumeUsage{used: 3, total: 10, available: 7, unit: :INODES}
             ] = reply.usage
    end

    test "reports abnormal=true when the probe path doesn't exist", %{target: target} do
      missing = Path.join(target, "definitely-not-here")

      reply =
        NodeServer.node_get_volume_stats(
          %NodeGetVolumeStatsRequest{volume_id: "stats-vol", volume_path: missing},
          nil
        )

      assert reply.volume_condition.abnormal == true
      assert reply.volume_condition.message =~ "FUSE mount probe"
    end
  end

  describe "block volume lifecycle" do
    setup %{staging_root: root} do
      staging = Path.join(root, "blk-staging")

      NodeServer.node_stage_volume(
        %NodeStageVolumeRequest{
          volume_id: "vol-blk",
          staging_target_path: staging,
          volume_capability: @block_capability,
          publish_context: %{"neonfs.attached_node" => "worker-a"}
        },
        nil
      )

      {:ok, staging: staging, target: Path.join(root, "pods/vol-blk/dev")}
    end

    # The kubelet's target path for a block volume is a file, not a
    # directory: the device node is bind-mounted onto it, and creating a
    # directory there makes the bind fail with a type mismatch.
    test "publishes the device onto a file target", %{staging: staging, target: target} do
      assert %_{} =
               NodeServer.node_publish_volume(
                 %NodePublishVolumeRequest{
                   volume_id: "vol-blk",
                   staging_target_path: staging,
                   target_path: target,
                   volume_capability: @block_capability,
                   readonly: false
                 },
                 nil
               )

      assert_received {:bind_mount, "/dev/nbd0", ^target, false}
      assert File.regular?(target)
    end

    test "publishes read-only when asked", %{staging: staging, target: target} do
      NodeServer.node_publish_volume(
        %NodePublishVolumeRequest{
          volume_id: "vol-blk",
          staging_target_path: staging,
          target_path: target,
          volume_capability: @block_capability,
          readonly: true
        },
        nil
      )

      assert_received {:bind_mount, "/dev/nbd0", ^target, true}
    end

    test "unpublish then unstage return it to nothing", %{staging: staging, target: target} do
      NodeServer.node_publish_volume(
        %NodePublishVolumeRequest{
          volume_id: "vol-blk",
          staging_target_path: staging,
          target_path: target,
          volume_capability: @block_capability,
          readonly: false
        },
        nil
      )

      assert %_{} =
               NodeServer.node_unpublish_volume(
                 %NodeUnpublishVolumeRequest{volume_id: "vol-blk", target_path: target},
                 nil
               )

      assert_received {:bind_unmount, ^target}

      assert %_{} =
               NodeServer.node_unstage_volume(
                 %NodeUnstageVolumeRequest{
                   volume_id: "vol-blk",
                   staging_target_path: staging
                 },
                 nil
               )

      assert_received {:block_detach, "/dev/nbd0"}
    end

    # A kubelet retry must not wedge on a teardown that already happened.
    test "both reversals are safe to call twice", %{staging: staging, target: target} do
      NodeServer.node_publish_volume(
        %NodePublishVolumeRequest{
          volume_id: "vol-blk",
          staging_target_path: staging,
          target_path: target,
          volume_capability: @block_capability,
          readonly: false
        },
        nil
      )

      unpublish = %NodeUnpublishVolumeRequest{volume_id: "vol-blk", target_path: target}
      assert %_{} = NodeServer.node_unpublish_volume(unpublish, nil)
      assert %_{} = NodeServer.node_unpublish_volume(unpublish, nil)

      unstage = %NodeUnstageVolumeRequest{
        volume_id: "vol-blk",
        staging_target_path: staging
      }

      assert %_{} = NodeServer.node_unstage_volume(unstage, nil)
      assert %_{} = NodeServer.node_unstage_volume(unstage, nil)
    end

    # `nbd-client -d` on a device nothing is bound to is the state the call
    # is asking for, so it must not fail the unstage.
    test "unstage succeeds when the device is already detached", %{staging: staging} do
      Application.put_env(:neonfs_csi, :block_detach_fn, fn _device ->
        {:error, :not_attached}
      end)

      assert %_{} =
               NodeServer.node_unstage_volume(
                 %NodeUnstageVolumeRequest{
                   volume_id: "vol-blk",
                   staging_target_path: staging
                 },
                 nil
               )
    end

    test "refuses a block target that is already a directory", %{
      staging: staging,
      target: target
    } do
      File.mkdir_p!(target)

      assert_raise GRPC.RPCError, ~r/must be a file/, fn ->
        NodeServer.node_publish_volume(
          %NodePublishVolumeRequest{
            volume_id: "vol-blk",
            staging_target_path: staging,
            target_path: target,
            volume_capability: @block_capability,
            readonly: false
          },
          nil
        )
      end
    end
  end

  describe "capability validation against the volume" do
    setup do
      Application.put_env(:neonfs_csi, :core_call_fn, fn
        NeonFS.Core, :get_volume, ["vol-blk"] ->
          {:ok, %Volume{id: "vol-blk", name: "vol-blk", type: :block, max_size: 4096}}

        NeonFS.Core, :get_volume, [_other] ->
          {:ok, %Volume{id: "vol-fs", name: "vol-fs", type: :fs}}
      end)

      :ok
    end

    test "refuses a mount capability against a block volume", %{staging_root: root} do
      assert_raise GRPC.RPCError, ~r/is a block volume/, fn ->
        NodeServer.node_stage_volume(
          %NodeStageVolumeRequest{
            volume_id: "vol-blk",
            staging_target_path: Path.join(root, "s"),
            volume_capability: @rw_capability
          },
          nil
        )
      end
    end

    test "refuses a block capability against a filesystem volume", %{staging_root: root} do
      assert_raise GRPC.RPCError, ~r/is a mount volume/, fn ->
        NodeServer.node_stage_volume(
          %NodeStageVolumeRequest{
            volume_id: "vol-fs",
            staging_target_path: Path.join(root, "s"),
            volume_capability: @block_capability
          },
          nil
        )
      end
    end

    # Publish compares against what was staged rather than asking core
    # again — the staged record is the authority on what is attached.
    test "refuses a publish whose capability contradicts the staging", %{staging_root: root} do
      staging = Path.join(root, "blk")

      NodeServer.node_stage_volume(
        %NodeStageVolumeRequest{
          volume_id: "vol-blk",
          staging_target_path: staging,
          volume_capability: @block_capability,
          publish_context: %{"neonfs.attached_node" => "worker-a"}
        },
        nil
      )

      assert_raise GRPC.RPCError, ~r/is a block volume/, fn ->
        NodeServer.node_publish_volume(
          %NodePublishVolumeRequest{
            volume_id: "vol-blk",
            staging_target_path: staging,
            target_path: Path.join(root, "t"),
            volume_capability: @rw_capability,
            readonly: false
          },
          nil
        )
      end
    end
  end

  describe "NodeGetVolumeStats for a block volume" do
    setup do
      Application.put_env(:neonfs_csi, :core_call_fn, fn NeonFS.Core, :get_volume, _ ->
        {:ok, %Volume{id: "vol-blk", name: "vol-blk", type: :block, max_size: 8_388_608}}
      end)

      :ok
    end

    # A raw device has no filesystem, so no inodes and no free space of its
    # own. Inventing those numbers is worse than the spec's answer of total
    # bytes alone.
    test "reports the device size and no inode statistics", %{staging_root: root} do
      reply =
        NodeServer.node_get_volume_stats(
          %NodeGetVolumeStatsRequest{
            volume_id: "vol-blk",
            volume_path: Path.join(root, "blk")
          },
          nil
        )

      assert [%{unit: :BYTES, total: 8_388_608, used: 8_388_608, available: 0}] = reply.usage
    end
  end
end
