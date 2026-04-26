defmodule NeonFS.CSI.NodeServerTest do
  use ExUnit.Case, async: false

  alias Csi.V1.{
    NodeGetCapabilitiesRequest,
    NodeGetInfoRequest,
    NodePublishVolumeRequest,
    NodeServiceCapability,
    NodeStageVolumeRequest,
    NodeUnpublishVolumeRequest,
    NodeUnstageVolumeRequest,
    VolumeCapability
  }

  alias NeonFS.CSI.NodeServer

  @rw_capability %VolumeCapability{
    access_mode: %VolumeCapability.AccessMode{mode: :SINGLE_NODE_WRITER}
  }

  @ro_capability %VolumeCapability{
    access_mode: %VolumeCapability.AccessMode{mode: :MULTI_NODE_READER_ONLY}
  }

  setup do
    NodeServer.reset_state_tables()

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

    on_exit(fn ->
      Application.delete_env(:neonfs_csi, :fuse_mount_fn)
      Application.delete_env(:neonfs_csi, :fuse_unmount_fn)
      Application.delete_env(:neonfs_csi, :bind_mount_fn)
      Application.delete_env(:neonfs_csi, :bind_unmount_fn)
      Application.delete_env(:neonfs_csi, :node_id)
      NodeServer.reset_state_tables()
    end)

    staging_root =
      Path.join(System.tmp_dir!(), "csi_node_test_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(staging_root) end)

    {:ok, staging_root: staging_root}
  end

  describe "NodeGetCapabilities" do
    test "advertises STAGE_UNSTAGE_VOLUME" do
      reply = NodeServer.node_get_capabilities(%NodeGetCapabilitiesRequest{}, nil)

      types =
        Enum.map(reply.capabilities, fn %NodeServiceCapability{type: {:rpc, rpc}} -> rpc.type end)

      assert :STAGE_UNSTAGE_VOLUME in types
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

    test "rejects block volumes", %{staging_root: root} do
      block_cap = %VolumeCapability{access_type: {:block, %VolumeCapability.BlockVolume{}}}

      assert_raise GRPC.RPCError, ~r/block volumes are not supported/, fn ->
        NodeServer.node_stage_volume(
          %NodeStageVolumeRequest{
            volume_id: "vol-block",
            staging_target_path: Path.join(root, "block"),
            volume_capability: block_cap
          },
          nil
        )
      end
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
end
