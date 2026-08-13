defmodule NeonFS.CSI.AttachLifecycleTest do
  @moduledoc """
  `ControllerPublishVolume` / `ControllerUnpublishVolume` for block
  volumes: one node at a time, enforced by an exclusive coordinator claim
  rather than by the CO's own bookkeeping.
  """
  use ExUnit.Case, async: false

  alias Csi.V1.{
    ControllerPublishVolumeRequest,
    ControllerUnpublishVolumeRequest,
    VolumeCapability
  }

  alias NeonFS.Client.ServiceInfo
  alias NeonFS.Core.Volume
  alias NeonFS.CSI.{AttachHolder, AttachRegistry, ControllerServer}

  @block_capability %VolumeCapability{
    access_type: {:block, %VolumeCapability.BlockVolume{}},
    access_mode: %VolumeCapability.AccessMode{mode: :SINGLE_NODE_WRITER}
  }

  @mount_capability %VolumeCapability{
    access_type: {:mount, %VolumeCapability.MountVolume{}},
    access_mode: %VolumeCapability.AccessMode{mode: :SINGLE_NODE_WRITER}
  }

  setup do
    AttachRegistry.reset()
    start_supervised!(AttachHolder)
    test_pid = self()

    Application.put_env(:neonfs_csi, :core_call_fn, fn
      NeonFS.Core, :get_volume, ["blk"] ->
        {:ok, %Volume{id: "blk", name: "blk", type: :block, max_size: 4096}}

      NeonFS.Core, :get_volume, ["fs"] ->
        {:ok, %Volume{id: "fs", name: "fs", type: :fs}}

      NeonFS.Core, :get_volume, [_unknown] ->
        {:error, :not_found}
    end)

    # Both worker ids resolve to this BEAM node, which is where the holder
    # is running — the claim itself is what distinguishes them.
    Application.put_env(:neonfs_csi, :service_list_fn, fn :csi ->
      [
        %ServiceInfo{node: node(), type: :csi, metadata: %{mode: :node, node_id: "worker-a"}},
        %ServiceInfo{node: node(), type: :csi, metadata: %{mode: :node, node_id: "worker-b"}}
      ]
    end)

    claims = :ets.new(:claims, [:set, :public])

    Application.put_env(:neonfs_csi, :coordinator_call_fn, fn
      :claim_path_for, [path, :exclusive, holder] ->
        send(test_pid, {:claim, path, holder})

        case :ets.lookup(claims, path) do
          [] ->
            claim_id = "claim-#{:erlang.unique_integer([:positive])}"
            :ets.insert(claims, {path, claim_id})
            {:ok, claim_id}

          [_held] ->
            {:error, %NeonFS.Error.Conflict{}}
        end

      :release, [claim_id] ->
        send(test_pid, {:release, claim_id})
        :ets.match_delete(claims, {:_, claim_id})
        :ok
    end)

    on_exit(fn ->
      Application.delete_env(:neonfs_csi, :core_call_fn)
      Application.delete_env(:neonfs_csi, :service_list_fn)
      Application.delete_env(:neonfs_csi, :coordinator_call_fn)
      AttachRegistry.reset()
    end)

    :ok
  end

  defp publish(volume_id, node_id, capability \\ @block_capability) do
    ControllerServer.controller_publish_volume(
      %ControllerPublishVolumeRequest{
        volume_id: volume_id,
        node_id: node_id,
        volume_capability: capability
      },
      nil
    )
  end

  defp unpublish(volume_id, node_id) do
    ControllerServer.controller_unpublish_volume(
      %ControllerUnpublishVolumeRequest{volume_id: volume_id, node_id: node_id},
      nil
    )
  end

  describe "ControllerPublishVolume" do
    test "attaches a block volume and names the node in the publish context" do
      reply = publish("blk", "worker-a")

      assert reply.publish_context["neonfs.attached_node"] == "worker-a"
      assert_received {:claim, "csi:attach:blk", holder}
      assert holder == Process.whereis(AttachHolder)
    end

    # The whole point: a second node cannot take a device the first still
    # holds, whatever the CO believes.
    test "refuses a second node while the first attachment is live" do
      publish("blk", "worker-a")

      assert_raise GRPC.RPCError, ~r/is attached to worker-a/, fn ->
        publish("blk", "worker-b")
      end
    end

    # A CO retry must not fail on its own previous success.
    test "re-attaching to the same node succeeds" do
      publish("blk", "worker-a")

      assert %_{} = publish("blk", "worker-a")
    end

    test "a filesystem volume attaches without taking a claim" do
      assert %_{} = publish("fs", "worker-a", @mount_capability)

      refute_received {:claim, _path, _holder}
    end

    test "refuses a capability that does not match the volume" do
      assert_raise GRPC.RPCError, ~r/does not match/, fn ->
        publish("blk", "worker-a", @mount_capability)
      end
    end

    # csi-sanity checks this directly: attaching to a node the driver has
    # never heard of describes a topology that does not exist, and recording
    # an attachment for it would be recording something nothing can act on.
    test "refuses a node that is not known" do
      assert_raise GRPC.RPCError, ~r/node worker-ghost is not known/, fn ->
        publish("blk", "worker-ghost")
      end
    end

    test "accepts the node this plugin is itself serving" do
      Application.put_env(:neonfs_csi, :service_list_fn, fn :csi -> [] end)
      Application.put_env(:neonfs_csi, :node_id, "self-reported")
      on_exit(fn -> Application.delete_env(:neonfs_csi, :node_id) end)

      assert %_{} = publish("fs", "self-reported", @mount_capability)
    end

    test "refuses a volume that does not exist" do
      assert_raise GRPC.RPCError, ~r/not found/, fn ->
        publish("ghost", "worker-a")
      end
    end

    test "requires a volume_id, a node_id and a capability" do
      assert_raise GRPC.RPCError, ~r/volume_id is required/, fn ->
        ControllerServer.controller_publish_volume(
          %ControllerPublishVolumeRequest{volume_id: ""},
          nil
        )
      end

      assert_raise GRPC.RPCError, ~r/node_id is required/, fn ->
        ControllerServer.controller_publish_volume(
          %ControllerPublishVolumeRequest{volume_id: "blk", node_id: ""},
          nil
        )
      end

      assert_raise GRPC.RPCError, ~r/volume_capability is required/, fn ->
        ControllerServer.controller_publish_volume(
          %ControllerPublishVolumeRequest{
            volume_id: "blk",
            node_id: "worker-a",
            volume_capability: nil
          },
          nil
        )
      end
    end
  end

  describe "ControllerUnpublishVolume" do
    test "releases the claim so another node can attach" do
      publish("blk", "worker-a")
      assert %_{} = unpublish("blk", "worker-a")
      assert_received {:release, _claim_id}

      assert %_{} = publish("blk", "worker-b")
    end

    # A detach that can fail is a volume no kubelet can move on from.
    test "is idempotent for an attachment that was never taken" do
      assert %_{} = unpublish("blk", "worker-a")
    end

    test "is idempotent when called twice" do
      publish("blk", "worker-a")

      assert %_{} = unpublish("blk", "worker-a")
      assert %_{} = unpublish("blk", "worker-a")
    end

    test "requires a volume_id" do
      assert_raise GRPC.RPCError, ~r/volume_id is required/, fn ->
        ControllerServer.controller_unpublish_volume(
          %ControllerUnpublishVolumeRequest{volume_id: ""},
          nil
        )
      end
    end
  end
end
