defmodule NeonFS.CSI.ControllerServerTest do
  use ExUnit.Case, async: false

  import Bitwise, only: [<<<: 2]

  alias Csi.V1.{
    CapacityRange,
    ControllerGetCapabilitiesRequest,
    ControllerPublishVolumeRequest,
    ControllerServiceCapability,
    ControllerUnpublishVolumeRequest,
    CreateVolumeRequest,
    DeleteVolumeRequest,
    GetCapacityRequest,
    ListVolumesRequest,
    ValidateVolumeCapabilitiesRequest,
    VolumeCapability
  }

  alias NeonFS.Core.Volume
  alias NeonFS.CSI.ControllerServer

  setup do
    ControllerServer.reset_publish_table()

    on_exit(fn ->
      Application.delete_env(:neonfs_csi, :core_call_fn)
      ControllerServer.reset_publish_table()
    end)

    :ok
  end

  defp put_core(handler) do
    Application.put_env(:neonfs_csi, :core_call_fn, handler)
  end

  defp sample_volume(name, overrides \\ %{}) do
    base = %Volume{
      id: "vol-id-" <> name,
      name: name,
      owner: nil,
      durability: nil,
      write_ack: nil,
      tiering: nil,
      caching: nil,
      io_weight: nil,
      compression: nil,
      verification: nil,
      encryption: nil,
      metadata_consistency: nil,
      logical_size: 0,
      physical_size: 0,
      chunk_count: 0,
      created_at: DateTime.from_unix!(0),
      updated_at: DateTime.from_unix!(0)
    }

    Map.merge(base, overrides)
  end

  describe "ControllerGetCapabilities" do
    test "advertises CREATE_DELETE_VOLUME, PUBLISH_UNPUBLISH_VOLUME, LIST_VOLUMES, GET_CAPACITY" do
      reply =
        ControllerServer.controller_get_capabilities(%ControllerGetCapabilitiesRequest{}, nil)

      types =
        Enum.map(reply.capabilities, fn %ControllerServiceCapability{type: {:rpc, rpc}} ->
          rpc.type
        end)

      assert :CREATE_DELETE_VOLUME in types
      assert :PUBLISH_UNPUBLISH_VOLUME in types
      assert :LIST_VOLUMES in types
      assert :GET_CAPACITY in types
    end
  end

  describe "CreateVolume" do
    test "creates a NeonFS volume from the CSI request" do
      put_core(fn NeonFS.Core, :create_volume, [name, opts] ->
        assert name == "pvc-1"
        assert opts == [replication_factor: 3]
        {:ok, sample_volume("pvc-1")}
      end)

      req = %CreateVolumeRequest{
        name: "pvc-1",
        capacity_range: %CapacityRange{required_bytes: 1024, limit_bytes: 0},
        parameters: %{"replication_factor" => "3"}
      }

      reply = ControllerServer.create_volume(req, nil)
      assert reply.volume.volume_id == "pvc-1"
      assert reply.volume.capacity_bytes == 1024
    end

    test "returns the existing volume when create reports :exists (idempotent)" do
      put_core(fn
        NeonFS.Core, :create_volume, _ -> {:error, :exists}
        NeonFS.Core, :get_volume, ["pvc-2"] -> {:ok, sample_volume("pvc-2")}
      end)

      reply =
        ControllerServer.create_volume(%CreateVolumeRequest{name: "pvc-2"}, nil)

      assert reply.volume.volume_id == "pvc-2"
    end

    test "raises invalid_argument when name is missing" do
      assert_raise GRPC.RPCError, ~r/name is required/, fn ->
        ControllerServer.create_volume(%CreateVolumeRequest{name: ""}, nil)
      end
    end

    test "drops unknown parameters silently" do
      put_core(fn NeonFS.Core, :create_volume, [_, opts] ->
        # `replication_factor` is honoured, `garbage` is dropped.
        assert opts == [replication_factor: 2]
        {:ok, sample_volume("pvc-3")}
      end)

      req = %CreateVolumeRequest{
        name: "pvc-3",
        parameters: %{"replication_factor" => "2", "garbage" => "value"}
      }

      ControllerServer.create_volume(req, nil)
    end
  end

  describe "DeleteVolume" do
    test "delegates to NeonFS.Core.delete_volume/1" do
      test_pid = self()

      put_core(fn NeonFS.Core, :delete_volume, [name] ->
        send(test_pid, {:deleted, name})
        :ok
      end)

      ControllerServer.delete_volume(%DeleteVolumeRequest{volume_id: "pvc-x"}, nil)
      assert_received {:deleted, "pvc-x"}
    end

    test "is idempotent on :not_found" do
      put_core(fn NeonFS.Core, :delete_volume, _ -> {:error, :not_found} end)

      assert %_{} =
               ControllerServer.delete_volume(%DeleteVolumeRequest{volume_id: "ghost"}, nil)
    end

    test "raises invalid_argument with empty volume_id" do
      assert_raise GRPC.RPCError, ~r/volume_id is required/, fn ->
        ControllerServer.delete_volume(%DeleteVolumeRequest{volume_id: ""}, nil)
      end
    end

    test "clears the publish table on successful delete" do
      put_core(fn NeonFS.Core, :delete_volume, _ -> :ok end)

      # Stage publish entries for two nodes.
      Application.put_env(:neonfs_csi, :core_call_fn, fn
        NeonFS.Core, :get_volume, _ -> {:ok, sample_volume("pvc-y")}
        NeonFS.Core, :delete_volume, _ -> :ok
      end)

      ControllerServer.controller_publish_volume(
        %ControllerPublishVolumeRequest{volume_id: "pvc-y", node_id: "node-1"},
        nil
      )

      ControllerServer.controller_publish_volume(
        %ControllerPublishVolumeRequest{volume_id: "pvc-y", node_id: "node-2"},
        nil
      )

      assert :ets.match(:csi_published_volumes, {{"pvc-y", :_}, :_}) |> length() == 2

      ControllerServer.delete_volume(%DeleteVolumeRequest{volume_id: "pvc-y"}, nil)

      assert :ets.match(:csi_published_volumes, {{"pvc-y", :_}, :_}) == []
    end
  end

  describe "ValidateVolumeCapabilities" do
    setup do
      put_core(fn NeonFS.Core, :get_volume, _ -> {:ok, sample_volume("pvc-vc")} end)
      :ok
    end

    test "confirms supported access modes" do
      caps = [
        %VolumeCapability{
          access_mode: %VolumeCapability.AccessMode{mode: :SINGLE_NODE_WRITER}
        }
      ]

      reply =
        ControllerServer.validate_volume_capabilities(
          %ValidateVolumeCapabilitiesRequest{volume_id: "pvc-vc", volume_capabilities: caps},
          nil
        )

      assert reply.confirmed
      assert reply.confirmed.volume_capabilities == caps
    end

    test "refuses block volumes" do
      caps = [%VolumeCapability{access_type: {:block, %VolumeCapability.BlockVolume{}}}]

      reply =
        ControllerServer.validate_volume_capabilities(
          %ValidateVolumeCapabilitiesRequest{volume_id: "pvc-vc", volume_capabilities: caps},
          nil
        )

      assert reply.confirmed == nil
      assert reply.message =~ "not supported"
    end

    test "raises not_found for an unknown volume" do
      put_core(fn NeonFS.Core, :get_volume, _ -> {:error, :not_found} end)

      assert_raise GRPC.RPCError, ~r/not found/, fn ->
        ControllerServer.validate_volume_capabilities(
          %ValidateVolumeCapabilitiesRequest{volume_id: "ghost", volume_capabilities: []},
          nil
        )
      end
    end
  end

  describe "ListVolumes" do
    test "returns all volumes when no pagination is requested" do
      put_core(fn NeonFS.Core, :list_volumes, [] ->
        {:ok, [sample_volume("a"), sample_volume("b"), sample_volume("c")]}
      end)

      reply = ControllerServer.list_volumes(%ListVolumesRequest{}, nil)
      assert length(reply.entries) == 3
      assert reply.next_token == ""
    end

    test "honours max_entries and emits a next_token when more remain" do
      put_core(fn NeonFS.Core, :list_volumes, [] ->
        {:ok, [sample_volume("a"), sample_volume("b"), sample_volume("c")]}
      end)

      reply = ControllerServer.list_volumes(%ListVolumesRequest{max_entries: 2}, nil)

      assert Enum.map(reply.entries, & &1.volume.volume_id) == ["a", "b"]
      assert reply.next_token == "b"
    end

    test "resumes from a starting_token" do
      put_core(fn NeonFS.Core, :list_volumes, [] ->
        {:ok, [sample_volume("a"), sample_volume("b"), sample_volume("c")]}
      end)

      reply =
        ControllerServer.list_volumes(
          %ListVolumesRequest{starting_token: "b", max_entries: 5},
          nil
        )

      assert Enum.map(reply.entries, & &1.volume.volume_id) == ["c"]
      assert reply.next_token == ""
    end
  end

  describe "GetCapacity" do
    test "reports the cluster's total_available bytes" do
      put_core(fn NeonFS.Core.StorageMetrics, :cluster_capacity, [] ->
        %{total_capacity: 10_000, total_used: 1_000, total_available: 9_000, drives: []}
      end)

      reply = ControllerServer.get_capacity(%GetCapacityRequest{}, nil)
      assert reply.available_capacity == 9_000
    end

    test "saturates at 2^62 when the cluster is :unlimited" do
      put_core(fn NeonFS.Core.StorageMetrics, :cluster_capacity, [] ->
        %{total_capacity: :unlimited, total_used: 0, total_available: :unlimited, drives: []}
      end)

      reply = ControllerServer.get_capacity(%GetCapacityRequest{}, nil)
      assert reply.available_capacity == 1 <<< 62
    end
  end

  describe "ControllerPublishVolume" do
    setup do
      put_core(fn NeonFS.Core, :get_volume, _ -> {:ok, sample_volume("pvc-pub")} end)
      :ok
    end

    test "records the publish attachment" do
      ControllerServer.controller_publish_volume(
        %ControllerPublishVolumeRequest{volume_id: "pvc-pub", node_id: "node-1"},
        nil
      )

      assert [{{"pvc-pub", "node-1"}, _}] =
               :ets.lookup(:csi_published_volumes, {"pvc-pub", "node-1"})
    end

    test "echoes a publish_context with the readonly flag" do
      reply =
        ControllerServer.controller_publish_volume(
          %ControllerPublishVolumeRequest{
            volume_id: "pvc-pub",
            node_id: "node-1",
            readonly: true
          },
          nil
        )

      assert reply.publish_context["readonly"] == "true"
    end

    test "raises not_found when the volume doesn't exist" do
      put_core(fn NeonFS.Core, :get_volume, _ -> {:error, :not_found} end)

      assert_raise GRPC.RPCError, ~r/not found/, fn ->
        ControllerServer.controller_publish_volume(
          %ControllerPublishVolumeRequest{volume_id: "ghost", node_id: "node-1"},
          nil
        )
      end
    end
  end

  describe "ControllerUnpublishVolume" do
    test "deletes the publish attachment for a single node" do
      :ets.insert(:csi_published_volumes, {{"pvc", "node-1"}, %{}})
      :ets.insert(:csi_published_volumes, {{"pvc", "node-2"}, %{}})

      ControllerServer.controller_unpublish_volume(
        %ControllerUnpublishVolumeRequest{volume_id: "pvc", node_id: "node-1"},
        nil
      )

      assert :ets.lookup(:csi_published_volumes, {"pvc", "node-1"}) == []
      assert [_] = :ets.lookup(:csi_published_volumes, {"pvc", "node-2"})
    end

    test "empty node_id clears all attachments for the volume" do
      :ets.insert(:csi_published_volumes, {{"pvc", "node-1"}, %{}})
      :ets.insert(:csi_published_volumes, {{"pvc", "node-2"}, %{}})

      ControllerServer.controller_unpublish_volume(
        %ControllerUnpublishVolumeRequest{volume_id: "pvc", node_id: ""},
        nil
      )

      assert :ets.match(:csi_published_volumes, {{"pvc", :_}, :_}) == []
    end

    test "is idempotent on unknown attachments" do
      assert %_{} =
               ControllerServer.controller_unpublish_volume(
                 %ControllerUnpublishVolumeRequest{volume_id: "ghost", node_id: "node-x"},
                 nil
               )
    end
  end
end
