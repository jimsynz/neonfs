defmodule NeonFS.CSI.ControllerServerTest do
  use ExUnit.Case, async: false

  import Bitwise, only: [<<<: 2]

  alias Csi.V1.{
    CapacityRange,
    ControllerGetCapabilitiesRequest,
    ControllerGetVolumeRequest,
    ControllerPublishVolumeRequest,
    ControllerServiceCapability,
    ControllerUnpublishVolumeRequest,
    CreateSnapshotRequest,
    CreateVolumeRequest,
    DeleteSnapshotRequest,
    DeleteVolumeRequest,
    GetCapacityRequest,
    ListSnapshotsRequest,
    ListVolumesRequest,
    ValidateVolumeCapabilitiesRequest,
    VolumeCapability,
    VolumeContentSource
  }

  alias NeonFS.Core.Volume
  alias NeonFS.CSI.{ControllerServer, VolumeHealth}

  setup do
    ControllerServer.reset_publish_table()
    VolumeHealth.reset_table()

    on_exit(fn ->
      Application.delete_env(:neonfs_csi, :core_call_fn)
      ControllerServer.reset_publish_table()
      VolumeHealth.reset_table()
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

  describe "ControllerGetCapabilities advertises volume-condition capabilities" do
    test "includes GET_VOLUME and VOLUME_CONDITION" do
      reply =
        ControllerServer.controller_get_capabilities(%ControllerGetCapabilitiesRequest{}, nil)

      types =
        Enum.map(reply.capabilities, fn %ControllerServiceCapability{type: {:rpc, rpc}} ->
          rpc.type
        end)

      assert :GET_VOLUME in types
      assert :VOLUME_CONDITION in types
    end
  end

  describe "ControllerGetVolume" do
    test "returns volume + status with abnormal=false for a healthy cluster" do
      put_core(fn
        NeonFS.Core, :get_volume, ["healthy"] ->
          {:ok,
           sample_volume("healthy", %{durability: %{type: :replicate, factor: 1, min_copies: 1}})}

        NeonFS.Core.StorageMetrics, :cluster_capacity, [] ->
          %{drives: [%{state: :active}], total_capacity: 100, total_used: 10}

        NeonFS.Core.ServiceRegistry, :list_by_type, [:core] ->
          [%{node: :n1}, %{node: :n2}, %{node: :n3}]

        NeonFS.Core.Escalation, :list, [[status: :pending]] ->
          []
      end)

      reply =
        ControllerServer.controller_get_volume(
          %ControllerGetVolumeRequest{volume_id: "healthy"},
          nil
        )

      assert reply.volume.volume_id == "healthy"
      assert reply.status.volume_condition.abnormal == false
      assert reply.status.volume_condition.message == ""
    end

    test "reports abnormal when replication factor exceeds core nodes" do
      put_core(fn
        NeonFS.Core, :get_volume, ["over"] ->
          {:ok,
           sample_volume("over", %{durability: %{type: :replicate, factor: 5, min_copies: 1}})}

        NeonFS.Core.StorageMetrics, :cluster_capacity, [] ->
          %{drives: [%{state: :active}], total_capacity: 100, total_used: 10}

        NeonFS.Core.ServiceRegistry, :list_by_type, [:core] ->
          [%{node: :n1}, %{node: :n2}]

        NeonFS.Core.Escalation, :list, _ ->
          []
      end)

      reply =
        ControllerServer.controller_get_volume(
          %ControllerGetVolumeRequest{volume_id: "over"},
          nil
        )

      assert reply.status.volume_condition.abnormal == true
      assert reply.status.volume_condition.message =~ "replication factor 5"
    end

    test "reports abnormal on a pending critical escalation" do
      put_core(fn
        NeonFS.Core, :get_volume, ["v"] ->
          {:ok, sample_volume("v", %{durability: %{type: :replicate, factor: 1, min_copies: 1}})}

        NeonFS.Core.StorageMetrics, :cluster_capacity, [] ->
          %{drives: [%{state: :active}], total_capacity: 100, total_used: 10}

        NeonFS.Core.ServiceRegistry, :list_by_type, [:core] ->
          [%{node: :n1}]

        NeonFS.Core.Escalation, :list, _ ->
          [%{severity: :critical, category: "drive_failure"}]
      end)

      reply =
        ControllerServer.controller_get_volume(%ControllerGetVolumeRequest{volume_id: "v"}, nil)

      assert reply.status.volume_condition.abnormal == true
      assert reply.status.volume_condition.message =~ "critical escalations"
    end

    test "returns published_node_ids from the publish table" do
      :ets.insert(:csi_published_volumes, {{"pvc", "node-a"}, %{}})
      :ets.insert(:csi_published_volumes, {{"pvc", "node-b"}, %{}})

      put_core(fn
        NeonFS.Core, :get_volume, ["pvc"] ->
          {:ok,
           sample_volume("pvc", %{durability: %{type: :replicate, factor: 1, min_copies: 1}})}

        NeonFS.Core.StorageMetrics, :cluster_capacity, [] ->
          %{drives: [%{state: :active}], total_capacity: 100, total_used: 10}

        NeonFS.Core.ServiceRegistry, :list_by_type, [:core] ->
          [%{node: :n1}]

        NeonFS.Core.Escalation, :list, _ ->
          []
      end)

      reply =
        ControllerServer.controller_get_volume(%ControllerGetVolumeRequest{volume_id: "pvc"}, nil)

      assert Enum.sort(reply.status.published_node_ids) == ["node-a", "node-b"]
    end

    test "raises NOT_FOUND when the volume is gone" do
      put_core(fn NeonFS.Core, :get_volume, _ -> {:error, :not_found} end)

      assert_raise GRPC.RPCError, ~r/not found/, fn ->
        ControllerServer.controller_get_volume(
          %ControllerGetVolumeRequest{volume_id: "ghost"},
          nil
        )
      end
    end

    test "rejects empty volume_id with INVALID_ARGUMENT" do
      assert_raise GRPC.RPCError, ~r/required/, fn ->
        ControllerServer.controller_get_volume(%ControllerGetVolumeRequest{volume_id: ""}, nil)
      end
    end
  end

  describe "ControllerGetCapabilities — snapshot capabilities (#967)" do
    test "advertises CREATE_DELETE_SNAPSHOT, LIST_SNAPSHOTS, and CLONE_VOLUME" do
      reply =
        ControllerServer.controller_get_capabilities(%ControllerGetCapabilitiesRequest{}, nil)

      types =
        Enum.map(reply.capabilities, fn %ControllerServiceCapability{type: {:rpc, rpc}} ->
          rpc.type
        end)

      assert :CREATE_DELETE_SNAPSHOT in types
      assert :LIST_SNAPSHOTS in types
      assert :CLONE_VOLUME in types
    end
  end

  describe "CreateSnapshot" do
    test "resolves the source volume by name and calls Snapshot.create/2" do
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_volume, ["my-vol"] ->
          {:ok, sample_volume("my-vol", %{id: "vol-uuid-1"})}

        NeonFS.Core.Snapshot, :create, ["vol-uuid-1", [name: "daily-snap"]] ->
          send(test_pid, :create_called)

          {:ok,
           %{
             id: "snap-uuid-1",
             volume_id: "vol-uuid-1",
             name: "daily-snap",
             root_chunk_hash: <<1>>,
             created_at: DateTime.from_unix!(1_700_000_000)
           }}
      end)

      reply =
        ControllerServer.create_snapshot(
          %CreateSnapshotRequest{source_volume_id: "my-vol", name: "daily-snap"},
          nil
        )

      assert_received :create_called
      assert reply.snapshot.snapshot_id == "my-vol/snap-uuid-1"
      assert reply.snapshot.source_volume_id == "my-vol"
      assert reply.snapshot.ready_to_use == true
      assert reply.snapshot.creation_time.seconds == 1_700_000_000
    end

    test "raises NOT_FOUND when the source volume is unknown" do
      put_core(fn NeonFS.Core, :get_volume, _ -> {:error, :not_found} end)

      assert_raise GRPC.RPCError, ~r/not found/, fn ->
        ControllerServer.create_snapshot(
          %CreateSnapshotRequest{source_volume_id: "ghost", name: "s"},
          nil
        )
      end
    end

    test "raises INVALID_ARGUMENT with empty source_volume_id" do
      assert_raise GRPC.RPCError, ~r/source_volume_id is required/, fn ->
        ControllerServer.create_snapshot(
          %CreateSnapshotRequest{source_volume_id: "", name: "s"},
          nil
        )
      end
    end

    test "raises INVALID_ARGUMENT with empty name" do
      assert_raise GRPC.RPCError, ~r/name is required/, fn ->
        ControllerServer.create_snapshot(
          %CreateSnapshotRequest{source_volume_id: "vol", name: ""},
          nil
        )
      end
    end
  end

  describe "DeleteSnapshot" do
    test "decodes the CSI snapshot id and calls Snapshot.delete/2" do
      test_pid = self()

      put_core(fn
        NeonFS.Core, :get_volume, ["my-vol"] ->
          {:ok, sample_volume("my-vol", %{id: "vol-uuid"})}

        NeonFS.Core.Snapshot, :delete, ["vol-uuid", "snap-uuid"] ->
          send(test_pid, :deleted)
          :ok
      end)

      assert %_{} =
               ControllerServer.delete_snapshot(
                 %DeleteSnapshotRequest{snapshot_id: "my-vol/snap-uuid"},
                 nil
               )

      assert_received :deleted
    end

    test "returns success for a malformed snapshot id (idempotent)" do
      # No core_call_fn → would crash if we tried to call core.
      assert %_{} =
               ControllerServer.delete_snapshot(
                 %DeleteSnapshotRequest{snapshot_id: "no-slash-here"},
                 nil
               )
    end

    test "returns success when the source volume is already gone" do
      put_core(fn NeonFS.Core, :get_volume, _ -> {:error, :not_found} end)

      assert %_{} =
               ControllerServer.delete_snapshot(
                 %DeleteSnapshotRequest{snapshot_id: "vanished/snap-uuid"},
                 nil
               )
    end

    test "raises INVALID_ARGUMENT with empty snapshot_id" do
      assert_raise GRPC.RPCError, ~r/snapshot_id is required/, fn ->
        ControllerServer.delete_snapshot(%DeleteSnapshotRequest{snapshot_id: ""}, nil)
      end
    end
  end

  describe "ListSnapshots" do
    defp sample_snap(id, name \\ nil, opts \\ %{}) do
      %{
        id: id,
        volume_id: Map.get(opts, :volume_id, "vol-uuid"),
        name: name,
        root_chunk_hash: <<1>>,
        created_at: Map.get(opts, :created_at, DateTime.from_unix!(0))
      }
    end

    test "scopes to source_volume_id" do
      put_core(fn
        NeonFS.Core, :get_volume, ["my-vol"] ->
          {:ok, sample_volume("my-vol", %{id: "vol-uuid"})}

        NeonFS.Core.Snapshot, :list, ["vol-uuid"] ->
          {:ok, [sample_snap("s1"), sample_snap("s2")]}
      end)

      reply =
        ControllerServer.list_snapshots(
          %ListSnapshotsRequest{source_volume_id: "my-vol"},
          nil
        )

      ids = Enum.map(reply.entries, & &1.snapshot.snapshot_id)
      assert "my-vol/s1" in ids
      assert "my-vol/s2" in ids
      assert reply.next_token == ""
    end

    test "filters by snapshot_id (single-result lookup)" do
      put_core(fn
        NeonFS.Core, :get_volume, ["my-vol"] ->
          {:ok, sample_volume("my-vol", %{id: "vol-uuid"})}

        NeonFS.Core.Snapshot, :get, ["vol-uuid", "snap-s1"] ->
          {:ok, sample_snap("snap-s1")}
      end)

      reply =
        ControllerServer.list_snapshots(
          %ListSnapshotsRequest{snapshot_id: "my-vol/snap-s1"},
          nil
        )

      assert length(reply.entries) == 1
      assert hd(reply.entries).snapshot.snapshot_id == "my-vol/snap-s1"
    end

    test "returns empty when filtering by an unknown source_volume" do
      put_core(fn NeonFS.Core, :get_volume, _ -> {:error, :not_found} end)

      reply =
        ControllerServer.list_snapshots(
          %ListSnapshotsRequest{source_volume_id: "ghost"},
          nil
        )

      assert reply.entries == []
    end

    test "unfiltered list walks every volume" do
      put_core(fn
        NeonFS.Core, :list_volumes, [] ->
          {:ok,
           [
             sample_volume("vol-a", %{id: "ida"}),
             sample_volume("vol-b", %{id: "idb"})
           ]}

        NeonFS.Core.Snapshot, :list, ["ida"] ->
          {:ok, [sample_snap("a1", nil, %{volume_id: "ida"})]}

        NeonFS.Core.Snapshot, :list, ["idb"] ->
          {:ok, [sample_snap("b1", nil, %{volume_id: "idb"})]}
      end)

      reply = ControllerServer.list_snapshots(%ListSnapshotsRequest{}, nil)

      ids = Enum.map(reply.entries, & &1.snapshot.snapshot_id) |> Enum.sort()
      assert ids == ["vol-a/a1", "vol-b/b1"]
    end

    test "honours max_entries pagination" do
      put_core(fn
        NeonFS.Core, :get_volume, ["v"] ->
          {:ok, sample_volume("v", %{id: "vid"})}

        NeonFS.Core.Snapshot, :list, ["vid"] ->
          {:ok, [sample_snap("a"), sample_snap("b"), sample_snap("c")]}
      end)

      reply =
        ControllerServer.list_snapshots(
          %ListSnapshotsRequest{source_volume_id: "v", max_entries: 2},
          nil
        )

      assert length(reply.entries) == 2
      assert reply.next_token != ""
    end
  end

  describe "CreateVolume from VolumeContentSource (#967)" do
    test "promotes a snapshot into a new volume when source is a snapshot" do
      put_core(fn
        NeonFS.Core, :get_volume, ["src-vol"] ->
          {:ok, sample_volume("src-vol", %{id: "src-id"})}

        NeonFS.Core.Snapshot, :promote, ["src-id", "snap-id", "new-vol", []] ->
          {:ok, sample_volume("new-vol", %{id: "new-id"})}
      end)

      source = %VolumeContentSource{
        type: {:snapshot, %VolumeContentSource.SnapshotSource{snapshot_id: "src-vol/snap-id"}}
      }

      reply =
        ControllerServer.create_volume(
          %CreateVolumeRequest{name: "new-vol", volume_content_source: source},
          nil
        )

      assert reply.volume.volume_id == "new-vol"
      assert reply.volume.content_source == source
    end

    test "clones a source volume by snapshotting then promoting" do
      put_core(fn
        NeonFS.Core, :get_volume, ["src-vol"] ->
          {:ok, sample_volume("src-vol", %{id: "src-id"})}

        NeonFS.Core.Snapshot, :create, ["src-id", [name: "csi-clone-source-cloned-vol"]] ->
          {:ok, sample_snap("clone-snap-id", "csi-clone-source-cloned-vol")}

        NeonFS.Core.Snapshot, :promote, ["src-id", "clone-snap-id", "cloned-vol", []] ->
          {:ok, sample_volume("cloned-vol", %{id: "cloned-id"})}
      end)

      source = %VolumeContentSource{
        type: {:volume, %VolumeContentSource.VolumeSource{volume_id: "src-vol"}}
      }

      reply =
        ControllerServer.create_volume(
          %CreateVolumeRequest{name: "cloned-vol", volume_content_source: source},
          nil
        )

      assert reply.volume.volume_id == "cloned-vol"
    end

    test "raises INVALID_ARGUMENT for a malformed CSI snapshot id" do
      source = %VolumeContentSource{
        type: {:snapshot, %VolumeContentSource.SnapshotSource{snapshot_id: "no-slash"}}
      }

      assert_raise GRPC.RPCError, ~r/malformed snapshot id/, fn ->
        ControllerServer.create_volume(
          %CreateVolumeRequest{name: "v", volume_content_source: source},
          nil
        )
      end
    end

    test "raises NOT_FOUND when cloning a non-existent volume" do
      put_core(fn NeonFS.Core, :get_volume, _ -> {:error, :not_found} end)

      source = %VolumeContentSource{
        type: {:volume, %VolumeContentSource.VolumeSource{volume_id: "ghost"}}
      }

      assert_raise GRPC.RPCError, ~r/source volume ghost not found/, fn ->
        ControllerServer.create_volume(
          %CreateVolumeRequest{name: "v", volume_content_source: source},
          nil
        )
      end
    end
  end
end
