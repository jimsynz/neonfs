defmodule NeonFS.Integration.AuditTest do
  @moduledoc """
  Phase 6 integration tests for audit logging.

  Tests that security operations produce audit log entries:
  - ACL changes generate audit events
  - Key rotation generates audit events
  - Events are queryable by type, UID, and resource
  - CLI handler audit list returns events
  """
  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Core.KeyManager
  alias NeonFS.Core.VolumeEncryption
  alias NeonFS.Core.VolumeRegistry

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 1

  describe "audit log records security operations" do
    test "ACL grant produces audit event", %{cluster: cluster} do
      :ok = init_audit_cluster(cluster)

      # Grant ACL
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :grant, [
          volume_id(cluster),
          {:uid, 1000},
          [:read]
        ])

      # AuditLog.log uses cast (async), so wait for it to be processed
      assert_eventually timeout: 5_000 do
        events =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :query, [
            [event_type: :volume_acl_changed, limit: 10]
          ])

        is_list(events) and events != []
      end

      events =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :query, [
          [event_type: :volume_acl_changed, limit: 10]
        ])

      acl_event = List.first(events)
      assert acl_event.event_type == :volume_acl_changed
    end

    test "key rotation produces audit event", %{cluster: cluster} do
      :ok = init_encrypted_audit_cluster(cluster)

      # Start key rotation
      {:ok, _rotation} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.KeyRotation, :start_rotation, [
          volume_id(cluster)
        ])

      # AuditLog.log uses cast (async), so wait for it to be processed
      assert_eventually timeout: 5_000 do
        events =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :query, [
            [event_type: :key_rotated, limit: 10]
          ])

        is_list(events) and events != []
      end

      events =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :query, [
          [event_type: :key_rotated, limit: 10]
        ])

      rotation_event = List.first(events)
      assert rotation_event.event_type == :key_rotated
    end

    test "audit events are queryable by actor UID", %{cluster: cluster} do
      :ok = init_audit_cluster(cluster)

      # Log a custom audit event with specific UID
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :log_event, [
          [
            event_type: :acl_grant,
            actor_uid: 42,
            resource: "volume:#{volume_id(cluster)}",
            details: %{principal: "uid:1000", permissions: [:read]},
            outcome: :success
          ]
        ])

      # AuditLog.log uses cast (async), so wait for it to be processed
      assert_eventually timeout: 5_000 do
        events =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :query, [
            [actor_uid: 42, limit: 10]
          ])

        is_list(events) and events != []
      end

      # Query by UID
      events =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :query, [
          [actor_uid: 42, limit: 10]
        ])

      assert is_list(events)
      assert events != []
      assert Enum.all?(events, &(&1.actor_uid == 42))
    end

    test "audit events are queryable by resource", %{cluster: cluster} do
      :ok = init_audit_cluster(cluster)

      resource = "volume:#{volume_id(cluster)}"

      # Log a custom audit event with specific resource
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :log_event, [
          [
            event_type: :acl_revoke,
            actor_uid: 0,
            resource: resource,
            details: %{principal: "uid:2000"},
            outcome: :success
          ]
        ])

      # AuditLog.log uses cast (async), so wait for it to be processed
      assert_eventually timeout: 5_000 do
        events =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :query, [
            [resource: resource, limit: 10]
          ])

        is_list(events) and events != []
      end

      # Query by resource
      events =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :query, [
          [resource: resource, limit: 10]
        ])

      assert is_list(events)
      assert events != []
      assert Enum.all?(events, &(&1.resource == resource))
    end
  end

  describe "CLI handler audit commands" do
    test "handle_audit_list returns events with filters", %{cluster: cluster} do
      :ok = init_audit_cluster(cluster)

      # Generate some audit events
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :log_event, [
          [
            event_type: :acl_grant,
            actor_uid: 100,
            resource: "volume:test",
            details: %{},
            outcome: :success
          ]
        ])

      # AuditLog.log uses cast (async), so wait for it to be processed
      assert_eventually timeout: 5_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_audit_list, [
               %{"limit" => 10}
             ]) do
          {:ok, events} when is_list(events) -> events != []
          _ -> false
        end
      end

      # Query via CLI handler
      {:ok, events} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_audit_list, [
          %{"limit" => 10}
        ])

      assert is_list(events)
      assert events != []

      # Each event should have required fields
      event = List.first(events)
      assert Map.has_key?(event, :id)
      assert Map.has_key?(event, :event_type)
      assert Map.has_key?(event, :timestamp)
    end
  end

  # ─── Helpers ──────────────────────────────────────────────────────────

  defp init_cluster_base(cluster) do
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["audit-test"])

    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, []) do
            {:ok, _} -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )
  end

  defp init_audit_cluster(cluster) do
    init_cluster_base(cluster)

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "audit-volume",
        %{}
      ])

    Process.put(:test_volume_id, volume.id)
    :ok
  end

  defp init_encrypted_audit_cluster(cluster) do
    init_cluster_base(cluster)

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :create, [
        "enc-audit-volume",
        [
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        ]
      ])

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, [
        "enc-audit-volume"
      ])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, KeyManager, :setup_volume_encryption, [
        volume.id
      ])

    Process.put(:test_volume_id, volume.id)
    :ok
  end

  defp volume_id(_cluster) do
    Process.get(:test_volume_id)
  end
end
