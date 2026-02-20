# Task 0118: Phase 10 Integration Tests

## Status
Complete

## Phase
10 - Event Notification

## Description
Write integration tests that verify the event notification system works correctly across a multi-node cluster. These tests use the `PeerCluster` framework to spawn real peer nodes and exercise the full event lifecycle: event emission on metadata writes, cross-node delivery via `:pg`, local dispatch via Registry, subscription management, gap detection, partition recovery, and FUSE cache invalidation.

The most critical validation is proving the core design goal: interface nodes receive push-based metadata invalidation in real-time, reducing RPC round-trips while maintaining correctness across network partitions.

## Acceptance Criteria
- [ ] New integration test file `neonfs_integration/test/integration/event_notification_test.exs`
- [ ] Test: file creation on core node emits `FileCreated` event received by subscriber on a different node
- [ ] Test: volume creation emits `VolumeCreated` event received by volume lifecycle subscribers
- [ ] Test: directory creation emits `DirCreated` event received by volume subscribers
- [ ] Test: file deletion emits `FileDeleted` event received by subscriber
- [ ] Test: ACL change emits `VolumeAclChanged` event
- [ ] Test: rename emits `FileRenamed` with correct old and new paths
- [ ] Test: events contain correct envelope fields (source_node, sequence, hlc_timestamp)
- [ ] Test: sequence numbers are monotonically increasing from the same source
- [ ] Test: multiple subscribers on the same node all receive the same event (fan-out via Registry)
- [ ] Test: subscriber on node A receives events for writes performed on node B (cross-node delivery)
- [ ] Test: subscribing to volume X does not receive events for volume Y (volume isolation)
- [ ] Test: partition recovery — disconnect a core node, perform writes, reconnect, verify subscriber caches are invalidated
- [ ] Test: FUSE handler uses cached metadata after initial fetch, invalidates on event
- [ ] Test: unsubscribe stops event delivery
- [ ] All tests pass with `mix test` in the `neonfs_integration` directory
- [ ] Tests use `@moduletag timeout: 300_000` for adequate multi-node test time

## Testing Strategy
Use `PeerCluster.start_cluster!/3` to create a 3-node cluster with at least one node running neonfs_fuse. Use `PeerCluster.rpc/6` to execute operations on specific nodes.

### Test Scenarios

1. **Basic Event Delivery**: Set up a subscriber process on node2 for volume V. Create a file on node1 in volume V. Verify node2's subscriber receives a `FileCreated` event with correct volume_id, file_id, and path within the envelope.

2. **Volume Lifecycle Events**: Subscribe to volume events on node2. Create/update/delete a volume on node1. Verify node2 receives VolumeCreated, VolumeUpdated, VolumeDeleted events respectively.

3. **Cross-Node Multi-Subscriber**: Set up 3 subscriber processes on node2 (same volume). Perform a write on node1. Verify all 3 subscribers receive the event. Verify only one message crossed the network (check Relay's `:pg` membership count = 1 for node2).

4. **Volume Isolation**: Subscribe to volume A on node2. Perform writes on volume B from node1. Verify node2 does NOT receive any events.

5. **Sequence Ordering**: Perform 10 sequential writes on node1. Verify subscriber on node2 receives events with sequences 1..10 from node1.

6. **Partition Recovery**: Start subscriber on node2 with a cache. Populate cache entries. Disconnect node1 (the core node). Perform writes on another core node. Reconnect node1. Verify node2's subscriber received `:neonfs_invalidate_all` and caches were cleared. Verify subsequent reads fetch fresh data.

7. **Unsubscribe**: Subscribe on node2, verify events are received. Unsubscribe. Perform more writes. Verify no more events are received.

8. **FUSE Cache Integration**: Mount a volume on a FUSE node. Perform `getattr` — verify RPC call made. Perform `getattr` again — verify cache hit (no RPC). Modify the file from a core node. Wait for event delivery. Perform `getattr` — verify cache invalidated, fresh RPC call made with updated data.

## Dependencies
- task_0112 (Event structs)
- task_0113 (Subscription API, Relay, Registry)
- task_0114 (Broadcaster)
- task_0115 (Core metadata event emission)
- task_0116 (Partition recovery)
- task_0117 (FUSE metadata cache)

## Files to Create/Modify
- `neonfs_integration/test/integration/event_notification_test.exs` (new — integration tests)

## Reference
- spec/pubsub.md — full specification
- Existing pattern: `neonfs_integration/test/integration/cluster_ca_test.exs` for multi-node test structure
- Existing pattern: `neonfs_integration/test/integration/failure_test.exs` for node failure/reconnection testing
- Existing pattern: `neonfs_integration/test/support/cluster_case.ex` for `wait_until`, `assert_eventually`

## Notes
The partition recovery test is the most important validation — it proves the system is correct under adverse conditions. To test partition recovery:
1. Use `:erpc.call(node, :net_kernel, :disconnect, [other_node])` to simulate partition
2. Perform metadata writes on remaining nodes
3. Reconnect with `Node.connect/1`
4. Wait for the debounce period (5 seconds by default)
5. Verify cache invalidation occurred on the reconnected node

For the FUSE cache integration test, the test must account for event delivery being asynchronous. Use `assert_eventually` or `wait_until` helpers from `ClusterCase` to wait for cache invalidation after an event-triggering write.

The `:pg` group membership test (verifying only one message crosses the network per node) is tricky to observe directly. Instead, verify the functional outcome: all 3 local subscribers received the event, and the Relay's `volume_refs` state shows a single `:pg` membership.

These tests complement the unit tests from tasks 0112-0117. Integration tests verify all components work together in a realistic multi-node environment, while unit tests verify individual functions in isolation.
