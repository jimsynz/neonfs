# The metadata mock (`build_mock_metadata_*_opts`) is a single ETS store
# that doesn't model per-shard index trees, so unit tests run at one shard
# (behaviour-identical to pre-sharding). True N-shard distribution is
# exercised by the integration suite (#1312).
Application.put_env(:neonfs_core, :metadata_shard_count, 1)

# The FileIndex flush fans per-shard commits out to this stateless worker
# pool (#1308); start a suite-global instance so tests that drive FileIndex
# writes can commit. Workers carry no per-test state — they apply with the
# writer opts each FileIndex call passes them.
{PartitionSupervisor, shard_committer_opts} = NeonFS.Core.ShardCommitter.pool_spec()
{:ok, _} = PartitionSupervisor.start_link(shard_committer_opts)

Application.put_env(:kernel, :epmd_module, NeonFS.Epmd)
{:ok, _} = Node.start(:neonfs_core_test, name_domain: :shortnames)

Mimic.copy(NeonFS.Core.ChunkFetcher)
Mimic.copy(NeonFS.Core.BackgroundWorker)
Mimic.copy(NeonFS.IO.Scheduler)
Mimic.copy(NeonFS.Core.BlobStore)
Mimic.copy(NeonFS.Core.ChunkIndex)
Mimic.copy(NeonFS.Core.DriveRegistry)
Mimic.copy(NeonFS.Core.DriveTrust)
Mimic.copy(NeonFS.Core.FileIndex)
Mimic.copy(NeonFS.Core.NodeRegistry)
Mimic.copy(NeonFS.Core.Replication)
Mimic.copy(NeonFS.Core.ReplicaRepair)
Mimic.copy(NeonFS.Core.VolumeRegistry)
Mimic.copy(NeonFS.Core.Volume.MetadataReader)
Mimic.copy(NeonFS.Core.RaSupervisor)

# Exclude `:benchmark` tests by default (e.g. #1481's blob-NIF throughput
# baseline) — they print throughput numbers and take a while. Run one with
# `mix test path/to/bench_test.exs --include benchmark`.
ExUnit.start(capture_log: true, exclude: [:benchmark])
