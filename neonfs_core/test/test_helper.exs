Application.put_env(:kernel, :epmd_module, NeonFS.Epmd)
{:ok, _} = Node.start(:neonfs_core_test, name_domain: :shortnames)

Mimic.copy(NeonFS.Core.ChunkFetcher)
Mimic.copy(NeonFS.Core.BackgroundWorker)
Mimic.copy(NeonFS.IO.Scheduler)

ExUnit.start(capture_log: true)
