# Ensure the NFS application is started
Application.ensure_all_started(:neonfs_nfs)

unless Node.alive?() do
  Application.put_env(:kernel, :epmd_module, NeonFS.Epmd)
  {:ok, _} = Node.start(:neonfs_nfs_test, name_domain: :shortnames)
end

# Disable global's partition prevention — peer-cluster integration tests
# rapidly create/destroy peer clusters and `global` misinterprets this as
# overlapping partitions, proactively disconnecting healthy nodes mid-test.
# Must be set at runtime since kernel is already started by the time
# config.exs runs.
Application.put_env(:kernel, :prevent_overlapping_partitions, false)

# The peer-cluster BEAM NFSv3 read-path smoke test (#587) lives at
# `test/integration/nfsv3_beam_read_test.exs`. Other interface packages
# moved their integration tests in via the same pattern under #582.

Mimic.copy(NeonFS.Client.ChunkReader)
Mimic.copy(NeonFS.Client.Router)

ExUnit.start(capture_log: true)
