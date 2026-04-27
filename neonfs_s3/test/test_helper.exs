unless Node.alive?() do
  Application.put_env(:kernel, :epmd_module, NeonFS.Epmd)
  {:ok, _} = Node.start(:neonfs_s3_test, name_domain: :shortnames)
end

# Disable global's partition prevention — peer-cluster integration tests
# rapidly create/destroy peer clusters and `global` misinterprets this as
# overlapping partitions, proactively disconnecting healthy nodes mid-test.
# Must be set at runtime since kernel is already started by the time
# config.exs runs.
Application.put_env(:kernel, :prevent_overlapping_partitions, false)

Mimic.copy(NeonFS.Client.ChunkReader)

# Exclude diagnostic profile tests by default (e.g. #534's
# process-heap profile). Match with `--include profile` to run them
# explicitly. Mirrors the exclusion that lived in
# `neonfs_integration/test/test_helper.exs` before the per-interface
# split (#582) moved these tests into `neonfs_s3`.
ExUnit.start(exclude: [:profile])
