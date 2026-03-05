# Ensure the NFS application is started
Application.ensure_all_started(:neonfs_nfs)

{:ok, _} = Node.start(:neonfs_nfs_test, name_domain: :shortnames)

# Note: NFS integration tests that require neonfs_core have been moved to
# neonfs_integration.

ExUnit.start(capture_log: true)
