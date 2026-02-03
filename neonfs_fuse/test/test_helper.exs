# Ensure applications are started
Application.ensure_all_started(:neonfs_core)
Application.ensure_all_started(:neonfs_fuse)

{:ok, _} = Node.start(:neonfs_fuse_test, name_domain: :shortnames)

ExUnit.start()
