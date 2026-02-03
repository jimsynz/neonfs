{:ok, _} = Node.start(:neonfs_core_test, name_domain: :shortnames)

ExUnit.start(capture_log: true)
