{:ok, _} = Node.start(:neonfs_client_test, name_domain: :shortnames)

ExUnit.start(capture_log: true)
