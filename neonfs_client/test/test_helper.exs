Application.put_env(:kernel, :epmd_module, NeonFS.Epmd)
{:ok, _} = Node.start(:neonfs_client_test, name_domain: :shortnames)

Mimic.copy(NeonFS.Client.Router)
Mimic.copy(NeonFS.Client.Discovery)
Mimic.copy(NeonFS.Client.Join)
Mimic.copy(NeonFS.Client.RootPlacement)

ExUnit.start(capture_log: true)
