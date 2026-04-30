{:ok, _} = Registry.start_link(keys: :unique, name: NeonFS.Containerd.WriteRegistry)
{:ok, _} = NeonFS.Containerd.WriteSupervisor.start_link([])

ExUnit.start()
