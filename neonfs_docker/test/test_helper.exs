unless Node.alive?() do
  Application.put_env(:kernel, :epmd_module, NeonFS.Epmd)
  {:ok, _} = Node.start(:neonfs_docker_test, name_domain: :shortnames)
end

# Disable global's partition prevention — peer-cluster integration tests
# rapidly create/destroy peer clusters and `global` misinterprets this as
# overlapping partitions, proactively disconnecting healthy nodes mid-test.
# Must be set at runtime since kernel is already started by the time
# config.exs runs.
Application.put_env(:kernel, :prevent_overlapping_partitions, false)

# Exclude `:docker` tests on hosts without a working `docker` CLI on
# `PATH`. The integration test in `test/integration/` drives the full
# `docker volume create -d neonfs … && docker run --rm -v ...` flow
# against a real daemon; hosts without `docker` would surface install
# errors rather than meaningful failures.
docker_excludes =
  try do
    case System.cmd("docker", ["info"], stderr_to_stdout: true) do
      {_, 0} -> []
      _ -> [:docker]
    end
  rescue
    ErlangError -> [:docker]
  end

ExUnit.configure(exclude: docker_excludes)
ExUnit.start(capture_log: true)
