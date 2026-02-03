{:ok, _} = Node.start(:neonfs_integration_test, name_domain: :shortnames)

# Build CLI (cargo skips if unchanged)
cli_dir = Path.expand("../../neonfs-cli", __DIR__)

case System.cmd("cargo", ["build", "--release"],
       cd: cli_dir,
       stderr_to_stdout: true
     ) do
  {_output, 0} ->
    :ok

  {output, code} ->
    IO.puts("\n❌ Failed to build CLI (exit code #{code}):")
    IO.puts(output)
    System.halt(1)
end

ExUnit.start(capture_log: true)
