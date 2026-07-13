defmodule NeonFS.Bench.Rig do
  @moduledoc """
  Thin shim over the test rig's own `neonfs-rig ssh` subcommand, so the
  benchmark harness drives cluster nodes through the exact same SSH path
  (key, options, ports) as `lib/rig.sh` — no duplicated connection config.

  The `neonfs-rig` executable is located via the `BENCH_RIG` environment
  variable, exported by the `bench` subcommand wrapper.
  """

  @doc "Absolute path to the `neonfs-rig` executable, from `$BENCH_RIG`."
  @spec bin() :: String.t()
  def bin do
    System.get_env("BENCH_RIG") ||
      raise "BENCH_RIG not set — run the harness via `neonfs-rig bench`, not `mix run` directly"
  end

  @doc """
  Run `command` on node `node` over SSH. Raises with the captured output
  on a non-zero exit so a failed op aborts the benchmark rather than
  silently reporting a bogus rate.
  """
  @spec ssh!(String.t(), String.t()) :: String.t()
  def ssh!(node, command) do
    case System.cmd(bin(), ["ssh", node, command], stderr_to_stdout: true) do
      {out, 0} -> out
      {out, code} -> raise "rig ssh (node #{node}) exited #{code}:\n#{out}"
    end
  end
end
