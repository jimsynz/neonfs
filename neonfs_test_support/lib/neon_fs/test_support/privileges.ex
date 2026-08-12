defmodule NeonFS.TestSupport.Privileges do
  @moduledoc """
  Probes for the privileges a test needs from the machine running it.

  A test that mounts a filesystem, attaches a loop device or starts a
  `containerd` needs to be root, and a developer's workstation is not.
  The suite's answer is a capability tag the `test_helper.exs` excludes
  when the probe says no — `:requires_root` alongside `:loopback`,
  `:requires_containerd` and `:requires_test_registry`.

  A tag with no matching exclusion is worse than no tag at all: it reads
  as gated while running everywhere, so the suite fails on a workstation
  for reasons that have nothing to do with the change under test.
  """

  @doc """
  Whether the current process is running as root.
  """
  @spec root?() :: boolean()
  def root? do
    case System.cmd("id", ["-u"], stderr_to_stdout: true) do
      {"0\n", 0} -> true
      _otherwise -> false
    end
  end
end
