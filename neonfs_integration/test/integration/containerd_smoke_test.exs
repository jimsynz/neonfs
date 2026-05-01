defmodule NeonFS.Integration.ContainerdSmokeTest do
  @moduledoc """
  Prep slice for #554. Asserts that the CI runner has `containerd` and
  `ctr` available so future containerd content-store integration tests
  (which will exercise `neonfs_containerd`'s gRPC plugin against a real
  containerd daemon) have something to talk to.

  Tagged `:requires_containerd` — `test_helper.exs` excludes the tag by
  default when those binaries are missing, and the `neonfs_integration`
  CI job opts back in via `mix test --include requires_containerd` after
  installing the package.
  """

  use ExUnit.Case, async: true

  @moduletag :requires_containerd

  describe "containerd toolchain" do
    test "containerd is on PATH and reports a version" do
      path = System.find_executable("containerd")
      assert is_binary(path), "expected `containerd` on PATH"

      {output, exit_code} = System.cmd(path, ["--version"], stderr_to_stdout: true)
      assert exit_code == 0, "`containerd --version` exited #{exit_code}: #{output}"
      assert output =~ ~r/containerd/i
    end

    test "ctr is on PATH and reports a version" do
      path = System.find_executable("ctr")
      assert is_binary(path), "expected `ctr` on PATH"

      {output, exit_code} = System.cmd(path, ["--version"], stderr_to_stdout: true)
      assert exit_code == 0, "`ctr --version` exited #{exit_code}: #{output}"
      assert output =~ ~r/ctr/i
    end
  end
end
