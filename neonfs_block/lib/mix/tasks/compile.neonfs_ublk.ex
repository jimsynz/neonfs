defmodule Mix.Tasks.Compile.NeonfsUblk do
  @moduledoc """
  Builds the ublk helper binary and stages it in `priv/native`.

  The helper is a `cargo` **bin** crate rather than a NIF, so Rustler has
  nothing to say about it — but it is still half of a wire protocol whose
  other half is this application, and the two halves are only kept in step by
  being compiled together. That is why this is a compiler rather than a
  packaging step: the binary cannot *run* in CI or the dev container, which
  have no ublk driver, so a build failure is the only signal available there
  that someone changed one side of `NeonFS.Block.Ublk.Protocol` and not the
  other. A packaging-time build would move that signal to the nightly.

  Built with `--release` unconditionally. The helper exists for its numbers;
  a debug io_uring loop would make the rig's measurements describe the build
  profile rather than the storage engine.
  """

  use Mix.Task.Compiler

  @crate "neonfs_ublk"

  @impl Mix.Task.Compiler
  def run(_args) do
    crate_dir = Path.join([__DIR__, "..", "..", "..", "native", @crate]) |> Path.expand()

    case cargo(crate_dir) do
      0 -> stage(crate_dir)
      status -> {:error, [diagnostic("cargo build failed with status #{status}")]}
    end
  end

  @impl Mix.Task.Compiler
  def clean do
    File.rm(staged_path())
    :ok
  end

  defp cargo(crate_dir) do
    {_output, status} =
      System.cmd("cargo", ["build", "--release"],
        cd: crate_dir,
        into: IO.stream(:stdio, :line),
        stderr_to_stdout: true
      )

    status
  end

  defp stage(crate_dir) do
    built = Path.join([crate_dir, "target", "release", @crate])
    staged = staged_path()

    File.mkdir_p!(Path.dirname(staged))
    File.cp!(built, staged)
    File.chmod!(staged, 0o755)

    {:ok, []}
  end

  defp staged_path, do: Path.join([Mix.Project.app_path(), "priv", "native", @crate])

  defp diagnostic(message) do
    %Mix.Task.Compiler.Diagnostic{
      compiler_name: "neonfs_ublk",
      file: Path.join(["native", @crate, "Cargo.toml"]),
      message: message,
      position: 0,
      severity: :error
    }
  end
end
