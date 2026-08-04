defmodule NeonFS.Core.CommitTimeoutInvariantTest do
  @moduledoc """
  A mutating `FileIndex` call must outlast the commit it waits on.

  Every mutation stages into a windowed flush that commits through
  `VolumeCommitter`. If the client call gives up first, a slow-but-
  successful commit is reported to the caller as a timeout while the write
  goes on to land — observed on a loaded runner as
  `{:timeout, {GenServer, :call, [FileIndex, {:create_committing_chunks, …}]}}`
  from a multi-MiB NFSv3 write that in fact succeeded.

  The committer's `@commit_timeout` comment already asserted this
  ordering; nothing enforced it, and the two had drifted to 30 s versus
  10–15 s. This test is the enforcement.
  """

  use ExUnit.Case, async: true

  alias NeonFS.Core.{FileIndex, VolumeCommitter}

  test "a mutating call outlasts the commit it waits on" do
    assert FileIndex.mutation_call_timeout() > VolumeCommitter.commit_timeout(),
           """
           FileIndex.mutation_call_timeout/0 (#{FileIndex.mutation_call_timeout()}ms) must exceed \
           VolumeCommitter.commit_timeout/0 (#{VolumeCommitter.commit_timeout()}ms), or a caller \
           abandons a commit that is still running and a successful write reports as a timeout.
           """
  end

  # Guards against a new mutation arriving with a fresh literal. Scanning
  # the source is crude, but the alternative — asserting a timeout a
  # `GenServer.call` was made with — is not observable from outside.
  test "no mutating client call carries a hard-coded timeout" do
    offenders =
      "lib/neon_fs/core/file_index.ex"
      |> File.read!()
      |> String.split("GenServer.call(")
      |> Enum.drop(1)
      |> Enum.filter(&mutation_call_with_literal_timeout?/1)

    assert offenders == [],
           "FileIndex mutation calls must use mutation_call_timeout/0; " <>
             "#{length(offenders)} call(s) still pass a literal."
  end

  # Look only at the text up to the call's closing paren, and only when it
  # targets this module — a literal timeout on some other server is not
  # this invariant's business.
  defp mutation_call_with_literal_timeout?(fragment) do
    call = fragment |> String.split(")\n") |> List.first() || ""

    String.contains?(call, "__MODULE__") and
      Regex.match?(~r/,\s*\d[\d_]*\s*$/, String.trim_trailing(call))
  end
end
