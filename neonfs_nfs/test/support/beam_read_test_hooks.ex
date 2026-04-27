defmodule NeonFS.NFS.IntegrationTest.BeamReadTestHooks do
  @moduledoc """
  App-env hook implementations for the BEAM NFSv3 read-path smoke
  test (sub-issue #587 of #533). The test runs the BEAM stack on a
  *core* peer; `core_call_fn` and `read_file_stream_fn` are
  short-circuited to local apply / `NeonFS.Core.read_file_stream/3`
  rather than going through `NeonFS.Client.Router` and
  `NeonFS.Client.ChunkReader` (which are the non-core-peer
  abstractions; the cross-node-via-ChunkReader path is the explicit
  subject of #588).

  Lives in `test/support` so it compiles into `_build/test/lib/.../ebin`
  on the test runner; peer nodes inherit the same code paths via
  `:code.get_path/0`, so `&Mod.local_read_file_stream/4` is a stable
  remote-function capture.
  """

  @spec local_read_file_stream(String.t(), String.t(), non_neg_integer(), non_neg_integer()) ::
          Enumerable.t()
  def local_read_file_stream(volume_name, path, offset, count) do
    # `Core.read_file_stream/3` wraps the stream in `{:ok, %{stream:
    # ..., file_size: ...}}`. The handler's `take_bytes/2` wants the
    # raw `Enumerable.t()`, so unwrap.
    case NeonFS.Core.read_file_stream(volume_name, path, offset: offset, length: count) do
      {:ok, %{stream: stream}} -> stream
      {:error, _} = err -> err
    end
  end
end
