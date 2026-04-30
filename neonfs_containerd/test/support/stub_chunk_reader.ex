defmodule NeonFS.Containerd.StubChunkReader do
  @moduledoc """
  Test stub for `NeonFS.Client.ChunkReader`. Drives
  `NeonFS.Containerd.ContentServer` from a process-dictionary script
  rather than a real cluster.

  Set the script via `set_response/1` before each test:

      StubChunkReader.set_response({:ok, %{stream: ["chunk1", "chunk2"], file_size: 12}})

  ContentServer (via the `:chunk_reader` Application env) calls
  `read_file_stream/3` here, the stub returns the script verbatim,
  and the test inspects whatever the server did with the stream.
  """

  @key {__MODULE__, :response}

  @doc """
  Stash the next response `read_file_stream/3` should return.
  """
  @spec set_response(term()) :: :ok
  def set_response(response) do
    Process.put(@key, response)
    :ok
  end

  @doc """
  Stash the captured arguments for assertion in tests.
  """
  @spec last_args() :: [term()]
  def last_args do
    Process.get({__MODULE__, :last_args}, [])
  end

  @doc """
  ChunkReader-shaped read_file_stream — returns whatever
  `set_response/1` stashed. Captures arguments for `last_args/0`.
  """
  @spec read_file_stream(String.t(), String.t(), keyword()) :: term()
  def read_file_stream(volume, path, opts) do
    Process.put({__MODULE__, :last_args}, [volume, path, opts])
    Process.get(@key, {:error, :not_set})
  end
end
