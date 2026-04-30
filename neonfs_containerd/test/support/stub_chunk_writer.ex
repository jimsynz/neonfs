defmodule NeonFS.Containerd.StubChunkWriter do
  @moduledoc """
  Test stub for `NeonFS.Client.ChunkWriter`. A single agent stores
  the next `write_file_stream/4` response, the args of the last
  call, and the binary segments drained from the data stream.

  Tests are `async: false` because the agent is shared across calls.
  Set up via:

      StubChunkWriter.start({:ok, [%{hash: "h1"}]})
      Application.put_env(:neonfs_containerd, :chunk_writer_module, StubChunkWriter)

  And inspect via `collected_segments/0` / `last_args/0`.
  """

  use Agent

  @name {:global, __MODULE__}

  @doc """
  Start (or reset) the stub with the response `write_file_stream/4`
  should return. Restarts the agent if it's already running.
  """
  @spec start(term()) :: {:ok, pid()}
  def start(response) do
    case :global.whereis_name(__MODULE__) do
      :undefined ->
        Agent.start_link(fn -> initial(response) end, name: @name)

      pid ->
        Agent.update(@name, fn _ -> initial(response) end)
        {:ok, pid}
    end
  end

  defp initial(response), do: %{response: response, segments: [], args: nil}

  @doc "Stop the stub agent."
  @spec stop() :: :ok
  def stop do
    case :global.whereis_name(__MODULE__) do
      :undefined -> :ok
      _pid -> Agent.stop(@name)
    end
  end

  @doc "Segments drained from the data stream during the last call."
  @spec collected_segments() :: [binary()]
  def collected_segments do
    Agent.get(@name, & &1.segments)
  end

  @doc "Args of the last `write_file_stream/4` call: `{volume, path, opts}`."
  @spec last_args() :: {String.t(), String.t(), keyword()} | nil
  def last_args do
    Agent.get(@name, & &1.args)
  end

  @doc """
  ChunkWriter-shaped `write_file_stream/4`. Captures args and drains
  the data stream synchronously before returning the stashed
  response.
  """
  @spec write_file_stream(String.t(), String.t(), Enumerable.t(), keyword()) :: term()
  def write_file_stream(volume, path, stream, opts) do
    Agent.update(@name, &Map.put(&1, :args, {volume, path, opts}))

    Enum.each(stream, fn segment ->
      Agent.update(@name, fn s -> %{s | segments: s.segments ++ [segment]} end)
    end)

    Agent.get(@name, & &1.response)
  end
end
