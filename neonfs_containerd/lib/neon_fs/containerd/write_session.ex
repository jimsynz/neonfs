defmodule NeonFS.Containerd.WriteSession do
  @moduledoc """
  In-progress write session for a single containerd write `ref`.

  Owns:

    * A streaming SHA-256 hasher accumulating every byte of `data`
      handed in via `feed/2`.
    * A `Task` running `NeonFS.Client.ChunkWriter.write_file_stream/4`
      against the partial-write path (`_writes/<ref>` per the volume
      layout decision in #547). The task pulls binary segments from a
      process inbox the session feeds.
    * Bookkeeping for `started_at`, `updated_at`, `offset`, `total`,
      `expected` so STAT requests can return the canonical state and
      so resume across reconnects works without buffering a frame.

  ## Lifecycle

  1. `start_link/1` boots the GenServer + ChunkWriter task. The task
     blocks waiting for the first segment.
  2. `feed/2` appends bytes — runs them through the hasher, advances
     `:offset`, hands the bytes off to the task. Returns `{:ok, new_offset}`.
  3. `commit/2` halts the inbox stream, awaits the chunk-writer task,
     verifies the running digest matches `expected`, then calls the
     core `commit_chunks/4` RPC against the canonical `sha256/<ab>/<cd>/<rest>`
     path. On any failure best-effort aborts the chunks via the
     ChunkWriter abort hook.
  4. `abort/1` halts the inbox stream and cleans up. Used when the
     containerd `Abort` RPC fires (sub-issue #552 — for now this is
     the failure-cleanup path used by `commit/2` on digest mismatch).

  Resume: the GenServer outlives any single bidi-stream connection
  via the registry. A reconnect with the same `ref` finds the running
  session and `feed`s into it from `offset` onward.
  """

  use GenServer, restart: :temporary
  require Logger

  alias NeonFS.Client.{ChunkWriter, Router}
  alias NeonFS.Containerd.{Digest, WriteRegistry}

  @typedoc "GenServer state."
  @type t :: %__MODULE__{
          ref: String.t() | nil,
          volume: String.t() | nil,
          started_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil,
          offset: non_neg_integer(),
          total: non_neg_integer(),
          expected: String.t(),
          hash: term(),
          writer_task: Task.t() | nil,
          writer_done: boolean(),
          chunk_writer_module: module()
        }

  defstruct ref: nil,
            volume: nil,
            started_at: nil,
            updated_at: nil,
            offset: 0,
            total: 0,
            expected: "",
            hash: nil,
            writer_task: nil,
            writer_done: false,
            chunk_writer_module: ChunkWriter

  ## Public API

  @doc """
  Start a session for `ref`. Spawns the chunk-writer task and
  registers the GenServer in `WriteRegistry`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    ref = Keyword.fetch!(opts, :ref)
    GenServer.start_link(__MODULE__, opts, name: WriteRegistry.via(ref))
  end

  @doc """
  Append `data` to the session. Updates the running hash and offset
  and ships the bytes to the chunk-writer task. Returns the new
  cumulative offset.

  An `expected_offset` other than the current offset rejects the
  feed with `{:error, :offset_mismatch}` so resume races don't
  silently corrupt the stream.
  """
  @spec feed(pid(), binary(), non_neg_integer() | nil) ::
          {:ok, non_neg_integer()} | {:error, :offset_mismatch}
  def feed(pid, data, expected_offset \\ nil) do
    GenServer.call(pid, {:feed, data, expected_offset})
  end

  @doc """
  Returns the session's current `offset`, `total`, `started_at`,
  `updated_at`. Cheap — no hashing or I/O.
  """
  @spec stat(pid()) :: %{
          offset: non_neg_integer(),
          total: non_neg_integer(),
          started_at: DateTime.t(),
          updated_at: DateTime.t()
        }
  def stat(pid) do
    GenServer.call(pid, :stat)
  end

  @doc """
  Set the session's expected digest (containerd may send `:expected`
  on any frame). Recorded for the eventual hash check on `commit/2`.
  """
  @spec set_expected(pid(), String.t()) :: :ok
  def set_expected(pid, expected) when is_binary(expected) do
    GenServer.call(pid, {:set_expected, expected})
  end

  @doc """
  Set the session's expected total size if provided. Informational
  only — used in STAT replies.
  """
  @spec set_total(pid(), non_neg_integer()) :: :ok
  def set_total(pid, total) when is_integer(total) and total >= 0 do
    GenServer.call(pid, {:set_total, total})
  end

  @doc """
  Halt the inbox stream, await the chunk-writer task, verify the
  running hash equals `expected`, then commit the chunks at the
  canonical `sha256/<ab>/<cd>/<rest>` path.

  On any failure the partial chunks are best-effort aborted. The
  session terminates either way — successful commits don't survive
  for re-use.
  """
  @spec commit(pid(), String.t() | nil) ::
          {:ok, %{digest: String.t(), offset: non_neg_integer(), total: non_neg_integer()}}
          | {:error, :digest_mismatch | :empty_write | term()}
  def commit(pid, expected_override \\ nil) do
    GenServer.call(pid, {:commit, expected_override}, :infinity)
  end

  @doc """
  Halt the inbox stream and abort any chunks the writer task already
  shipped. Terminates the session.
  """
  @spec abort(pid()) :: :ok
  def abort(pid) do
    GenServer.call(pid, :abort, :infinity)
  end

  @doc """
  Abort every session whose `updated_at` is older than `max_age_seconds`
  (default 24 hours). Returns the list of refs that were aborted —
  callers can log / telemetry-emit them. Sessions still actively
  receiving data are left alone.
  """
  @spec abort_stale(pos_integer()) :: [String.t()]
  def abort_stale(max_age_seconds \\ 86_400) do
    cutoff = DateTime.add(DateTime.utc_now(), -max_age_seconds, :second)

    WriteRegistry.list_all()
    |> Enum.flat_map(fn {ref, pid} ->
      try do
        snapshot = stat(pid)

        if DateTime.compare(snapshot.updated_at, cutoff) == :lt do
          _ = abort(pid)
          [ref]
        else
          []
        end
      catch
        :exit, _ -> []
      end
    end)
  end

  ## GenServer callbacks

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    now = DateTime.utc_now()

    chunk_writer = Keyword.get(opts, :chunk_writer_module, ChunkWriter)

    state = %__MODULE__{
      ref: Keyword.fetch!(opts, :ref),
      volume: Keyword.get(opts, :volume, default_volume()),
      started_at: now,
      updated_at: now,
      total: Keyword.get(opts, :total, 0),
      expected: Keyword.get(opts, :expected, ""),
      hash: :crypto.hash_init(:sha256),
      chunk_writer_module: chunk_writer
    }

    {:ok, state, {:continue, :start_writer}}
  end

  @impl true
  def handle_continue(:start_writer, state) do
    volume = state.volume
    path = "_writes/#{state.ref}"
    chunk_writer = state.chunk_writer_module

    task =
      Task.async(fn ->
        # The data stream pulls binary segments from the task's own
        # mailbox. The session pushes via `send(task.pid, {:segment, data})`
        # — bypassing GenServer.call so the GenServer never blocks
        # awaiting the task.
        data_stream =
          Stream.resource(
            fn -> :ok end,
            fn _ ->
              receive do
                {:segment, data} -> {[data], :ok}
                :done -> {:halt, :ok}
              end
            end,
            fn _ -> :ok end
          )

        chunk_writer.write_file_stream(volume, path, data_stream, [])
      end)

    {:noreply, %{state | writer_task: task}}
  end

  @impl true
  def handle_call({:feed, data, expected_offset}, _from, state) do
    cond do
      not is_nil(expected_offset) and expected_offset != state.offset ->
        {:reply, {:error, :offset_mismatch}, state}

      data == "" ->
        {:reply, {:ok, state.offset}, touch(state)}

      true ->
        send(state.writer_task.pid, {:segment, data})

        new_state =
          state
          |> Map.update!(:hash, &:crypto.hash_update(&1, data))
          |> Map.update!(:offset, &(&1 + byte_size(data)))
          |> touch()

        {:reply, {:ok, new_state.offset}, new_state}
    end
  end

  def handle_call(:stat, _from, state) do
    {:reply,
     %{
       offset: state.offset,
       total: state.total,
       started_at: state.started_at,
       updated_at: state.updated_at
     }, state}
  end

  def handle_call({:set_expected, expected}, _from, state) do
    {:reply, :ok, %{state | expected: expected}}
  end

  def handle_call({:set_total, total}, _from, state) do
    {:reply, :ok, %{state | total: total}}
  end

  def handle_call({:commit, expected_override}, from, state) do
    expected = expected_override || state.expected
    send(state.writer_task.pid, :done)
    state = %{state | writer_done: true}
    GenServer.reply(from, do_commit(state, expected))
    {:stop, :normal, state}
  end

  def handle_call(:abort, from, state) do
    send(state.writer_task.pid, :done)
    state = %{state | writer_done: true}
    GenServer.reply(from, do_abort(state))
    {:stop, :normal, state}
  end

  @impl true
  def handle_info({ref, _result}, state) when is_reference(ref) do
    # Task.async / Task.await tag — handled by Task.await/2 inline.
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:noreply, state}
  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, _state), do: :ok

  ## Internal

  defp touch(state), do: %{state | updated_at: DateTime.utc_now()}

  defp do_commit(state, expected) do
    with :ok <- validate_expected_present(expected),
         {:ok, refs} <- await_writer(state),
         {:ok, computed_digest} <- finalise_hash(state),
         :ok <- verify_digest(computed_digest, expected),
         {:ok, path} <- Digest.to_path(expected),
         {:ok, _meta} <- commit_to_canonical(state.volume, path, refs) do
      {:ok,
       %{
         digest: expected,
         offset: state.offset,
         total: state.total
       }}
    else
      {:error, :digest_mismatch} = err ->
        # The chunks landed in the blob store but the file didn't
        # commit; chunks orphan to GC. Best-effort delete here would
        # be premature without the locations map — `chunk_refs_to_commit_opts/1`
        # has it but it's tied to the refs returned from the writer.
        # Same shape as core's own write-failure path.
        err

      {:error, _} = err ->
        err
    end
  end

  defp do_abort(state) do
    case await_writer(state) do
      {:ok, _refs} -> :ok
      {:error, _} -> :ok
    end
  end

  defp await_writer(%__MODULE__{writer_task: nil}), do: {:ok, []}

  defp await_writer(%__MODULE__{writer_task: task}) do
    case Task.await(task, :infinity) do
      {:ok, refs} -> {:ok, refs}
      {:error, _} = err -> err
    end
  end

  defp finalise_hash(state) do
    digest_bytes = :crypto.hash_final(state.hash)
    {:ok, "sha256:" <> Base.encode16(digest_bytes, case: :lower)}
  end

  defp verify_digest(_computed, ""), do: {:error, :digest_mismatch}
  defp verify_digest(same, same), do: :ok
  defp verify_digest(_computed, _expected), do: {:error, :digest_mismatch}

  defp validate_expected_present(""), do: {:error, :digest_mismatch}
  defp validate_expected_present(_), do: :ok

  defp commit_to_canonical(volume, path, refs) do
    %{hashes: hashes, locations: locations, chunk_codecs: chunk_codecs, total_size: total_size} =
      ChunkWriter.chunk_refs_to_commit_opts(refs)

    extra = [total_size: total_size, locations: locations, chunk_codecs: chunk_codecs]

    commit_fn = Application.get_env(:neonfs_containerd, :commit_chunks_fn, &default_commit/4)
    commit_fn.(volume, path, hashes, extra)
  end

  defp default_commit(volume, path, hashes, extra) do
    Router.call(NeonFS.Core, :commit_chunks, [volume, path, hashes, extra])
  end

  defp default_volume do
    Application.get_env(:neonfs_containerd, :volume, "containerd")
  end
end
