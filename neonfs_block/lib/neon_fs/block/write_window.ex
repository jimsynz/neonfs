defmodule NeonFS.Block.WriteWindow do
  @moduledoc """
  A write-back window for one attached device.

  A guest write reaching `NeonFS.Block.Device` costs a whole extent rewrite
  and a metadata commit, whatever its size. Sixteen 4 KiB writes into one
  128 KiB extent therefore cost sixteen of each, when they could cost one.
  This buffers them and drains once — which is the whole reason the extent
  map exists rather than being an end in itself.

  ## The flush contract is sacred

  NBD lets a write without FUA be acknowledged once the server has it;
  durability is what FLUSH promises. That is the only reason this is legal.
  So `flush/1` drains before it answers, and a write carrying FUA reaches
  the device through a flush like any other. Acknowledging a flush over a
  window that has not drained would tell a guest filesystem its journal is
  durable when it is not, which is the one thing a block device must never
  do.

  ## Bounds: a byte cap and a time cap

  It drains on whichever comes first. The byte cap bounds memory — it is
  device-RAM-bounded, never device-size-bounded, since what is held is one
  copy of each dirty extent. The timer bounds how long an un-acked write
  sits undurable: a guest that never flushes is legal under NBD and
  alarming in a post-mortem, so time alone is enough to make it land.

  A byte cap alone, drained only by flush, was rejected for that reason.
  Both knobs are configurable — `:write_window_bytes` and
  `:write_window_ms` — and a trickle workload will commit half-empty
  batches, so the amplification win shrinks exactly where the device is
  idle enough not to care.

  Setting `:write_window_bytes` to `0` drains every write as it arrives,
  which is the behaviour from before this existed.

  ## It has to answer reads too

  A write-back cache that does not serve reads is a correctness bug, not a
  slower cache: a guest that writes a block and reads it back would see
  what was there before. `buffered/2` is what `Device`'s read path overlays
  onto the committed extents it fetched, so a read sees this window's
  writes whether or not they have landed.

  ## Per attachment, which is why single-attach is load-bearing

  One window per device on this node, shared by every connection to it —
  several sockets to one export is how blk-mq gets its parallelism, and a
  per-connection window would let two of them buffer the same extent and
  lose one. That makes it correct only because block volumes are RWO:
  **single-attach enforcement is now load-bearing for correctness rather
  than merely for safety**, and a fencing gap is a data-integrity gap
  rather than an availability one.

  ## A drain that fails poisons the window

  The writes it was holding were acknowledged and are now lost. Reporting
  that at the next operation — including the next flush — is the only
  honest answer; carrying on would let a guest believe a journal commit
  succeeded over bytes that never landed. The device is expected to be torn
  down after one.

  ## Telemetry

    * `[:neonfs, :block, :window_drain]` — Measurements: `extents`,
      `writes`, the guest writes those extents absorbed, `chunk_bytes`, what
      the chunk layer moved to land them, and `duration`. Metadata:
      `export`, `reason` (`:bytes`, `:time`, `:flush` or `:ordering`).

      `writes / extents` is the coalescing ratio, and it is the number this
      module exists to raise. `chunk_bytes` is where a write's amplification
      is now measured: a buffered write has moved nothing yet, so charging
      it to the write's own command event would report a cost that had not
      been paid.
  """

  use GenServer

  alias NeonFS.Block.Device

  require Logger

  @default_bytes 1_048_576
  @default_ms 50

  # How many times a drain redoes itself against contention it lost.
  @drain_retries 3

  @type t :: pid()

  @doc """
  How many bytes of dirty extents may accumulate before a drain.
  """
  @spec byte_cap() :: non_neg_integer()
  def byte_cap, do: Application.get_env(:neonfs_block, :write_window_bytes, @default_bytes)

  @doc """
  How long a dirty extent may sit before a drain, in milliseconds.
  """
  @spec time_cap() :: pos_integer()
  def time_cap, do: Application.get_env(:neonfs_block, :write_window_ms, @default_ms)

  @doc false
  def start_link(device), do: GenServer.start_link(__MODULE__, device)

  @doc """
  Buffers `data` at `offset`, reading only the extents it does not cover
  end to end and only the first time it touches one.

  Answers once the bytes are held, which for a write without FUA is what
  the guest is owed. A write that fills the byte cap drains before
  answering, so the cap bounds memory rather than merely describing it.
  """
  @spec write(t(), non_neg_integer(), binary()) :: :ok | {:error, term()}
  def write(window, offset, data) do
    GenServer.call(window, {:write, offset, data}, 60_000)
  end

  @doc """
  Drains everything held, then answers. The flush contract.
  """
  @spec flush(t()) :: :ok | {:error, term()}
  def flush(window), do: GenServer.call(window, :flush, 60_000)

  @doc """
  The buffered bytes of `index`, or `:miss`.

  What the read path overlays onto what it fetched, so a read sees writes
  this window has not landed yet.
  """
  @spec buffered(t(), non_neg_integer()) :: {:ok, binary()} | :miss
  def buffered(window, index), do: GenServer.call(window, {:buffered, index}, 60_000)

  @doc """
  Drains and answers, for an operation that must not be reordered around
  the window — a discard, whose punch would otherwise land before writes
  that were issued first.
  """
  @spec drain(t()) :: :ok | {:error, term()}
  def drain(window), do: GenServer.call(window, :drain, 60_000)

  @impl GenServer
  def init(device) do
    {:ok, %{device: device, dirty: %{}, bytes: 0, writes: 0, timer: nil, failed: nil}}
  end

  @impl GenServer
  def handle_call({:write, offset, data}, _from, %{failed: nil} = state) do
    case absorb(state, offset, data) do
      {:ok, state} -> reply_after_cap(state)
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:buffered, index}, _from, %{failed: nil} = state) do
    case Map.fetch(state.dirty, index) do
      {:ok, %{bytes: bytes}} -> {:reply, {:ok, bytes}, state}
      :error -> {:reply, :miss, state}
    end
  end

  def handle_call(op, _from, %{failed: nil} = state) when op in [:flush, :drain] do
    {result, state} = drain(state, if(op == :flush, do: :flush, else: :ordering))
    {:reply, result, state}
  end

  # Every operation after a failed drain answers with that failure. The
  # writes it was holding were acknowledged and are gone, so there is no
  # state to carry on from.
  def handle_call(_op, _from, %{failed: reason} = state), do: {:reply, {:error, reason}, state}

  @impl GenServer
  def handle_info(:drain, %{failed: nil} = state) do
    {_result, state} = drain(%{state | timer: nil}, :time)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ─── Buffering ─────────────────────────────────────────────────────────

  defp absorb(state, offset, data) do
    state.device
    |> extent_spans(offset, byte_size(data))
    |> Enum.reduce_while({:ok, {state, 0}}, fn span, {:ok, {acc, taken}} ->
      {_index, _within, count} = span

      case absorb_span(acc, span, binary_part(data, taken, count)) do
        {:ok, acc} -> {:cont, {:ok, {acc, taken + count}}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, {state, _taken}} -> {:ok, %{state | writes: state.writes + 1}}
      {:error, _reason} = error -> error
    end
  end

  # An extent already dirty is spliced in memory: no read, no placement, and
  # the reason sixteen writes into one extent cost one of each rather than
  # sixteen.
  defp absorb_span(state, {index, within, count}, slice) do
    case Map.fetch(state.dirty, index) do
      {:ok, entry} ->
        {:ok, put_in(state.dirty[index], %{entry | bytes: splice(entry.bytes, within, slice)})}

      :error ->
        with {:ok, entry} <- first_touch(state.device, index, within, count, slice) do
          {:ok,
           %{
             state
             | dirty: Map.put(state.dirty, index, entry),
               bytes: state.bytes + byte_size(entry.bytes)
           }}
        end
    end
  end

  # A span covering the extent end to end needs no read: the bytes it is
  # about to hold are the whole extent. It carries no expectation either,
  # because it owes nothing to what was there before — and naming one would
  # make two writers to different extents collide for no reason.
  defp first_touch(device, index, 0, count, slice) when count == :erlang.byte_size(slice) do
    if count == extent_width(device, index) do
      {:ok, %{bytes: slice, expect: :covered}}
    else
      read_and_splice(device, index, 0, slice)
    end
  end

  defp first_touch(device, index, within, _count, slice) do
    read_and_splice(device, index, within, slice)
  end

  defp read_and_splice(device, index, within, slice) do
    with {:ok, existing, target} <- Device.extent_snapshot(device, index) do
      {:ok, %{bytes: splice(existing, within, slice), expect: target}}
    end
  end

  defp extent_width(%{chunk_bytes: chunk_bytes, size: size}, index),
    do: min(chunk_bytes, size - index * chunk_bytes)

  # The write's own decomposition into extents. Core does the same for a
  # read, from its refs; a window that asked core for refs on every write
  # would be paying the round trip it exists to remove.
  defp extent_spans(%{chunk_bytes: chunk_bytes}, offset, length) do
    first = div(offset, chunk_bytes)
    last = div(offset + length - 1, chunk_bytes)

    Enum.map(first..last, fn index ->
      extent_start = index * chunk_bytes
      span_start = max(offset, extent_start)
      span_end = min(offset + length, extent_start + chunk_bytes)
      {index, span_start - extent_start, span_end - span_start}
    end)
  end

  defp splice(bytes, within, slice) do
    tail_start = within + byte_size(slice)

    <<binary_part(bytes, 0, within)::binary, slice::binary,
      binary_part(bytes, tail_start, byte_size(bytes) - tail_start)::binary>>
  end

  defp reply_after_cap(state) do
    if state.bytes >= byte_cap() do
      {result, state} = drain(state, :bytes)
      {:reply, result, state}
    else
      {:reply, :ok, arm_timer(state)}
    end
  end

  # One timer per batch, armed by the write that made the window dirty and
  # left alone by the rest: a timer re-armed on every write is a timer a
  # steady stream of them never lets fire, which is the bound this exists to
  # provide.
  defp arm_timer(%{timer: nil, dirty: dirty} = state) when map_size(dirty) > 0 do
    %{state | timer: Process.send_after(self(), :drain, time_cap())}
  end

  defp arm_timer(state), do: state

  # ─── Draining ──────────────────────────────────────────────────────────

  defp drain(%{dirty: dirty} = state, _reason) when map_size(dirty) == 0 do
    {:ok, cancel_timer(state)}
  end

  defp drain(state, reason) do
    start_time = System.monotonic_time()
    extents = Enum.map(state.dirty, fn {index, %{bytes: bytes}} -> {index, bytes} end)
    expect = for {index, %{expect: e}} <- state.dirty, e != :covered, do: {index, e}

    case publish_retrying(state.device, extents, expect, 0) do
      :ok ->
        emit_drain(state, reason, start_time)
        {:ok, %{cancel_timer(state) | dirty: %{}, bytes: 0, writes: 0}}

      {:error, reason_out} ->
        Logger.error("block write window drain failed; its writes are lost",
          export: state.device.export,
          reason: inspect(reason_out)
        )

        {{:error, reason_out}, %{cancel_timer(state) | failed: reason_out}}
    end
  end

  # Losing a compare-and-swap is contention, not a fault, and this is the
  # only place that still knows the commit is retryable — `Device`'s retry
  # wraps a write that now merely buffers. Past the budget the window is
  # poisoned, which is the honest end for writes that were acknowledged.
  defp publish_retrying(device, extents, expect, attempt) do
    case Device.publish_extents(device, extents, expect) do
      {:error, reason} = error ->
        if attempt < @drain_retries and contended?(reason) do
          Process.sleep(10 * 2 ** attempt)
          publish_retrying(device, extents, expect, attempt + 1)
        else
          error
        end

      result ->
        result
    end
  end

  defp contended?(:stale_chunks), do: true
  defp contended?({:cas_retries_exhausted, _}), do: true
  defp contended?({_stage, {:cas_retries_exhausted, _}}), do: true
  defp contended?(_reason), do: false

  defp cancel_timer(%{timer: nil} = state), do: state

  defp cancel_timer(%{timer: timer} = state) do
    _ = Process.cancel_timer(timer)
    %{state | timer: nil}
  end

  defp emit_drain(state, reason, start_time) do
    :telemetry.execute(
      [:neonfs, :block, :window_drain],
      %{
        extents: map_size(state.dirty),
        writes: state.writes,
        chunk_bytes:
          Enum.reduce(state.dirty, 0, fn {_i, %{bytes: b}}, acc -> acc + byte_size(b) end),
        duration: System.monotonic_time() - start_time
      },
      %{export: state.device.export, reason: reason}
    )
  end
end
