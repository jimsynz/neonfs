defmodule NeonFS.Integration.ProcessMemoryProfile do
  @moduledoc """
  Per-process memory snapshot helper used by the streaming-upload
  process-heap profile (#534).

  `snapshot/0` walks `Process.list/0` and grabs `:memory`,
  `:initial_call`, `:registered_name`, and `:current_function` for
  every process. Two snapshots taken before and after an operation
  let the caller compute a per-process delta and rank the processes
  whose heap grew most.

  This module deliberately does *not* depend on `:recon` so the
  profile can be invoked on a peer node without adding deps.
  """

  alias NeonFS.Client.ChunkWriter
  alias NeonFS.S3.Backend, as: S3Backend

  @typedoc "One process's snapshot."
  @type entry :: %{
          required(:pid) => pid(),
          required(:memory) => non_neg_integer(),
          required(:initial_call) => mfa() | :undefined,
          required(:registered_name) => atom() | :undefined,
          required(:current_function) => mfa() | :undefined,
          required(:dictionary_keys) => [term()],
          required(:message_queue_len) => non_neg_integer(),
          required(:heap_size) => non_neg_integer(),
          required(:total_heap_size) => non_neg_integer(),
          required(:stack_size) => non_neg_integer(),
          required(:reductions) => non_neg_integer()
        }

  @typedoc "Map keyed by pid for stable diffing across snapshots."
  @type snapshot :: %{required(pid()) => entry()}

  @doc """
  Take a per-process memory snapshot for every alive process on the
  calling node.
  """
  @spec snapshot() :: snapshot()
  def snapshot do
    for pid <- Process.list(), entry = entry_for(pid), entry != nil, into: %{} do
      {pid, entry}
    end
  end

  @doc """
  Compute the per-process delta between `before` and `after`. Returns a
  list of `{pid, delta_bytes, after_entry}` tuples sorted by delta
  descending — biggest growers first. Processes that didn't exist in
  `before` are reported with their full `after` memory as the delta.
  """
  @spec rank_growth(snapshot(), snapshot()) ::
          [{pid(), integer(), entry()}]
  def rank_growth(before, aft) do
    aft
    |> Enum.map(fn {pid, after_entry} ->
      before_mem =
        case Map.fetch(before, pid) do
          {:ok, %{memory: m}} -> m
          :error -> 0
        end

      {pid, after_entry.memory - before_mem, after_entry}
    end)
    |> Enum.sort_by(fn {_pid, delta, _} -> -delta end)
  end

  @doc """
  Format the top-N growers into a readable string for the test log.
  """
  @spec format_top(non_neg_integer(), [{pid(), integer(), entry()}]) :: iodata()
  def format_top(n, ranked) do
    ranked
    |> Enum.take(n)
    |> Enum.map_join("\n", fn {pid, delta, entry} ->
      "  #{format_bytes(delta)}\t#{inspect(pid)}\tname=#{inspect(entry.registered_name)}\tinit=#{format_mfa(entry.initial_call)}\tnow=#{format_mfa(entry.current_function)}\tdict=#{inspect(entry.dictionary_keys)}"
    end)
  end

  defp format_mfa({m, f, a}), do: "#{inspect(m)}.#{f}/#{a}"
  defp format_mfa(other), do: inspect(other)

  defp format_bytes(bytes) when bytes >= 1_048_576,
    do: "#{Float.round(bytes / 1_048_576, 2)} MiB"

  defp format_bytes(bytes) when bytes >= 1024, do: "#{Float.round(bytes / 1024, 1)} KiB"
  defp format_bytes(bytes), do: "#{bytes} B"

  defp entry_for(pid) do
    keys = [
      :memory,
      :initial_call,
      :registered_name,
      :current_function,
      :dictionary,
      :message_queue_len,
      :heap_size,
      :total_heap_size,
      :stack_size,
      :reductions
    ]

    case Process.info(pid, keys) do
      nil ->
        nil

      info ->
        %{
          pid: pid,
          memory: Keyword.fetch!(info, :memory),
          initial_call: Keyword.fetch!(info, :initial_call),
          registered_name: registered_name_or_undefined(Keyword.fetch!(info, :registered_name)),
          current_function: Keyword.fetch!(info, :current_function),
          dictionary_keys: dictionary_keys(Keyword.fetch!(info, :dictionary)),
          message_queue_len: Keyword.fetch!(info, :message_queue_len),
          heap_size: Keyword.fetch!(info, :heap_size),
          total_heap_size: Keyword.fetch!(info, :total_heap_size),
          stack_size: Keyword.fetch!(info, :stack_size),
          reductions: Keyword.fetch!(info, :reductions)
        }
    end
  end

  defp registered_name_or_undefined([]), do: :undefined
  defp registered_name_or_undefined(name) when is_atom(name), do: name

  defp dictionary_keys(dict) when is_list(dict) do
    Enum.map(dict, fn
      {k, _v} -> k
      other -> other
    end)
  end

  @doc """
  Drive a streaming write through the given upload path
  (`{ChunkWriter, :write}` or `{S3, :put_object_stream}`) and return a
  per-process memory diagnostic.

  The `path` argument tells this helper which entry point to drive:

    * `{:chunk_writer, [target_node: node, ...]}` — call
      `NeonFS.Client.ChunkWriter.write_file_stream/4` directly.
    * `{:s3, [ctx: ctx]}` — call `NeonFS.S3.Backend.put_object_stream/5`.

  This split lets us rule out / blame the layer above ChunkWriter when
  hunting per-chunk process-heap growth.

  Stream construction lives here because anonymous-function streams
  don't survive RPC boundaries — the peer constructs the stream from
  a binary seed plus integer total size and consumes it locally.

  Returns a map with `:refs`, `:total_delta`, `:processes_used_delta`,
  `:peak_processes_used_delta` (sampled every 5 ms during the upload),
  and `:top_growers` (formatted text).
  """
  @spec run_streaming_profile(
          volume_name :: String.t(),
          path_choice :: {:chunk_writer, keyword()} | {:s3, keyword()},
          seed :: binary(),
          upload_size :: non_neg_integer()
        ) :: map()
  def run_streaming_profile(volume_name, path_choice, seed, upload_size) do
    Enum.each(Process.list(), &:erlang.garbage_collect/1)

    before_total = :erlang.memory()
    before_processes = snapshot()

    sampler =
      spawn(fn ->
        sample_loop(
          before_total[:processes_used],
          before_processes,
          before_total[:processes_used]
        )
      end)

    stream = seeded_stream(seed, upload_size)
    path = "/profile/upload-#{System.unique_integer([:positive])}.bin"

    {result, refs_count} =
      case path_choice do
        {:chunk_writer, opts} ->
          {:ok, refs} = ChunkWriter.write_file_stream(volume_name, path, stream, opts)
          {:ok, length(refs)}

        {:s3, opts} ->
          ctx = Keyword.fetch!(opts, :ctx)

          put_opts = %Firkin.PutOpts{
            content_type: "application/octet-stream",
            metadata: %{}
          }

          {:ok, _etag} =
            S3Backend.put_object_stream(ctx, volume_name, Path.basename(path), stream, put_opts)

          # Approximate ref count from upload size for reporting.
          {:ok, div(upload_size, 256 * 1024)}
      end

    {:ok, ^result} = {result, result}

    {peak_processes_used, peak_processes} = stop_sampler(sampler)

    after_total = :erlang.memory()

    growers = rank_growth(before_processes, peak_processes)
    top_text = format_top(15, growers)

    %{
      refs: refs_count,
      total_delta: after_total[:total] - before_total[:total],
      processes_used_delta: after_total[:processes_used] - before_total[:processes_used],
      peak_processes_used_delta: peak_processes_used - before_total[:processes_used],
      top_growers: IO.iodata_to_binary(top_text)
    }
  end

  defp sample_loop(peak, peak_snapshot, _last_seen) do
    receive do
      {:report, reply_to} ->
        send(reply_to, {:peak, peak, peak_snapshot})
    after
      5 ->
        cur = :erlang.memory(:processes_used)

        {next_peak, next_snapshot} =
          if cur > peak do
            {cur, snapshot()}
          else
            {peak, peak_snapshot}
          end

        sample_loop(next_peak, next_snapshot, cur)
    end
  end

  defp stop_sampler(pid) do
    send(pid, {:report, self()})

    receive do
      {:peak, peak, snap} -> {peak, snap}
    after
      5_000 -> raise "sampler timeout"
    end
  end

  defp seeded_stream(seed, total_size) do
    seed_size = byte_size(seed)
    full = div(total_size, seed_size)

    Stream.unfold({0, full}, fn
      {n, n} -> nil
      {i, n} when i < n -> {seed, {i + 1, n}}
    end)
  end
end
