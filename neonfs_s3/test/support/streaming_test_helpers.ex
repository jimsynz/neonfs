defmodule NeonFS.S3.IntegrationTest.StreamingHelpers do
  @moduledoc """
  Helpers for the streaming-upload peak-RSS integration test (#499).

  These functions are invoked on peer nodes via `PeerCluster.rpc/4`, so
  they must live in `lib/` (not `test/`) to be loadable on a peer's
  code path. The interesting bits:

    * `seeded_stream/2` builds a deterministic byte stream from a seed
      block by repeating it; large totals never materialise as a single
      binary, so a peer's heap doesn't blow up on construction.
    * `seeded_stream_hash/2` computes the SHA-256 of the same stream
      without producing any new bytes, so the test driver can compare
      against the receiver's hash.
    * `start_memory_sampler/2` / `stop_memory_sampler/1` sample
      `:erlang.memory()` periodically and track the breakdown captured
      at the moment `:processes_used` peaks. See the function docs for
      why `:processes_used` rather than `:total`.
    * `consume_and_hash/1` drains a read-side stream (`Enumerable.t()`
      of binary chunks) into a SHA-256, never accumulating a flat
      binary larger than one chunk.
    * `s3_put_with_sampling/6` and `webdav_put_with_sampling/6`
      orchestrate "start sampler → run upload → stop sampler" inside a
      single peer-side rpc, returning the upload result alongside the
      baseline / peak `:erlang.memory()` breakdowns and the SHA-256 of
      the bytes the stream emitted.
    * `gc_everything/0` triggers a major GC across every process on
      the calling node so the test driver can take a clean baseline.
    * `s3_get_and_hash/3` / `webdav_get_and_hash/2` drive a streaming
      read of an existing object/resource and return its SHA-256 for
      cross-interface byte-identity verification.
  """

  alias NeonFS.S3.Backend, as: S3Backend
  alias NeonFS.WebDAV.Backend, as: WebDAVBackend

  @doc """
  Force a major garbage collect across every process on the calling
  node. Useful before taking a memory baseline so the snapshot
  reflects the steady state, not whatever transient binaries the
  previous activity left lying around.
  """
  @spec gc_everything() :: :ok
  def gc_everything do
    Enum.each(Process.list(), &:erlang.garbage_collect/1)
    :ok
  end

  @typedoc "Result tuple returned by the `*_put_with_sampling/6` helpers."
  @type sampling_result(reply) :: %{
          required(:reply) => reply,
          required(:baseline) => non_neg_integer(),
          required(:peak) => non_neg_integer(),
          required(:baseline_breakdown) => keyword(),
          required(:peak_breakdown) => keyword(),
          required(:upload_hash) => binary()
        }

  @doc """
  Build a deterministic stream of `total_size` bytes by repeating
  `seed`. The final element is sliced to make the total exact when
  `total_size` is not a multiple of `byte_size(seed)`.

  The stream emits binary chunks of `byte_size(seed)` bytes (plus one
  shorter tail chunk where applicable). Memory cost on construction is
  one reference to `seed` — the bytes are not duplicated until a
  consumer actually pulls them.
  """
  @spec seeded_stream(seed :: binary(), total_size :: non_neg_integer()) :: Enumerable.t()
  def seeded_stream(seed, total_size)
      when is_binary(seed) and byte_size(seed) > 0 and is_integer(total_size) and
             total_size >= 0 do
    seed_size = byte_size(seed)
    full_repeats = div(total_size, seed_size)
    tail_size = rem(total_size, seed_size)

    Stream.unfold({0, full_repeats, tail_size}, fn
      {n, n, 0} ->
        nil

      {n, n, tail} when tail > 0 ->
        {binary_part(seed, 0, tail), {n, n, 0}}

      {i, full, tail} when i < full ->
        {seed, {i + 1, full, tail}}
    end)
  end

  @doc """
  SHA-256 of the same stream `seeded_stream/2` would produce, computed
  without iterating Erlang processes worth of binary copies. Useful as
  the "expected" hash on the upload side so the test can compare
  against the read-side hash.
  """
  @spec seeded_stream_hash(seed :: binary(), total_size :: non_neg_integer()) :: binary()
  def seeded_stream_hash(seed, total_size) do
    seed
    |> seeded_stream(total_size)
    |> consume_and_hash()
  end

  @doc """
  Drain an enumerable of binary chunks into a SHA-256 hash. Used on
  the read side to verify byte-identity without accumulating the full
  payload in memory.
  """
  @spec consume_and_hash(Enumerable.t()) :: binary()
  def consume_and_hash(stream) do
    stream
    |> Enum.reduce(:crypto.hash_init(:sha256), fn chunk, acc ->
      :crypto.hash_update(acc, chunk)
    end)
    |> :crypto.hash_final()
  end

  @doc """
  Start a background process that samples `:erlang.memory()` every
  `interval_ms` milliseconds and tracks the breakdown at the moment
  `:processes_used` peaks.

  Why `:processes_used` rather than `:total`? `:total` is dominated by
  the **binary heap** — refc binaries created by the chunker NIF and
  the TLS framing layer take a global GC sweep to be released, and
  under a tight upload loop the major-GC tick is what bounds
  `memory(:binary)`, not the streaming pipeline's working set. The
  pipeline itself only ever holds one or two chunks at a time;
  `:processes_used` captures that working set faithfully (it counts
  the bytes actively used in process heaps, excluding refc binaries
  on the global heap and excluding allocated-but-free space). Binary
  heap growth is reported alongside in the breakdown so a regression
  that genuinely retains chunk references (rather than just deferring
  GC) still surfaces.

  The sampler is **unlinked** from the caller, since the canonical
  caller in this codebase is a `:rpc.call`-driven transient that exits
  the moment this function returns. As a safety net, the sampler
  self-terminates after `max_window_ms` even if `stop_memory_sampler/1`
  is never called — picks up the slack when a test crashes between
  start and stop.

  Returns the sampler's PID. The PID is serialisable across nodes, so
  the test driver can pass it back into `stop_memory_sampler/1` from
  any node.
  """
  @spec start_memory_sampler(pos_integer(), pos_integer()) :: pid()
  def start_memory_sampler(interval_ms, max_window_ms \\ 600_000)
      when is_integer(interval_ms) and interval_ms > 0 and is_integer(max_window_ms) and
             max_window_ms > 0 do
    deadline = System.monotonic_time(:millisecond) + max_window_ms
    spawn(fn -> sample_loop(0, [], interval_ms, deadline) end)
  end

  @doc """
  Ask `pid` for its peak observation and return `{peak_total, peak_breakdown}`.
  Sends a `:report` message and waits up to 5_000 ms for the reply;
  raises on timeout.

  Cross-node `send/2` is fine — Erlang distribution serialises the
  PID, and the sampler's mailbox is on its own node.
  """
  @spec stop_memory_sampler(pid()) :: {non_neg_integer(), keyword()}
  def stop_memory_sampler(pid) when is_pid(pid) do
    send(pid, {:report, self()})

    receive do
      {:peak, peak, breakdown} -> {peak, breakdown}
    after
      5_000 -> raise "memory sampler #{inspect(pid)} did not respond within 5_000 ms"
    end
  end

  @doc """
  Run an S3 upload through `NeonFS.S3.Backend.put_object_stream/5` with
  background memory sampling. Returns the upload reply plus the
  baseline / peak `:erlang.memory(:total)` snapshots and the SHA-256
  of the bytes the stream emitted.
  """
  @spec s3_put_with_sampling(
          ctx :: map(),
          bucket :: binary(),
          key :: binary(),
          seed :: binary(),
          total_size :: non_neg_integer(),
          opts :: keyword() | map()
        ) :: sampling_result({:ok, binary()} | {:error, term()})
  def s3_put_with_sampling(ctx, bucket, key, seed, total_size, opts) do
    do_put_with_sampling(seed, total_size, fn stream ->
      S3Backend.put_object_stream(ctx, bucket, key, stream, normalise_s3_opts(opts))
    end)
  end

  @doc """
  Run a WebDAV upload through
  `NeonFS.WebDAV.Backend.put_content_stream/4` with background memory
  sampling. `path_segments` is the WebDAV path split into list form
  (`[volume_name | rest]`).
  """
  @spec webdav_put_with_sampling(
          auth :: term(),
          path_segments :: [binary()],
          seed :: binary(),
          total_size :: non_neg_integer(),
          opts :: map(),
          extra :: keyword()
        ) :: sampling_result({:ok, term()} | {:error, term()})
  def webdav_put_with_sampling(auth, path_segments, seed, total_size, opts, _extra \\ []) do
    do_put_with_sampling(seed, total_size, fn stream ->
      WebDAVBackend.put_content_stream(auth, path_segments, stream, opts)
    end)
  end

  @doc """
  Drive an S3 readback via `get_object/4`, hashing the bytes as they
  arrive without accumulating the full body. Returns the SHA-256.
  """
  @spec s3_get_and_hash(ctx :: map(), bucket :: binary(), key :: binary()) ::
          {:ok, binary()} | {:error, term()}
  def s3_get_and_hash(ctx, bucket, key) do
    case S3Backend.get_object(ctx, bucket, key, %{range: nil}) do
      {:ok, %{body: body}} -> {:ok, consume_and_hash(body)}
      {:error, _} = err -> err
    end
  end

  @doc """
  Drive a WebDAV readback via `resolve/2` + `get_content/3`, hashing
  the chunks as they arrive. Returns the SHA-256.
  """
  @spec webdav_get_and_hash(auth :: term(), path_segments :: [binary()]) ::
          {:ok, binary()} | {:error, term()}
  def webdav_get_and_hash(auth, path_segments) do
    with {:ok, resource} <- WebDAVBackend.resolve(auth, path_segments),
         {:ok, stream} <- WebDAVBackend.get_content(auth, resource, %{}) do
      {:ok, consume_and_hash(stream)}
    end
  end

  # ——— Internals ————————————————————————————————————————————————

  defp do_put_with_sampling(seed, total_size, upload_fn) do
    upload_hash = seeded_stream_hash(seed, total_size)

    Enum.each(Process.list(), &:erlang.garbage_collect/1)

    baseline_breakdown = :erlang.memory()
    baseline = baseline_breakdown[:processes_used]
    sampler = start_memory_sampler(5)

    reply = upload_fn.(seeded_stream(seed, total_size))
    {peak, peak_breakdown} = stop_memory_sampler(sampler)

    %{
      reply: reply,
      baseline: baseline,
      peak: peak,
      baseline_breakdown: baseline_breakdown,
      peak_breakdown: peak_breakdown,
      upload_hash: upload_hash
    }
  end

  defp normalise_s3_opts(%Firkin.PutOpts{} = opts), do: opts

  defp normalise_s3_opts(%{} = m) do
    %Firkin.PutOpts{
      content_type: Map.get(m, :content_type, "application/octet-stream"),
      metadata: Map.get(m, :metadata, %{})
    }
  end

  defp normalise_s3_opts([]) do
    %Firkin.PutOpts{content_type: "application/octet-stream", metadata: %{}}
  end

  defp sample_loop(peak, peak_breakdown, interval_ms, deadline) do
    now = System.monotonic_time(:millisecond)
    remaining = deadline - now

    if remaining <= 0 do
      # Self-terminate so a forgotten sampler can't leak forever.
      :ok
    else
      wait = min(interval_ms, remaining)

      receive do
        {:report, reply_to} ->
          send(reply_to, {:peak, peak, peak_breakdown})
      after
        wait ->
          breakdown = :erlang.memory()
          metric = breakdown[:processes_used]

          {next_peak, next_breakdown} =
            if metric > peak do
              {metric, breakdown}
            else
              {peak, peak_breakdown}
            end

          sample_loop(next_peak, next_breakdown, interval_ms, deadline)
      end
    end
  end
end
