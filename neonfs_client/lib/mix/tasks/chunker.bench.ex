defmodule Mix.Tasks.Chunker.Bench do
  @moduledoc """
  Throughput benchmark comparing `NeonFS.Client.Chunker.Native`
  (Rustler NIF over the `fastcdc` crate) against
  `NeonFS.Client.Chunker.Pure` (the pure-Elixir port from #797).

  Reports MB/s and per-chunk overhead at the input sizes mentioned in
  the issue's decision criterion (small ≤ 64 KiB, bulk ≥ 1 MiB).

      $ mix chunker.bench
      $ mix chunker.bench --sizes 65536,262144,1048576

  The deliverable for #797 is the comparison; the issue accepts any
  ratio above ~30% bulk + faster-on-small as the threshold for
  considering a switch.
  """

  use Mix.Task

  alias NeonFS.Client.Chunker.{Native, Pure}

  @default_sizes [65_536, 262_144, 1_048_576, 4_194_304]
  @default_iterations 5

  @impl Mix.Task
  def run(argv) do
    {opts, _, _} =
      OptionParser.parse(argv,
        switches: [sizes: :string, iterations: :integer]
      )

    sizes = parse_sizes(opts[:sizes]) || @default_sizes
    iterations = opts[:iterations] || @default_iterations

    Mix.shell().info("FastCDC chunker benchmark")
    Mix.shell().info("avg=256KiB, min=64KiB, max=1MiB, iterations=#{iterations}\n")

    Mix.shell().info(
      String.pad_trailing("size", 12) <>
        String.pad_trailing("native MB/s", 16) <>
        String.pad_trailing("pure MB/s", 16) <>
        String.pad_trailing("ratio", 10) <>
        "chunks"
    )

    Mix.shell().info(String.duplicate("-", 70))

    for size <- sizes do
      data = :crypto.strong_rand_bytes(size)
      opts = chunker_opts_for(size)

      native_us = bench(fn -> chunk_native(data, opts) end, iterations)
      pure_us = bench(fn -> chunk_pure(data, opts) end, iterations)

      native_mb_s = mb_per_sec(size, native_us)
      pure_mb_s = mb_per_sec(size, pure_us)
      ratio = if native_us > 0 and pure_us > 0, do: native_us / pure_us, else: nil

      chunks = Pure.chunks(data, opts) |> length()

      Mix.shell().info(
        String.pad_trailing(human_size(size), 12) <>
          String.pad_trailing(format_throughput(native_mb_s), 16) <>
          String.pad_trailing(format_throughput(pure_mb_s), 16) <>
          String.pad_trailing(format_ratio(ratio), 10) <>
          "#{chunks}"
      )
    end

    Mix.shell().info("")

    Mix.shell().info(
      "ratio = pure-Elixir time / native time (pure is N× slower); 1.00x means parity."
    )
  end

  defp parse_sizes(nil), do: nil

  defp parse_sizes(str) do
    str |> String.split(",", trim: true) |> Enum.map(&String.to_integer/1)
  end

  defp chunker_opts_for(size) do
    # Mirror the production thresholds in `chunking::auto_strategy`.
    cond do
      size < 64 * 1024 -> %{min: 64, avg: 1024, max: 4096}
      size < 1024 * 1024 -> %{min: 64 * 1024, avg: 256 * 1024, max: 1024 * 1024}
      true -> %{min: 64 * 1024, avg: 256 * 1024, max: 1024 * 1024}
    end
  end

  defp chunk_native(data, opts) do
    {:ok, _} = Native.chunk_data(data, "fastcdc", opts.avg)
  end

  defp chunk_pure(data, opts) do
    Pure.chunks(data, opts)
  end

  defp bench(fun, iterations) do
    # Warm-up.
    fun.()

    1..iterations
    |> Enum.map(fn _ -> :timer.tc(fun) |> elem(0) end)
    |> Enum.min()
  end

  defp mb_per_sec(_size_bytes, 0), do: :infinity

  defp mb_per_sec(size_bytes, us) do
    seconds = us / 1_000_000
    mb = size_bytes / (1024 * 1024)
    mb / seconds
  end

  defp human_size(n) when n >= 1_048_576, do: "#{div(n, 1_048_576)} MiB"
  defp human_size(n) when n >= 1024, do: "#{div(n, 1024)} KiB"
  defp human_size(n), do: "#{n} B"

  defp format_throughput(:infinity), do: ">>"
  defp format_throughput(mb), do: :erlang.float_to_binary(mb, decimals: 1)

  defp format_ratio(nil), do: "—"

  defp format_ratio(ratio) when is_float(ratio) and ratio > 0,
    do: "#{:erlang.float_to_binary(1 / ratio, decimals: 2)}x"
end
