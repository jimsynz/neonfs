defmodule NeonFS.Docker.OptsParser do
  @moduledoc """
  Parse and validate `docker volume create -o key=value,...` opts into
  the typed keyword list `NeonFS.Core.Volume.new/2` expects.

  Docker passes `-o` as a JSON map of stringly-typed values; threading
  that map straight through to `Volume.new/2` crashed
  `Keyword.get(%{}, :owner, nil)` (the original bug from #583). This
  module bridges the wire shape to the typed shape:

    * Whitelists known keys against `@safe_keys` so untrusted user
      input can't blow the atom table — `String.to_existing_atom/1`
      only ever runs against pre-allocated, whitelisted keys.
    * Coerces values per-key (`atime_mode` → atom, `io_weight` →
      positive integer, `durability` → replicated config map).
    * Rejects unknown keys / malformed values with a clear `Err`
      message rather than silently dropping them.

  Supported keys (more can be added as needed; out of scope for the
  initial cut: `tiering`, `caching`, `compression`, `verification`,
  `encryption` — they're complex nested configs that don't map
  cleanly to `key=value` strings):

    * `owner` — string, free-form.
    * `atime_mode` — `"noatime"` or `"relatime"`.
    * `write_ack` — `"local"`, `"quorum"`, or `"all"`.
    * `io_weight` — positive integer (string-encoded on the wire).
    * `durability` — positive integer N → replicated with factor N
      and `min_copies = ⌈N/2⌉` (majority quorum), matching the shape
      of `Volume.default_durability/0` at factor 3.
  """

  @safe_keys ~w[owner atime_mode write_ack io_weight durability]

  @doc """
  Parse a docker `Opts` map into a keyword list suitable for
  `NeonFS.Core.Volume.new/2`. Empty input parses to an empty list
  (the equivalent of "use Volume defaults").
  """
  @spec parse(map()) :: {:ok, keyword()} | {:error, String.t()}
  def parse(opts) when is_map(opts) do
    opts
    |> Enum.reduce_while({:ok, []}, &reduce_pair/2)
    |> case do
      {:ok, pairs} -> {:ok, Enum.reverse(pairs)}
      {:error, _} = err -> err
    end
  end

  defp reduce_pair({key, value}, {:ok, acc}) when key in @safe_keys do
    case coerce(key, value) do
      {:ok, pair} -> {:cont, {:ok, [pair | acc]}}
      {:error, _} = err -> {:halt, err}
    end
  end

  defp reduce_pair({key, _value}, {:ok, _acc}) do
    {:halt, {:error, "unknown docker volume opt: #{key}"}}
  end

  defp coerce("owner", v) when is_binary(v), do: {:ok, {:owner, v}}
  defp coerce("owner", v), do: {:error, "owner must be a string (got #{inspect(v)})"}

  defp coerce("atime_mode", v) when v in ["noatime", "relatime"],
    do: {:ok, {:atime_mode, String.to_existing_atom(v)}}

  defp coerce("atime_mode", v),
    do: {:error, ~s|atime_mode must be "noatime" or "relatime" (got #{inspect(v)})|}

  defp coerce("write_ack", v) when v in ["local", "quorum", "all"],
    do: {:ok, {:write_ack, String.to_existing_atom(v)}}

  defp coerce("write_ack", v),
    do: {:error, ~s|write_ack must be "local", "quorum", or "all" (got #{inspect(v)})|}

  defp coerce("io_weight", v) when is_binary(v) do
    case Integer.parse(v) do
      {n, ""} when n > 0 -> {:ok, {:io_weight, n}}
      _ -> {:error, "io_weight must be a positive integer (got #{inspect(v)})"}
    end
  end

  defp coerce("io_weight", v),
    do: {:error, "io_weight must be a string-encoded positive integer (got #{inspect(v)})"}

  defp coerce("durability", v) when is_binary(v) do
    case Integer.parse(v) do
      {n, ""} when n >= 1 ->
        min_copies = max(1, div(n + 1, 2))
        {:ok, {:durability, %{type: :replicate, factor: n, min_copies: min_copies}}}

      _ ->
        {:error, "durability must be a positive integer (got #{inspect(v)})"}
    end
  end

  defp coerce("durability", v),
    do: {:error, "durability must be a string-encoded positive integer (got #{inspect(v)})"}
end
