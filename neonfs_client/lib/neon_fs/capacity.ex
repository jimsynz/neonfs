defmodule NeonFS.Capacity do
  @moduledoc """
  Parsing of human-readable capacity strings ("100M", "1.5G", "2T",
  or plain byte counts).

  Shared by core's drive configuration (`NeonFS.Core.DriveConfig`
  delegates here) and the cluster-state validator, which lives in
  `neonfs_client` so any node type can persist `cluster.json` (#1160).
  """

  @mib 1024 * 1024
  @gib 1024 * @mib
  @tib 1024 * @gib

  @doc """
  Parses a capacity string into bytes.

  ## Examples

      iex> NeonFS.Capacity.parse("1000000")
      {:ok, 1_000_000}

      iex> NeonFS.Capacity.parse("100M")
      {:ok, 104_857_600}

      iex> NeonFS.Capacity.parse("1G")
      {:ok, 1_073_741_824}

      iex> NeonFS.Capacity.parse("2T")
      {:ok, 2_199_023_255_552}

      iex> NeonFS.Capacity.parse("0")
      {:ok, 0}

      iex> NeonFS.Capacity.parse("1.5G")
      {:ok, 1_610_612_736}

  """
  @spec parse(String.t()) :: {:ok, non_neg_integer()} | {:error, String.t()}
  def parse(string) when is_binary(string) do
    string = String.trim(string)

    case parse_parts(string) do
      {:ok, bytes} when bytes >= 0 ->
        {:ok, bytes}

      {:ok, _negative} ->
        {:error, "capacity must not be negative: #{inspect(string)}"}

      :error ->
        {:error, "invalid capacity format: #{inspect(string)}"}
    end
  end

  @doc """
  Like `parse/1` but raises on invalid input.
  """
  @spec parse!(String.t()) :: non_neg_integer()
  def parse!(string) do
    case parse(string) do
      {:ok, bytes} -> bytes
      {:error, reason} -> raise ArgumentError, reason
    end
  end

  defp parse_parts(string) do
    case Integer.parse(string) do
      {value, ""} ->
        {:ok, value}

      {_value, _rest} ->
        parse_with_suffix(string)

      :error ->
        parse_with_suffix(string)
    end
  end

  defp parse_with_suffix(string) do
    suffix_pattern = ~r/\A([0-9]+(?:\.[0-9]+)?)\s*([mMgGtT])\z/

    case Regex.run(suffix_pattern, string) do
      [_, number_str, suffix] ->
        multiplier = suffix_multiplier(String.downcase(suffix))

        case Float.parse(number_str) do
          {number, ""} -> {:ok, trunc(number * multiplier)}
          _ -> :error
        end

      nil ->
        :error
    end
  end

  defp suffix_multiplier("m"), do: @mib
  defp suffix_multiplier("g"), do: @gib
  defp suffix_multiplier("t"), do: @tib
end
