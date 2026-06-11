defmodule NeonFS.Core.DriveConfig do
  @moduledoc """
  Parsing and validation for drive configuration.

  Supports human-readable capacity suffixes (`M`, `G`, `T`) using IEC binary
  units (powers of 1024), and validates configured capacities against actual
  partition sizes at startup.
  """

  alias NeonFS.Core.Blob.Native

  require Logger

  @kib 1_024
  @mib @kib * 1_024
  @gib @mib * 1_024
  @tib @gib * 1_024

  @doc """
  Parses a capacity string into bytes.

  Accepts raw integers or values with a suffix:

    * `M` — mebibytes (value * 1024^2)
    * `G` — gibibytes (value * 1024^3)
    * `T` — tebibytes (value * 1024^4)

  Suffixes are case-insensitive. The numeric part may be an integer or float.

  ## Examples

      iex> NeonFS.Core.DriveConfig.parse_capacity("1000000")
      {:ok, 1_000_000}

      iex> NeonFS.Core.DriveConfig.parse_capacity("100M")
      {:ok, 104_857_600}

      iex> NeonFS.Core.DriveConfig.parse_capacity("1G")
      {:ok, 1_073_741_824}

      iex> NeonFS.Core.DriveConfig.parse_capacity("2T")
      {:ok, 2_199_023_255_552}

      iex> NeonFS.Core.DriveConfig.parse_capacity("0")
      {:ok, 0}

      iex> NeonFS.Core.DriveConfig.parse_capacity("1.5G")
      {:ok, 1_610_612_736}

  """
  @spec parse_capacity(String.t()) :: {:ok, non_neg_integer()} | {:error, String.t()}
  defdelegate parse_capacity(string), to: NeonFS.Capacity, as: :parse

  @doc """
  Like `parse_capacity/1` but raises on invalid input.
  """
  @spec parse_capacity!(String.t()) :: non_neg_integer()
  defdelegate parse_capacity!(string), to: NeonFS.Capacity, as: :parse!

  @doc """
  Validates that configured drive capacities don't exceed actual partition sizes.

  For each drive with a non-zero capacity, queries the filesystem via the
  `filesystem_info` NIF. Logs a warning if the configured capacity exceeds the
  partition's total size. Non-fatal — the node will still start.

  ## Parameters
    - `drives` — list of `NeonFS.Core.Drive.t()` structs
  """
  @spec validate_drives([NeonFS.Core.Drive.t()]) :: :ok
  def validate_drives(drives) do
    Enum.each(drives, &validate_drive_capacity/1)
  end

  @doc """
  Detects the filesystem capacity for a drive with `capacity_bytes: 0`.

  Queries the filesystem via the `filesystem_info` NIF and sets `capacity_bytes`
  to the partition's total size. No-op when capacity is already non-zero.
  Returns the drive unchanged on filesystem error (with a warning log).

  ## Parameters
    - `drive` — a `NeonFS.Core.Drive.t()` struct
  """
  @spec detect_capacity(NeonFS.Core.Drive.t()) :: NeonFS.Core.Drive.t()
  def detect_capacity(%{capacity_bytes: capacity} = drive) when capacity > 0, do: drive

  def detect_capacity(%{path: path, id: id} = drive) do
    case Native.filesystem_info(path) do
      {:ok, {total_bytes, _available, _used}} ->
        Logger.info("Auto-detected drive capacity from filesystem",
          drive_id: id,
          detected_capacity: format_bytes(total_bytes),
          path: path
        )

        %{drive | capacity_bytes: total_bytes}

      {:error, reason} ->
        Logger.warning("Could not detect drive capacity from filesystem",
          drive_id: id,
          path: path,
          reason: reason
        )

        drive
    end
  end

  ## Private

  defp validate_drive_capacity(%{capacity_bytes: 0}), do: :ok

  defp validate_drive_capacity(%{capacity_bytes: capacity, path: path, id: id}) do
    case Native.filesystem_info(path) do
      {:ok, {total_bytes, _available, _used}} when capacity > total_bytes ->
        Logger.warning(
          "Drive configured capacity exceeds partition total",
          drive_id: id,
          configured_capacity: format_bytes(capacity),
          partition_total: format_bytes(total_bytes),
          path: path
        )

      {:ok, _info} ->
        :ok

      {:error, reason} ->
        Logger.debug("Could not query filesystem for drive",
          drive_id: id,
          path: path,
          reason: reason
        )
    end
  end

  defp format_bytes(bytes) when bytes >= @tib do
    "#{Float.round(bytes / @tib, 1)}T"
  end

  defp format_bytes(bytes) when bytes >= @gib do
    "#{Float.round(bytes / @gib, 1)}G"
  end

  defp format_bytes(bytes) when bytes >= @mib do
    "#{Float.round(bytes / @mib, 1)}M"
  end

  defp format_bytes(bytes), do: "#{bytes} bytes"
end
