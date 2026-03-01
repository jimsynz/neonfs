defmodule NeonFS.IO.DriveStrategy do
  @moduledoc """
  Behaviour for drive-type-specific I/O scheduling strategies.

  Different storage media require different I/O patterns:
  - **HDD**: batch reads and writes separately, sort by chunk hash prefix to
    minimise seeking (elevator scheduling)
  - **SSD**: parallel I/O with no seek penalty, simple FIFO ordering,
    interleave reads and writes freely

  Each strategy maintains an internal queue and returns batches of operations
  when requested. The drive worker consumer calls `next_batch/2` to pull
  operations in the optimal order for the underlying media.
  """

  alias NeonFS.IO.Operation

  @type state :: term()

  @doc """
  Initialises the strategy with drive-specific configuration.

  Returns an opaque state term used by subsequent calls.
  """
  @callback init(config :: keyword()) :: state()

  @doc """
  Adds an operation to the strategy's internal queue.

  Returns updated state.
  """
  @callback enqueue(state(), Operation.t()) :: state()

  @doc """
  Returns up to `count` operations to execute, ordered by the strategy's policy.

  Returns `{operations, updated_state}`.
  """
  @callback next_batch(state(), count :: pos_integer()) :: {[Operation.t()], state()}

  @doc """
  Returns the drive type this strategy is designed for.
  """
  @callback type() :: :hdd | :ssd

  @doc """
  Detects whether the drive at `path` is an HDD or SSD.

  Reads `/sys/block/<device>/queue/rotational` on Linux.
  Falls back to `fallback` (default `:ssd`) when sysfs is unavailable.
  """
  @spec detect(String.t(), keyword()) :: :hdd | :ssd
  def detect(path, opts \\ []) do
    fallback = Keyword.get(opts, :fallback, :ssd)
    sysfs_root = Keyword.get(opts, :sysfs_root, "/sys/block")

    case resolve_block_device(path) do
      {:ok, device} ->
        rotational_path = Path.join([sysfs_root, device, "queue", "rotational"])
        read_rotational(rotational_path, fallback)

      :error ->
        fallback
    end
  end

  defp read_rotational(path, fallback) do
    case File.read(path) do
      {:ok, content} ->
        case String.trim(content) do
          "1" -> :hdd
          "0" -> :ssd
          _ -> fallback
        end

      {:error, _} ->
        fallback
    end
  end

  defp resolve_block_device(path) do
    case System.cmd("df", [path], stderr_to_stdout: true) do
      {output, 0} ->
        output
        |> String.split("\n")
        |> Enum.at(1, "")
        |> String.split()
        |> List.first("")
        |> extract_device_name()

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  defp extract_device_name(device_path) do
    name =
      device_path
      |> Path.basename()
      |> String.replace(~r/[0-9]+$/, "")

    if name == "" do
      :error
    else
      {:ok, name}
    end
  end
end
