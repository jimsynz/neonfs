defmodule NeonFS.Core.StorageMetrics do
  @moduledoc """
  Telemetry-driven per-drive storage usage tracking.

  Subscribes to BlobStore write and delete telemetry events to maintain live
  per-drive `used_bytes` counters. On init, computes initial usage by scanning
  `ChunkIndex.list_all/0` and pushing results to `DriveRegistry`.

  ## Query API

  - `cluster_capacity/0` — aggregate per-drive and cluster-wide stats
  - `drive_capacity/2` — stats for a single drive
  - `available_capacity_for_tier/2` — available bytes in a tier (excluding specified drives)
  - `available_capacity_any_tier/1` — available bytes across all tiers
  - `redundant_bytes_on_drive/2` — bytes that can be pruned (over-replicated chunks)
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{ChunkIndex, DriveRegistry, FileIndex, VolumeRegistry}

  ## Client API

  @doc """
  Starts the StorageMetrics GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns cluster-wide capacity information with per-drive breakdown.
  """
  @spec cluster_capacity() :: map()
  def cluster_capacity do
    drives =
      DriveRegistry.list_drives()
      |> Enum.map(fn drive ->
        available =
          if drive.capacity_bytes == 0,
            do: :unlimited,
            else: max(drive.capacity_bytes - drive.used_bytes, 0)

        %{
          node: drive.node,
          drive_id: drive.id,
          tier: drive.tier,
          capacity_bytes: drive.capacity_bytes,
          used_bytes: drive.used_bytes,
          available_bytes: available,
          state: drive.state
        }
      end)

    {total_cap, total_used, total_avail} =
      Enum.reduce(drives, {0, 0, 0}, fn drive, {cap, used, avail} ->
        if drive.capacity_bytes == 0 or drive.available_bytes == :unlimited do
          {cap, used + drive.used_bytes, avail}
        else
          {cap + drive.capacity_bytes, used + drive.used_bytes, avail + drive.available_bytes}
        end
      end)

    has_unlimited = Enum.any?(drives, &(&1.capacity_bytes == 0))

    %{
      drives: drives,
      total_capacity: if(has_unlimited, do: :unlimited, else: total_cap),
      total_used: total_used,
      total_available: if(has_unlimited, do: :unlimited, else: total_avail)
    }
  end

  @doc """
  Returns capacity info for a single drive.
  """
  @spec drive_capacity(node(), String.t()) :: {:ok, map()} | {:error, :not_found}
  def drive_capacity(node, drive_id) do
    case DriveRegistry.get_drive(node, drive_id) do
      {:ok, drive} ->
        available =
          if drive.capacity_bytes == 0,
            do: :unlimited,
            else: max(drive.capacity_bytes - drive.used_bytes, 0)

        {:ok,
         %{
           node: drive.node,
           drive_id: drive.id,
           tier: drive.tier,
           capacity_bytes: drive.capacity_bytes,
           used_bytes: drive.used_bytes,
           available_bytes: available,
           state: drive.state
         }}

      error ->
        error
    end
  end

  @doc """
  Returns available capacity across drives in a given tier, excluding specified drives.

  Returns `:unlimited` if any non-excluded drive in the tier has `capacity_bytes == 0`.
  """
  @spec available_capacity_for_tier(atom(), [{node(), String.t()}]) ::
          non_neg_integer() | :unlimited
  def available_capacity_for_tier(tier, exclude_drives \\ []) do
    drives =
      DriveRegistry.list_drives()
      |> Enum.filter(fn d ->
        d.tier == tier and d.state != :draining and
          {d.node, d.id} not in exclude_drives
      end)

    if Enum.any?(drives, &(&1.capacity_bytes == 0)) do
      :unlimited
    else
      Enum.reduce(drives, 0, fn d, acc ->
        acc + max(d.capacity_bytes - d.used_bytes, 0)
      end)
    end
  end

  @doc """
  Returns available capacity across all drives in the cluster, excluding specified drives.

  Returns `:unlimited` if any non-excluded drive has `capacity_bytes == 0`.
  """
  @spec available_capacity_any_tier([{node(), String.t()}]) :: non_neg_integer() | :unlimited
  def available_capacity_any_tier(exclude_drives \\ []) do
    drives =
      DriveRegistry.list_drives()
      |> Enum.filter(fn d ->
        d.state != :draining and {d.node, d.id} not in exclude_drives
      end)

    if Enum.any?(drives, &(&1.capacity_bytes == 0)) do
      :unlimited
    else
      Enum.reduce(drives, 0, fn d, acc ->
        acc + max(d.capacity_bytes - d.used_bytes, 0)
      end)
    end
  end

  @doc """
  Computes bytes on the given drive that are over-replicated and can be pruned.

  A chunk is over-replicated when `length(locations) > volume.durability.factor`.
  Computed on-demand by scanning ChunkIndex.
  """
  @spec redundant_bytes_on_drive(node(), String.t()) :: non_neg_integer()
  def redundant_bytes_on_drive(node, drive_id) do
    chunks = ChunkIndex.list_by_drive(node, drive_id)

    Enum.reduce(chunks, 0, fn chunk, acc ->
      target = target_replicas_for_chunk(chunk)

      if length(chunk.locations) > target do
        acc + (chunk.stored_size || 0)
      else
        acc
      end
    end)
  end

  ## GenServer Callbacks

  @impl true
  def init(_opts) do
    :telemetry.attach_many(
      "storage-metrics",
      [
        [:neonfs, :blob_store, :write_chunk, :stop],
        [:neonfs, :blob_store, :delete_chunk, :stop]
      ],
      &__MODULE__.handle_telemetry/4,
      nil
    )

    {:ok, %{}, {:continue, :compute_initial_usage}}
  end

  @impl true
  def handle_continue(:compute_initial_usage, state) do
    compute_initial_usage()
    {:noreply, state}
  end

  ## Telemetry Handlers

  @doc false
  def handle_telemetry(
        [:neonfs, :blob_store, :write_chunk, :stop],
        %{bytes_written: bytes_written},
        %{drive_id: drive_id},
        _config
      ) do
    increment_usage(drive_id, bytes_written)
  end

  def handle_telemetry(
        [:neonfs, :blob_store, :delete_chunk, :stop],
        %{bytes_freed: bytes_freed},
        %{drive_id: drive_id},
        _config
      ) do
    decrement_usage(drive_id, bytes_freed)
  end

  def handle_telemetry(_event, _measurements, _metadata, _config), do: :ok

  ## Private

  defp compute_initial_usage do
    chunks = ChunkIndex.list_all()

    usage_by_drive =
      Enum.reduce(chunks, %{}, fn chunk, acc ->
        accumulate_local_usage(chunk, acc)
      end)

    Enum.each(usage_by_drive, fn {drive_id, used_bytes} ->
      DriveRegistry.update_usage(drive_id, used_bytes)
    end)

    total_chunks = length(chunks)
    total_drives = map_size(usage_by_drive)

    if total_drives > 0 do
      Logger.info(
        "StorageMetrics: computed initial usage from #{total_chunks} chunks across #{total_drives} drives"
      )
    end
  end

  defp accumulate_local_usage(chunk, acc) do
    local_node = Node.self()

    Enum.reduce(chunk.locations, acc, fn location, inner_acc ->
      if location.node == local_node do
        stored = chunk.stored_size || 0
        Map.update(inner_acc, location.drive_id, stored, &(&1 + stored))
      else
        inner_acc
      end
    end)
  end

  defp increment_usage(drive_id, bytes) do
    case DriveRegistry.get_drive(Node.self(), drive_id) do
      {:ok, drive} ->
        DriveRegistry.update_usage(drive_id, drive.used_bytes + bytes)

      _ ->
        :ok
    end
  end

  defp decrement_usage(drive_id, bytes) do
    case DriveRegistry.get_drive(Node.self(), drive_id) do
      {:ok, drive} ->
        DriveRegistry.update_usage(drive_id, max(drive.used_bytes - bytes, 0))

      _ ->
        :ok
    end
  end

  defp target_replicas_for_chunk(chunk) do
    volume_id = get_volume_for_chunk(chunk)

    case volume_id do
      nil ->
        3

      vid ->
        case VolumeRegistry.get(vid) do
          {:ok, volume} -> volume.durability.factor
          _ -> 3
        end
    end
  end

  defp get_volume_for_chunk(chunk) do
    # Reverse lookup: find the first file referencing this chunk hash
    FileIndex.list_all()
    |> Enum.find_value(fn file ->
      if chunk.hash in file.chunks, do: file.volume_id
    end)
  rescue
    _ -> nil
  end
end
