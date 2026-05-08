defmodule NeonFS.Core.Volume.DriveSelector do
  @moduledoc """
  Picks the drive set that a per-volume metadata chunk should be
  replicated to, given the volume's durability config and the
  cluster's currently active drives.

  The selector is a pure function — no I/O, no GenServer — and the
  output is deterministic for a given input. Two writers on the same
  cluster pick the same drives for the first write, which lets
  AntiEntropy stay quiet rather than chase divergent placements.

  Drives are spread across distinct nodes when possible: the
  algorithm round-robins through nodes (sorted by name, drives within
  a node sorted by `drive_id`), so the first replica lands on the
  first node, the second on the second node, and so on, only doubling
  up on a node once every other node has been used.

  Returns `{:error, :insufficient_drives, %{available: N, needed: M}}`
  when fewer than the durability's minimum-acceptable number of
  drives are available — `min_copies` for `:replicate`, `data_chunks`
  for `:erasure`. (Operating below the *target* count is fine as long
  as the minimum is met; AntiEntropy fills the gap once more drives
  come online.)
  """

  alias NeonFS.Core.MetadataStateMachine

  @type drive_id :: String.t()
  @type durability :: map()

  @typedoc """
  Drive entry shape as registered in the bootstrap layer (#779).
  See `t:NeonFS.Core.MetadataStateMachine.drive_entry/0`.
  """
  @type drive_entry :: MetadataStateMachine.drive_entry()

  @type insufficient ::
          {:error, :insufficient_drives, %{available: non_neg_integer(), needed: pos_integer()}}

  @doc """
  Picks replica drives for a metadata chunk.

  `drives` may be a list of drive entries or the map shape returned
  by `MetadataStateMachine.get_drives/1` (the keys are ignored — only
  the values matter). At most one entry per `{node, drive_id}` is
  honoured — `drive_id` alone is not unique across nodes.

  Returns a list of `{node, drive_id}` composite keys identifying the
  selected replicas.
  """
  @spec select_replicas(durability(), [drive_entry()] | %{any() => drive_entry()}) ::
          {:ok, [{node(), drive_id()}]} | insufficient()
  def select_replicas(durability, drives) do
    drive_list = normalise_drives(drives)
    {target, minimum} = counts(durability)

    if length(drive_list) < minimum do
      {:error, :insufficient_drives, %{available: length(drive_list), needed: minimum}}
    else
      take = min(target, length(drive_list))

      {:ok,
       drive_list
       |> spread_across_nodes()
       |> Enum.take(take)
       |> Enum.map(&{&1.node, &1.drive_id})}
    end
  end

  ## Internals

  defp normalise_drives(drives) when is_map(drives) do
    drives |> Map.values() |> normalise_drives()
  end

  defp normalise_drives(drives) when is_list(drives) do
    # Drives are uniquely identified by the composite `{node, drive_id}`
    # — `drive_id` alone collides across nodes (every node may expose
    # a `default` drive). Match the bootstrap layer's v14 keying.
    drives
    |> Enum.uniq_by(&{&1.node, &1.drive_id})
    |> Enum.sort_by(&{&1.node, &1.drive_id})
  end

  defp counts(%{type: :replicate, factor: factor, min_copies: min_copies}),
    do: {factor, min_copies}

  defp counts(%{type: :erasure, data_chunks: data, parity_chunks: parity}),
    do: {data + parity, data}

  defp spread_across_nodes(drives) do
    drives
    |> Enum.group_by(& &1.node)
    |> Enum.sort_by(fn {node, _} -> node end)
    |> Enum.map(fn {_node, ds} -> Enum.sort_by(ds, & &1.drive_id) end)
    |> round_robin()
  end

  defp round_robin(buckets) do
    case Enum.split_with(buckets, &(&1 != [])) do
      {[], _} ->
        []

      {non_empty, _} ->
        heads = Enum.map(non_empty, &hd/1)
        tails = Enum.map(non_empty, &tl/1)
        heads ++ round_robin(tails)
    end
  end
end
