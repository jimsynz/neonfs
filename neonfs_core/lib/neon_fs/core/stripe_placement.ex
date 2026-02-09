defmodule NeonFS.Core.StripePlacement do
  @moduledoc """
  Stripe-aware chunk placement for erasure-coded volumes.

  Distributes stripe chunks across failure domains (nodes, then drives) to
  maximise fault tolerance. Unlike replication (same chunk on N nodes), stripe
  placement assigns N unique chunks to N distinct targets.

  Placement priority:
  1. Different nodes (highest fault tolerance)
  2. Different drives on the same node (medium)
  3. Same drive (lowest — logs a warning)
  """

  alias NeonFS.Core.{Drive, DriveRegistry}

  require Logger

  @type target :: %{node: node(), drive_id: String.t()}
  @type stripe_config :: %{data_chunks: pos_integer(), parity_chunks: pos_integer()}

  @capacity_threshold 0.90

  @doc """
  Selects placement targets for a stripe's chunks.

  Returns an ordered list of `%{node, drive_id}` targets — one per chunk in the
  stripe (data_chunks + parity_chunks). Targets are spread across failure
  domains: different nodes first, then different drives on the same node.

  ## Parameters

    * `stripe_config` — map with `:data_chunks` and `:parity_chunks`
    * `opts` — keyword list:
      * `:tier` — storage tier to filter drives (default: `:hot`)

  ## Returns

    * `{:ok, [target]}` — ordered list of targets
    * `{:error, :no_drives_available}` — no active drives in tier
  """
  @spec select_targets(stripe_config(), keyword()) :: {:ok, [target()]} | {:error, atom()}
  def select_targets(stripe_config, opts \\ []) do
    tier = Keyword.get(opts, :tier, :hot)
    total_chunks = stripe_config.data_chunks + stripe_config.parity_chunks

    drives = eligible_drives(tier)

    if drives == [] do
      {:error, :no_drives_available}
    else
      targets = assign_targets(drives, total_chunks)

      emit_placement_telemetry(targets, total_chunks, stripe_config)

      {:ok, targets}
    end
  end

  @doc """
  Validates that a placement meets minimum fault tolerance.

  A stripe can tolerate losing `parity_chunks` chunks. If all chunks on any
  single node exceed that count, a node failure could cause data loss.

  ## Returns

    * `:ok` — placement meets minimum fault tolerance
    * `{:warning, reason}` — placement is suboptimal
  """
  @spec validate_placement([target()], stripe_config()) :: :ok | {:warning, String.t()}
  def validate_placement(targets, stripe_config) do
    parity = stripe_config.parity_chunks
    min_domains = parity + 1

    distinct_nodes =
      targets
      |> Enum.map(& &1.node)
      |> Enum.uniq()
      |> length()

    if distinct_nodes >= min_domains do
      :ok
    else
      {:warning,
       "Only #{distinct_nodes} distinct node(s) for stripe with #{parity} parity chunks. " <>
         "Need #{min_domains}+ nodes for full fault tolerance."}
    end
  end

  # ─── Private ──────────────────────────────────────────────────────────

  defp eligible_drives(tier) do
    DriveRegistry.drives_for_tier(tier)
    |> Enum.filter(&(&1.state == :active))
    |> Enum.reject(&(Drive.usage_ratio(&1) >= @capacity_threshold))
    |> Enum.sort_by(&Drive.usage_ratio/1)
  end

  # Assigns `count` targets from available drives, maximising node separation.
  # Strategy: round-robin across nodes, cycling through each node's drives.
  defp assign_targets(drives, count) do
    by_node = group_drives_by_node(drives)
    node_keys = Map.keys(by_node) |> Enum.sort()

    if node_keys == [] do
      []
    else
      do_assign(by_node, node_keys, count, 0, [])
    end
  end

  defp group_drives_by_node(drives) do
    drives
    |> Enum.group_by(& &1.node)
    |> Enum.into(%{}, fn {node, node_drives} ->
      {node, Enum.sort_by(node_drives, &Drive.usage_ratio/1)}
    end)
  end

  # Round-robin across nodes, cycling through drives within each node.
  # `pass` tracks how many times we've cycled through all nodes.
  defp do_assign(_by_node, _node_keys, 0, _pass, acc), do: Enum.reverse(acc)

  defp do_assign(by_node, node_keys, remaining, pass, acc) do
    {new_acc, assigned} = assign_round(by_node, node_keys, pass, acc, remaining)

    if assigned == 0 do
      # No more targets can be assigned — fill remainder from first available
      fill_remaining(by_node, node_keys, remaining, new_acc)
    else
      do_assign(by_node, node_keys, remaining - assigned, pass + 1, new_acc)
    end
  end

  # One round: try to assign one target per node using the `pass`-th drive.
  defp assign_round(_by_node, _node_keys, _pass, acc, 0), do: {acc, 0}

  defp assign_round(by_node, node_keys, pass, acc, budget) do
    Enum.reduce(node_keys, {acc, 0, budget}, fn node, {a, count, left} ->
      pick_drive_for_round(by_node, node, pass, a, count, left)
    end)
    |> then(fn {a, count, _left} -> {a, count} end)
  end

  defp pick_drive_for_round(_by_node, _node, _pass, a, count, left) when left <= 0 do
    {a, count, left}
  end

  defp pick_drive_for_round(by_node, node, pass, a, count, left) do
    drives = Map.get(by_node, node, [])
    drive_idx = rem(pass, max(1, length(drives)))

    case Enum.at(drives, drive_idx) do
      nil -> {a, count, left}
      drive -> {[%{node: node, drive_id: drive.id} | a], count + 1, left - 1}
    end
  end

  # When round-robin exhausts unique drive slots, fill remaining by cycling.
  defp fill_remaining(_by_node, _node_keys, 0, acc), do: Enum.reverse(acc)

  defp fill_remaining(by_node, node_keys, remaining, acc) do
    all_targets =
      node_keys
      |> Enum.flat_map(fn node ->
        Map.get(by_node, node, [])
        |> Enum.map(fn drive -> %{node: node, drive_id: drive.id} end)
      end)

    case all_targets do
      [] ->
        Enum.reverse(acc)

      targets ->
        fill =
          Stream.cycle(targets)
          |> Enum.take(remaining)

        warn_reduced_fault_tolerance(remaining, node_keys)
        Enum.reverse(acc) ++ fill
    end
  end

  defp warn_reduced_fault_tolerance(extra_count, node_keys) do
    Logger.warning(
      "Stripe placement: #{extra_count} extra chunk(s) placed with reduced fault tolerance. " <>
        "Only #{length(node_keys)} node(s) available."
    )
  end

  defp emit_placement_telemetry(targets, total_chunks, stripe_config) do
    distinct_nodes =
      targets
      |> Enum.map(& &1.node)
      |> Enum.uniq()
      |> length()

    distinct_drives =
      targets
      |> Enum.map(fn t -> {t.node, t.drive_id} end)
      |> Enum.uniq()
      |> length()

    :telemetry.execute(
      [:neonfs, :stripe_placement, :select_targets],
      %{
        target_count: length(targets),
        distinct_nodes: distinct_nodes,
        distinct_drives: distinct_drives
      },
      %{
        total_chunks: total_chunks,
        data_chunks: stripe_config.data_chunks,
        parity_chunks: stripe_config.parity_chunks
      }
    )
  end
end
