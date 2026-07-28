defmodule NeonFS.Core.ReplicaAudit do
  @moduledoc """
  Counts how many drives actually hold each volume's chunks, and decides
  whether losing one drive would take a volume below its durability floor
  (#1618).

  Two operator-facing uses, one traversal:

    * `audit/0` reports the volumes that are currently under-replicated
      and the drives that hold a sole copy of something — the state an
      operator otherwise only discovers during an incident.
    * `guard_removal/3` is the pre-flight for `drive evacuate` and
      `drive remove`. It refuses when the drive's disappearance would put
      any volume under `min_copies`, with `--force` as the escape hatch,
      and refuses unconditionally when the cluster-critical `_system`
      volume would be left with no surviving copy at all.

  Nothing stopped either operation before this: evacuation finalisation
  deregisters a drive on a filesystem-empty check, and
  `DriveManager.remove_drive/2`'s `:force` only bypassed the
  data-presence test. #1573 turned that into unrecoverable loss because
  `_system` held exactly one copy of the CA key; #1617 now scales
  `_system` with the drive count, and this guard stops an operator (or a
  future bug) from spending the redundancy that buys.

  ## Why the guard applies to evacuation too

  Evacuation *relocates* rather than abandons, so a successful evacuation
  preserves copy counts. What it cannot promise is success: a
  mid-evacuation failure on a drive that held the only copy has no
  fallback, which is exactly the #1573 shape. The query is therefore the
  same for both operations — "what if this drive disappeared" — and
  `--force` is how an operator says they accept that window.

  ## Reads the authoritative tree, not the ETS cache

  Copy counts come from `ChunkIndex.list_volume_chunks/1`, which
  range-scans each volume's `chunk_index` tree. `ChunkIndex`'s ETS table
  is a cold write-through cache that is empty after a restart, so
  `lookup_by_hash/1` would report almost everything as absent and wave
  every removal through — the same trap #1573 fell into.

  A volume with no bootstrap pointer yet (provisioning deferred until the
  cluster has enough drives) reads back `:not_found`, which is genuinely
  "no chunks, no risk". Any other read failure — including a registry or
  index subsystem that isn't running, which raises or exits rather than
  returning an error — is *indeterminate*: the guard fails closed rather
  than assuming safety, and refuses one operation rather than crashing
  its caller.

  ## Erasure-coded volumes

  Erasure durability has no `min_copies` — each stripe shard is normally
  a single chunk with a single copy, and losing up to `parity_chunks` of
  them is recoverable by reconstruction. This audit models the per-chunk
  floor only (one copy), so it flags a shard that would reach zero copies
  but does not decide whether the stripe could still be rebuilt. See
  #1626 for stripe-level accounting.
  """

  require Logger

  alias NeonFS.Core.{ChunkIndex, VolumeRegistry}
  alias NeonFS.Error.ReplicaGuard

  @type volume_replication :: %{
          volume_id: binary(),
          volume_name: String.t(),
          system?: boolean(),
          min_copies: pos_integer(),
          chunk_count: non_neg_integer(),
          below_min_copies: non_neg_integer(),
          zero_copies: non_neg_integer(),
          least_copies: non_neg_integer()
        }

  @type sole_copy_drive :: %{
          node: node(),
          drive_id: String.t(),
          chunk_count: non_neg_integer()
        }

  @type report :: %{
          volumes: [volume_replication()],
          under_replicated: [volume_replication()],
          sole_copy_drives: [sole_copy_drive()]
        }

  @doc """
  Reports current replication health across every volume, `_system`
  included.

  `:volumes` covers all of them; `:under_replicated` is the subset with at
  least one chunk below `min_copies`; `:sole_copy_drives` lists the drives
  that are the only holder of at least one chunk, worst first — those are
  the drives whose loss costs data.

  Emits `[:neonfs, :replica_audit, :under_replicated]` per affected volume
  so the condition can be alerted on rather than discovered during an
  incident, and `[:neonfs, :replica_audit, :completed]` with the summary.
  """
  @spec audit() :: {:ok, report()} | {:error, term()}
  def audit do
    with {:ok, %{volumes: volumes, sole_copies: sole_copies}} <- scan(nil) do
      under_replicated = Enum.filter(volumes, &(&1.below_min_copies > 0))
      Enum.each(under_replicated, &emit_under_replicated/1)

      report = %{
        volumes: volumes,
        under_replicated: under_replicated,
        sole_copy_drives: rank_sole_copies(sole_copies)
      }

      :telemetry.execute(
        [:neonfs, :replica_audit, :completed],
        %{
          volume_count: length(volumes),
          under_replicated_count: length(under_replicated),
          sole_copy_drive_count: map_size(sole_copies)
        },
        %{}
      )

      {:ok, report}
    end
  end

  @doc """
  Per-volume impact of `drive_id` on `node` disappearing.

  Every volume is included so callers can distinguish "unaffected" from
  "not examined". `below_min_copies` and `zero_copies` count only chunks
  this drive actually holds — a chunk already short of copies elsewhere is
  repair's outstanding work, not something this operation makes worse.
  `least_copies` is the fewest surviving copies across *all* the volume's
  chunks, so it still shows the worst case the operation leaves behind.
  """
  @spec removal_impact(node(), String.t()) :: {:ok, [volume_replication()]} | {:error, term()}
  def removal_impact(node, drive_id) when is_atom(node) and is_binary(drive_id) do
    with {:ok, %{volumes: volumes}} <- scan({node, drive_id}) do
      {:ok, volumes}
    end
  end

  @doc """
  Pre-flight guard for an operation that would take `drive_id` on `node`
  out of service.

  ## Options

    * `:force` - proceed despite a below-`min_copies` finding, or an
      indeterminate one (default `false`). Never overrides `_system`
      being left with zero copies.
    * `:operation` - verb for the refusal message, e.g. `"Evacuating"`
      (default `"Removing"`).
  """
  @spec guard_removal(node(), String.t(), keyword()) :: :ok | {:error, ReplicaGuard.t()}
  def guard_removal(node, drive_id, opts \\ []) do
    force = Keyword.get(opts, :force, false)
    operation = Keyword.get(opts, :operation, "Removing")

    case removal_impact(node, drive_id) do
      {:ok, volumes} ->
        decide(volumes, node, drive_id, force, operation)

      {:error, reason} ->
        indeterminate(node, drive_id, reason, force, operation)
    end
  end

  ## Private — policy

  defp decide(volumes, node, drive_id, force, operation) do
    at_risk = volumes |> Enum.filter(&(&1.below_min_copies > 0)) |> Enum.map(&to_risk/1)
    system_zero? = Enum.any?(at_risk, &(&1.system? and &1.zero_copies > 0))

    cond do
      system_zero? ->
        refuse(:system_zero_copies, at_risk, node, drive_id, operation, nil)

      at_risk == [] ->
        :ok

      force ->
        allow_forced(at_risk, node, drive_id, operation)

      true ->
        refuse(:below_min_copies, at_risk, node, drive_id, operation, nil)
    end
  end

  # An unreadable replica state is not evidence of safety, so the guard
  # fails closed. `--force` still gets through: a cluster whose metadata
  # is unreachable must remain repairable, and refusing every drive
  # operation in that state would be its own outage.
  defp indeterminate(node, drive_id, reason, true = _force, operation) do
    Logger.warning("Replica guard could not read replica state, proceeding under --force",
      drive_id: drive_id,
      node: node,
      operation: operation,
      reason: inspect(reason)
    )

    :ok
  end

  defp indeterminate(node, drive_id, reason, _force, operation) do
    refuse(:indeterminate, [], node, drive_id, operation, reason)
  end

  defp refuse(reason, at_risk, node, drive_id, operation, details) do
    error =
      ReplicaGuard.exception(
        reason: reason,
        at_risk: at_risk,
        node: node,
        drive_id: drive_id,
        operation: operation,
        details: details
      )

    :telemetry.execute(
      [:neonfs, :replica_audit, :guard_refused],
      %{at_risk_count: length(at_risk)},
      %{node: node, drive_id: drive_id, reason: reason, operation: operation}
    )

    Logger.warning("Refused drive operation: #{Exception.message(error)}")

    {:error, error}
  end

  defp allow_forced(at_risk, node, drive_id, operation) do
    :telemetry.execute(
      [:neonfs, :replica_audit, :guard_forced],
      %{at_risk_count: length(at_risk)},
      %{node: node, drive_id: drive_id, operation: operation}
    )

    Logger.warning(
      "#{operation} drive '#{drive_id}' on #{node} under --force despite " <>
        "#{length(at_risk)} volume(s) below min_copies: " <>
        Enum.map_join(at_risk, ", ", & &1.volume_name)
    )

    :ok
  end

  defp to_risk(volume) do
    Map.take(volume, [
      :volume_name,
      :system?,
      :min_copies,
      :below_min_copies,
      :zero_copies,
      :least_copies
    ])
  end

  ## Private — traversal

  # One pass over every volume's authoritative chunk tree. `candidate` is
  # the drive whose loss is being modelled, or nil to describe the cluster
  # as it stands.
  defp scan(candidate) do
    with {:ok, volumes} <- list_volumes(),
         {:ok, acc} <- reduce_volumes(volumes, candidate) do
      {:ok, %{acc | volumes: Enum.reverse(acc.volumes)}}
    end
  end

  defp reduce_volumes(volumes, candidate) do
    Enum.reduce_while(volumes, {:ok, %{volumes: [], sole_copies: %{}}}, fn volume, {:ok, acc} ->
      case summarise_volume(volume, candidate) do
        {:ok, summary, sole_copies} -> {:cont, {:ok, accumulate(acc, summary, sole_copies)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # A registry that isn't running raises (its ETS table is absent) or exits
  # rather than returning an error. Either way the answer is "we don't
  # know", which the guard treats as indeterminate — crashing the caller
  # would take `DriveManager` down instead of refusing one operation.
  defp list_volumes do
    {:ok, VolumeRegistry.list(include_system: true)}
  rescue
    error -> {:error, {:volume_registry_unavailable, error}}
  catch
    :exit, reason -> {:error, {:volume_registry_unavailable, reason}}
  end

  defp accumulate(acc, summary, sole_copies) do
    %{
      volumes: [summary | acc.volumes],
      sole_copies: Map.merge(acc.sole_copies, sole_copies, fn _drive, a, b -> a + b end)
    }
  end

  defp summarise_volume(volume, candidate) do
    case list_chunks(volume.id) do
      {:ok, chunks} ->
        {:ok, fold_chunks(volume, chunks, candidate), sole_copies(chunks, candidate)}

      # No bootstrap pointer: provisioning is deferred until the cluster
      # has drives enough for the volume's durability, so there is nothing
      # stored yet and nothing at risk.
      {:error, :not_found} ->
        {:ok, fold_chunks(volume, [], candidate), %{}}

      {:error, reason} ->
        {:error, {:volume_unreadable, volume.id, reason}}
    end
  end

  # Same reasoning as `list_volumes/0`: an absent index subsystem is an
  # unknown answer, not a reason to take the caller down with it.
  defp list_chunks(volume_id) do
    ChunkIndex.list_volume_chunks(volume_id)
  rescue
    error -> {:error, error}
  catch
    :exit, reason -> {:error, reason}
  end

  defp fold_chunks(volume, chunks, candidate) do
    min_copies = min_copies(volume)

    counts = Enum.reduce(chunks, empty_counts(), &count_chunk(&1, &2, min_copies, candidate))

    %{
      volume_id: volume.id,
      volume_name: volume.name,
      system?: Map.get(volume, :system, false),
      min_copies: min_copies,
      chunk_count: counts.chunk_count,
      below_min_copies: counts.below,
      zero_copies: counts.zero,
      least_copies: counts.least || min_copies
    }
  end

  defp empty_counts, do: %{chunk_count: 0, below: 0, zero: 0, least: nil}

  # A chunk this drive does not hold is unaffected by its loss, even when
  # it is already short of copies — the guard's question is what this
  # operation *changes*, not what repair still owes. Counting the whole
  # under-replicated backlog against every candidate drive would block all
  # drive maintenance whenever any volume was mid-repair. `audit/0` passes
  # no candidate, so there every chunk counts and the report describes the
  # backlog itself.
  defp count_chunk(chunk, acc, min_copies, candidate) do
    surviving = surviving_copies(chunk, candidate)

    acc = %{
      acc
      | chunk_count: acc.chunk_count + 1,
        least: min_or(acc.least, surviving)
    }

    if affected?(chunk, candidate) and surviving < min_copies do
      %{acc | below: acc.below + 1, zero: acc.zero + if(surviving == 0, do: 1, else: 0)}
    else
      acc
    end
  end

  defp affected?(_chunk, nil), do: true

  defp affected?(chunk, {node, drive_id}),
    do: Enum.any?(chunk.locations, &(&1.node == node and &1.drive_id == drive_id))

  # Distinct drives, not raw locations: the same drive listed twice (one
  # entry per tier after a migration) is still one failure domain.
  defp surviving_copies(chunk, candidate) do
    chunk.locations
    |> Enum.map(&{&1.node, &1.drive_id})
    |> Enum.uniq()
    |> Enum.reject(&(&1 == candidate))
    |> length()
  end

  # Only `audit/0` reports these, so skip the pass when a candidate drive
  # is being modelled.
  defp sole_copies(_chunks, {_node, _drive_id}), do: %{}

  defp sole_copies(chunks, nil) do
    Enum.reduce(chunks, %{}, fn chunk, acc ->
      case Enum.uniq(Enum.map(chunk.locations, &{&1.node, &1.drive_id})) do
        [only] -> Map.update(acc, only, 1, &(&1 + 1))
        _ -> acc
      end
    end)
  end

  defp rank_sole_copies(sole_copies) do
    sole_copies
    |> Enum.map(fn {{node, drive_id}, count} ->
      %{node: node, drive_id: drive_id, chunk_count: count}
    end)
    |> Enum.sort_by(&{-&1.chunk_count, &1.drive_id})
  end

  # Erasure volumes carry no `min_copies`; the per-chunk floor is one copy
  # (see the moduledoc on what that does and does not model).
  defp min_copies(%{durability: %{min_copies: min_copies}}) when is_integer(min_copies),
    do: min_copies

  defp min_copies(_volume), do: 1

  defp min_or(nil, value), do: value
  defp min_or(current, value), do: min(current, value)

  defp emit_under_replicated(volume) do
    :telemetry.execute(
      [:neonfs, :replica_audit, :under_replicated],
      %{
        below_min_copies: volume.below_min_copies,
        zero_copies: volume.zero_copies,
        least_copies: volume.least_copies,
        chunk_count: volume.chunk_count
      },
      %{
        volume_id: volume.volume_id,
        volume_name: volume.volume_name,
        min_copies: volume.min_copies,
        system?: volume.system?
      }
    )
  end
end
