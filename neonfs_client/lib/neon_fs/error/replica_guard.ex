defmodule NeonFS.Error.ReplicaGuard do
  @moduledoc """
  A drive operation was refused because it would drop a volume below its
  durability floor.

  Shares `class: :conflict` with `NeonFS.Error.Conflict` — the refusal is
  transient in the same sense a lock conflict is: re-replication clears it.
  It is a distinct module because the operator diagnostics are the whole
  point of the error, and `Conflict`'s message is fixed.

  `reason` selects the refusal:

    * `:below_min_copies` — one or more volumes would fall under their
      `min_copies` floor. `--force` overrides.
    * `:system_zero_copies` — the cluster-critical `_system` volume would
      be left with no surviving copy. `--force` must not override this.
    * `:indeterminate` — the replica state could not be read, so the
      operation cannot be shown to be safe. Fails closed; `--force`
      overrides, because a cluster whose metadata is unreadable must
      still be repairable.

  `at_risk` carries one entry per affected volume: its name, its
  `min_copies` floor, how many of its chunks would fall below that floor,
  how many would reach zero copies, and the fewest surviving copies any of
  its chunks would be left with.
  """
  use Splode.Error,
    fields: [
      :node,
      :drive_id,
      :details,
      at_risk: [],
      operation: "Removing",
      reason: :below_min_copies
    ],
    class: :conflict

  @type reason :: :below_min_copies | :system_zero_copies | :indeterminate

  @type volume_risk :: %{
          volume_name: String.t(),
          system?: boolean(),
          min_copies: pos_integer(),
          below_min_copies: non_neg_integer(),
          zero_copies: non_neg_integer(),
          least_copies: non_neg_integer()
        }

  @type t :: %__MODULE__{}

  @doc """
  True when `--force` is allowed to override this refusal.
  """
  @spec forceable?(t()) :: boolean()
  def forceable?(%__MODULE__{reason: :system_zero_copies}), do: false
  def forceable?(%__MODULE__{}), do: true

  @impl true
  def message(%{reason: :system_zero_copies} = error) do
    "#{error.operation} drive #{describe_drive(error)} would leave the cluster-critical " <>
      "`_system` volume with no surviving copy of #{system_zero_count(error)} chunk(s). " <>
      "Refusing — `--force` cannot override this. Wait for re-replication onto another " <>
      "drive first; `neonfs drive replicas` shows the current copy counts."
  end

  def message(%{reason: :indeterminate} = error) do
    "#{error.operation} drive #{describe_drive(error)} cannot be shown to be safe: " <>
      "reading the authoritative replica state failed (#{inspect(error.details)}). " <>
      "Refusing rather than risking data loss. Pass `--force` to proceed anyway."
  end

  def message(error) do
    "#{error.operation} drive #{describe_drive(error)} would drop " <>
      "#{length(error.at_risk)} volume(s) below `min_copies`: " <>
      Enum.map_join(error.at_risk, "; ", &describe_volume/1) <>
      ". Pass `--force` to proceed anyway."
  end

  defp describe_drive(%{node: nil, drive_id: drive_id}), do: "'#{drive_id}'"
  defp describe_drive(%{node: node, drive_id: drive_id}), do: "'#{drive_id}' on #{node}"

  defp describe_volume(risk) do
    "#{risk.volume_name} (#{risk.below_min_copies} chunk(s) below min_copies " <>
      "#{risk.min_copies}, fewest surviving copies #{risk.least_copies})"
  end

  defp system_zero_count(%{at_risk: at_risk}) do
    at_risk
    |> Enum.filter(& &1.system?)
    |> Enum.map(& &1.zero_copies)
    |> Enum.sum()
  end

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
