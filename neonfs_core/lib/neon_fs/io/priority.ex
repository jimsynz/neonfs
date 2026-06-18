defmodule NeonFS.IO.Priority do
  @moduledoc """
  I/O priority levels for the scheduler.

  Seven classes ordered by urgency — higher weight means more urgent.
  User-facing operations always preempt background maintenance work.

  `:metadata_commit` (#1305) sits just below `:user_read` and above
  `:user_write`: a metadata commit finalises in-flight user writes and
  makes data visible, so it shouldn't queue behind bulk write data, but
  interactive reads still win. The weight is a deployment tuning knob.
  """

  @type t ::
          :user_read
          | :metadata_commit
          | :user_write
          | :replication
          | :read_repair
          | :repair
          | :scrub

  @priorities [
    :user_read,
    :metadata_commit,
    :user_write,
    :replication,
    :read_repair,
    :repair,
    :scrub
  ]

  @weights %{
    user_read: 100,
    metadata_commit: 90,
    user_write: 80,
    replication: 60,
    read_repair: 40,
    repair: 20,
    scrub: 10
  }

  @doc """
  Returns the numeric weight for a priority level.

  Higher weight means more urgent. Used by the WFQ scheduler
  to determine dispatch order.
  """
  @spec weight(t()) :: pos_integer()
  def weight(priority) when is_map_key(@weights, priority) do
    Map.fetch!(@weights, priority)
  end

  @doc """
  Returns all valid priority levels, ordered from highest to lowest urgency.
  """
  @spec all() :: [t()]
  def all, do: @priorities

  @doc """
  Returns `true` if the given value is a valid priority level.
  """
  @spec valid?(term()) :: boolean()
  def valid?(priority), do: priority in @priorities
end
