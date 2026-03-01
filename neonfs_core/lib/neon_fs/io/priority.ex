defmodule NeonFS.IO.Priority do
  @moduledoc """
  I/O priority levels for the scheduler.

  Six classes ordered by urgency — higher weight means more urgent.
  User-facing operations always preempt background maintenance work.
  """

  @type t ::
          :user_read
          | :user_write
          | :replication
          | :read_repair
          | :repair
          | :scrub

  @priorities [:user_read, :user_write, :replication, :read_repair, :repair, :scrub]

  @weights %{
    user_read: 100,
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
