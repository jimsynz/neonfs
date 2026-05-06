defmodule NeonFS.Core.Volume.HLC do
  @moduledoc """
  Per-volume Hybrid Logical Clock operations.

  Wraps `NeonFS.Core.HLC` to take a `NeonFS.Core.Volume.RootSegment`
  instead of a bare `HLC` struct. Each volume carries its own HLC
  state in `RootSegment.hlc`; metadata writes thread the current
  root segment through `now/1` (or `receive_timestamp/2` for remote
  timestamps), getting a fresh timestamp + an updated segment to
  encode into the new root chunk on commit (#785).

  Cross-volume HLC ordering is intentionally undefined — volumes are
  independent. Use `NeonFS.Core.HLC` directly only in cluster-scope
  contexts (which are increasingly rare under epic #750).
  """

  alias NeonFS.Core.HLC
  alias NeonFS.Core.Volume.RootSegment

  @doc """
  Generates a new HLC timestamp on the volume, returning the
  timestamp and the segment with its `hlc` advanced.
  """
  @spec now(RootSegment.t()) :: {HLC.timestamp(), RootSegment.t()}
  def now(%RootSegment{hlc: %HLC{} = hlc} = segment) do
    {timestamp, new_hlc} = HLC.now(hlc)
    {timestamp, %{segment | hlc: new_hlc}}
  end

  @doc """
  `now/1` variant that uses an explicit wall-clock value. Useful for
  deterministic tests.
  """
  @spec now(RootSegment.t(), non_neg_integer()) :: {HLC.timestamp(), RootSegment.t()}
  def now(%RootSegment{hlc: %HLC{} = hlc} = segment, wall_ms)
      when is_integer(wall_ms) and wall_ms >= 0 do
    {timestamp, new_hlc} = HLC.now(hlc, wall_ms)
    {timestamp, %{segment | hlc: new_hlc}}
  end

  @doc """
  Incorporates a remote timestamp into the volume's HLC.

  Returns `{:ok, timestamp, segment}` with the segment's HLC advanced
  past the merged timestamp, or `{:error, :clock_skew_detected, skew_ms}`
  if the remote wall time exceeds the local clock by more than the
  HLC's configured skew bound.
  """
  @spec receive_timestamp(RootSegment.t(), HLC.timestamp()) ::
          {:ok, HLC.timestamp(), RootSegment.t()}
          | {:error, :clock_skew_detected, non_neg_integer()}
  def receive_timestamp(%RootSegment{hlc: %HLC{} = hlc} = segment, remote_timestamp) do
    case HLC.receive_timestamp(hlc, remote_timestamp) do
      {:ok, timestamp, new_hlc} ->
        {:ok, timestamp, %{segment | hlc: new_hlc}}

      {:error, _, _} = err ->
        err
    end
  end

  @doc """
  `receive_timestamp/2` variant that uses an explicit wall-clock
  value. Useful for deterministic tests.
  """
  @spec receive_timestamp(RootSegment.t(), HLC.timestamp(), non_neg_integer()) ::
          {:ok, HLC.timestamp(), RootSegment.t()}
          | {:error, :clock_skew_detected, non_neg_integer()}
  def receive_timestamp(%RootSegment{hlc: %HLC{} = hlc} = segment, remote_timestamp, wall_ms)
      when is_integer(wall_ms) and wall_ms >= 0 do
    case HLC.receive_timestamp(hlc, remote_timestamp, wall_ms) do
      {:ok, timestamp, new_hlc} ->
        {:ok, timestamp, %{segment | hlc: new_hlc}}

      {:error, _, _} = err ->
        err
    end
  end
end
