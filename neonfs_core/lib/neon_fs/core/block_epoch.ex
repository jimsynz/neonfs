defmodule NeonFS.Core.BlockEpoch do
  @moduledoc """
  Fencing epochs for block devices.

  A `NamespaceCoordinator` claim releases when its holder's node dies, which
  covers a crashed attacher but not a partitioned one: a holder that is cut
  off still believes it owns the device and keeps issuing writes. The claim
  layer cannot tell those apart, and neither can the storage layer — a write
  from a partitioned holder is indistinguishable from a live one's.

  An epoch closes that. An attacher reads the device's current epoch and
  stamps every metadata commit with it. A new attacher preempting the old one
  bumps the epoch, so the old holder's next commit carries a number that is
  behind and is refused — it discovers it has been preempted at the first
  write it attempts, rather than never.

  The counter lives in the Ra state machine, so a bump is ordered against
  every other cluster event and survives the leader changing.

  ## Keyed per device, not per volume

  One volume can hold several devices, so the key is the resolved
  `{volume_id, path}` — the same key `NeonFS.Block.DeviceRegistry` claims on.
  A volume-wide counter would fence every device on a volume when one of them
  was preempted.

  ## What this does not do

  Nothing here stops a preempted holder's *chunk* writes: they are
  content-addressed and harmless, and become GC debt. It is the metadata
  commit — the only thing that can make a chunk part of the device — that is
  fenced.
  """

  alias NeonFS.Core.{MetadataStateMachine, RaSupervisor}

  @type device_key :: {volume_id :: binary(), path :: String.t()}

  @doc """
  The device's current epoch, 0 until it is first preempted.

  Read consistently rather than locally: an attacher that reads a stale epoch
  stamps its commits with a number the cluster has already moved past, and
  fences *itself* at its first write.
  """
  @spec current(device_key()) :: {:ok, non_neg_integer()} | {:error, term()}
  def current({volume_id, path} = key) when is_binary(volume_id) and is_binary(path) do
    RaSupervisor.query(&MetadataStateMachine.get_block_epoch(&1, key))
  end

  @doc """
  Bumps the device's epoch and returns the new value — the preempting
  attacher's own epoch.

  Ordered through Ra, so two attachers racing to preempt the same device get
  different epochs and only the later one can commit.
  """
  @spec bump(device_key()) :: {:ok, pos_integer()} | {:error, term()}
  def bump({volume_id, path} = key) when is_binary(volume_id) and is_binary(path) do
    case RaSupervisor.command({:bump_block_epoch, key}) do
      {:ok, {:ok, epoch}, _leader} -> {:ok, epoch}
      {:ok, other, _leader} -> {:error, other}
      {:error, _} = error -> error
      {:timeout, _} -> {:error, :timeout}
    end
  end

  @doc """
  Checks a writer's epoch against the cluster's before a metadata commit.

  `{:error, {:fenced, current}}` means the caller has been preempted: it no
  longer owns the device and must tear its end down rather than retry, which
  is the one error here that is not worth retrying.
  """
  @spec check(device_key(), non_neg_integer()) ::
          :ok | {:error, {:fenced, non_neg_integer()}} | {:error, term()}
  def check(key, epoch) when is_integer(epoch) and epoch >= 0 do
    with {:ok, current} <- current(key) do
      if epoch >= current, do: :ok, else: {:error, {:fenced, current}}
    end
  end
end
