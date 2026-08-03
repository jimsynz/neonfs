defmodule NeonFS.Core.IntentLog do
  @moduledoc """
  A cluster-wide **conflict lease** for metadata operations: at most one
  writer holds a given conflict key at a time.

  Acquisition is a Ra check-and-set on the conflict key, with a TTL-based
  lease that long-running operations can extend.

  ## What this is not

  This log does **not** make a cross-segment operation atomic, despite
  earlier documentation here claiming it did. It records an operation, its
  conflict key, its parameters and a TTL — and nothing else. It stores no
  participant shards, no expected or new roots, no mutation payload, no
  commit decision and no per-participant progress, so it cannot finish or
  abort an interrupted operation. Expiry releases the conflict key and
  leaves any partial commit durable, and there is no reconciler.

  Cross-shard atomicity comes from consensus instead — a single Ra command
  that validates every participating shard's expected root and flips them
  all or none. Treat this module as concurrency control only.

  ## Conflict Key Types

  - `{:file, file_id}` — one writer per file
  - `{:create, volume_id, parent_path, name}` — prevents duplicate creation
  - `{:dir, volume_id, parent_path}` — serialises directory modifications
  - `{:chunk_migration, chunk_hash}` — one migration per chunk
  - `{:volume_key_rotation, volume_id}` — one rotation per volume

  ## Usage

      intent = Intent.new(
        id: UUID.generate(),
        operation: :file_create,
        conflict_key: {:create, volume_id, parent_path, name},
        params: %{volume_id: volume_id, path: path}
      )

      case IntentLog.try_acquire(intent) do
        {:ok, intent_id} ->
          # Do work...
          IntentLog.complete(intent_id)

        {:error, %NeonFS.Error.Conflict{conflicting: existing}} ->
          # Another writer holds the lock
          :retry_later
      end

  ## Releasing

  `complete/1` and `fail/2` are for work whose completion is not itself a
  Ra command. When the work *is* one — a metadata batch published by
  `NeonFS.Core.Volume.MetadataWriter` — pass the intent id in that
  command's `:release_intents` instead: the entry that makes the write
  durable then frees the lease, and a writer that dies immediately after
  consensus leaves nothing held. Releasing such a lease afterwards means a
  crash in between holds the conflict key for the whole TTL, on a write
  that already succeeded.
  """

  alias NeonFS.Core.{Intent, MetadataStateMachine, RaServer, RaSupervisor}
  alias NeonFS.Error.{Conflict, Unavailable}

  require Logger

  @doc """
  Acquires an exclusive intent via Ra.

  Returns `{:ok, intent_id}` if the intent was acquired, or
  `{:error, %NeonFS.Error.Conflict{conflicting: existing_intent}}` if
  another active intent holds the same conflict key.

  If an existing intent's TTL has expired, the new intent takes over
  (expired intent marked as `:expired`).

  **Fails closed.** When Ra is unavailable this returns
  `{:error, %NeonFS.Error.Unavailable{}}` — it does not report success.
  A caller that treats unavailability as acquisition proceeds with nothing
  serialising it against a concurrent writer on another node, which is
  exactly the situation a lease exists to prevent and exactly when it is
  most likely. Callers decide what to do without the lease; they
  must not be told they hold one.
  """
  @spec try_acquire(Intent.t()) ::
          {:ok, binary()} | {:error, Conflict.t()} | {:error, term()}
  def try_acquire(%Intent{} = intent) do
    case ra_command({:try_acquire_intent, intent}) do
      {:ok, {:ok, :acquired}} -> {:ok, intent.id}
      {:ok, {:ok, :conflict, existing}} -> {:error, Conflict.from_reason(:conflict, existing)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Marks an intent as completed and releases its conflict key.
  """
  @spec complete(binary()) :: :ok | {:error, term()}
  def complete(intent_id) when is_binary(intent_id) do
    case ra_command({:complete_intent, intent_id}) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Marks an intent as failed with the given reason and releases its conflict key.
  """
  @spec fail(binary(), term()) :: :ok | {:error, term()}
  def fail(intent_id, reason) when is_binary(intent_id) do
    case ra_command({:fail_intent, intent_id, reason}) do
      {:ok, :ok} -> :ok
      {:ok, {:error, r}} -> {:error, r}
      {:error, r} -> {:error, r}
    end
  end

  @doc """
  Extends the TTL of a pending intent.

  Default extension is 300 seconds (5 minutes).
  """
  @spec extend(binary(), pos_integer()) :: :ok | {:error, term()}
  def extend(intent_id, additional_seconds \\ 300) when is_binary(intent_id) do
    case ra_command({:extend_intent, intent_id, additional_seconds}) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Retrieves an intent by ID.
  """
  @spec get(binary()) :: {:ok, Intent.t()} | {:error, :not_found} | {:error, term()}
  def get(intent_id) when is_binary(intent_id) do
    case RaSupervisor.query(fn state ->
           MetadataStateMachine.get_intent(state, intent_id)
         end) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, %Intent{} = intent} -> {:ok, intent}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Returns all active (pending, non-expired) intents.
  """
  @spec list_active() :: {:ok, [Intent.t()]} | {:error, term()}
  def list_active do
    RaSupervisor.query(fn state ->
      MetadataStateMachine.list_active_intents(state)
    end)
  end

  @doc """
  Returns all pending intents that have exceeded their TTL.
  """
  @spec list_expired() :: {:ok, [Intent.t()]} | {:error, term()}
  def list_expired do
    RaSupervisor.query(fn state ->
      MetadataStateMachine.list_expired_intents(state)
    end)
  end

  @doc """
  Removes all expired intents from Ra state.

  Returns `{:ok, count}` with the number of intents cleaned up.
  """
  @spec cleanup_expired_intents() :: {:ok, non_neg_integer()} | {:error, term()}
  def cleanup_expired_intents do
    case ra_command(:cleanup_expired_intents) do
      {:ok, {:ok, count}} -> {:ok, count}
      {:error, reason} -> {:error, reason}
    end
  end

  # Private helpers

  # credo:disable-for-next-line Credo.Check.Refactor.Try
  defp ra_command(cmd) do
    initialized = RaServer.initialized?()

    try do
      case RaSupervisor.command(cmd) do
        {:ok, result, _leader} ->
          {:ok, result}

        {:error, :noproc} ->
          ra_noproc_error(initialized)

        {:error, reason} ->
          {:error, reason}

        {:timeout, _node} ->
          {:error, Unavailable.from_reason(:timeout)}
      end
    catch
      :exit, {:noproc, _} ->
        ra_noproc_error(initialized)

      kind, reason ->
        Logger.debug("IntentLog Ra command error", kind: kind, reason: inspect(reason))

        if initialized,
          do: {:error, {:ra_error, {kind, reason}}},
          else: {:error, Unavailable.from_reason(:ra_not_available)}
    end
  end

  defp ra_noproc_error(true), do: {:error, Unavailable.from_reason(:ra_unavailable)}
  defp ra_noproc_error(false), do: {:error, Unavailable.from_reason(:ra_not_available)}
end
