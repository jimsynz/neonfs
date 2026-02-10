defmodule NeonFS.Core.Intent do
  @moduledoc """
  Represents a write intent for coordinating cross-segment operations.

  Intents are stored in the Ra state machine to provide:
  - Crash-safe record of in-flight operations
  - Exclusive access via conflict keys (only one active intent per conflict key)
  - TTL-based cleanup for stale intents from crashed writers

  ## Conflict Keys

  The `conflict_key` field enables exclusive access. Common patterns:
  - `{:file, file_id}` — one writer per file
  - `{:create, volume_id, parent_path, name}` — prevents duplicate creation
  - `{:dir, volume_id, parent_path}` — serialises directory renames
  - `{:chunk_migration, chunk_hash}` — one migration per chunk
  - `{:volume_key_rotation, volume_id}` — one rotation per volume

  ## States

  - `:pending` — intent acquired, operation not yet started
  - `:completed` — operation finished successfully
  - `:failed` — operation failed (with reason)
  - `:expired` — TTL exceeded without extension (stale intent from crashed writer)
  """

  @type state :: :pending | :completed | :failed | :expired

  @type t :: %__MODULE__{
          id: binary(),
          operation: atom(),
          conflict_key: term(),
          params: map(),
          state: state(),
          started_at: DateTime.t(),
          expires_at: DateTime.t(),
          completed_at: DateTime.t() | nil,
          error: term() | nil
        }

  @enforce_keys [:id, :operation, :conflict_key, :state, :started_at, :expires_at]
  defstruct [
    :id,
    :operation,
    :conflict_key,
    :state,
    :started_at,
    :expires_at,
    :completed_at,
    :error,
    params: %{}
  ]

  @default_ttl_seconds 300

  @doc """
  Creates a new intent with the given attributes.

  ## Options

  - `:ttl_seconds` — time to live in seconds (default: #{@default_ttl_seconds})
  """
  @spec new(keyword()) :: t()
  def new(attrs) do
    now = Keyword.get(attrs, :started_at, DateTime.utc_now())
    ttl = Keyword.get(attrs, :ttl_seconds, @default_ttl_seconds)

    %__MODULE__{
      id: Keyword.fetch!(attrs, :id),
      operation: Keyword.fetch!(attrs, :operation),
      conflict_key: Keyword.fetch!(attrs, :conflict_key),
      params: Keyword.get(attrs, :params, %{}),
      state: :pending,
      started_at: now,
      expires_at: DateTime.add(now, ttl, :second)
    }
  end

  @doc """
  Returns true if the intent's TTL has expired.
  """
  @spec expired?(t()) :: boolean()
  def expired?(%__MODULE__{} = intent) do
    expired_at?(intent, DateTime.utc_now())
  end

  @doc """
  Returns true if the intent's TTL has expired relative to the given time.
  """
  @spec expired_at?(t(), DateTime.t()) :: boolean()
  def expired_at?(%__MODULE__{} = intent, now) do
    DateTime.compare(now, intent.expires_at) != :lt
  end

  @doc """
  Returns true if the intent is in an active state (:pending).
  """
  @spec active?(t()) :: boolean()
  def active?(%__MODULE__{state: :pending}), do: true
  def active?(%__MODULE__{}), do: false
end
