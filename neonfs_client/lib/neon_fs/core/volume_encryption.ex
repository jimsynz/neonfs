defmodule NeonFS.Core.VolumeEncryption do
  @moduledoc """
  Per-volume encryption configuration.

  Tracks encryption mode, key versions, and rotation state for a volume.
  Only `:none` and `:server_side` modes are supported in Phase 6.

  Key versions allow multiple keys to coexist during rotation. The `keys` map
  stores wrapped key entries keyed by version number. Old versions are retained
  to decrypt chunks encrypted with previous keys.
  """

  alias __MODULE__

  @type mode :: :none | :server_side

  @type wrapped_key_entry :: %{
          wrapped_key: binary(),
          created_at: DateTime.t(),
          deprecated_at: DateTime.t() | nil
        }

  @type rotation_state :: %{
          from_version: pos_integer(),
          to_version: pos_integer(),
          started_at: DateTime.t(),
          progress: %{total_chunks: non_neg_integer(), migrated: non_neg_integer()}
        }

  @type t :: %__MODULE__{
          mode: mode(),
          current_key_version: non_neg_integer(),
          keys: %{pos_integer() => wrapped_key_entry()},
          rotation: rotation_state() | nil
        }

  defstruct mode: :none,
            current_key_version: 0,
            keys: %{},
            rotation: nil

  @doc """
  Creates a new encryption configuration.

  ## Options

  - `:mode` - `:none` (default) or `:server_side`
  - `:current_key_version` - Key version for new writes (default: 0 for `:none`, required for `:server_side`)
  - `:keys` - Map of version => wrapped key entry (default: %{})
  - `:rotation` - Rotation state (default: nil)

  ## Examples

      iex> VolumeEncryption.new(mode: :none)
      %VolumeEncryption{mode: :none, current_key_version: 0, keys: %{}, rotation: nil}

      iex> VolumeEncryption.new(mode: :server_side, current_key_version: 1, keys: %{1 => %{wrapped_key: <<...>>, created_at: ~U[...], deprecated_at: nil}})
      %VolumeEncryption{mode: :server_side, current_key_version: 1, ...}
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %VolumeEncryption{
      mode: Keyword.get(opts, :mode, :none),
      current_key_version: Keyword.get(opts, :current_key_version, 0),
      keys: Keyword.get(opts, :keys, %{}),
      rotation: Keyword.get(opts, :rotation)
    }
  end

  @doc """
  Returns whether encryption is enabled (mode is not `:none`).

  ## Examples

      iex> VolumeEncryption.active?(%VolumeEncryption{mode: :none})
      false

      iex> VolumeEncryption.active?(%VolumeEncryption{mode: :server_side})
      true
  """
  @spec active?(t()) :: boolean()
  def active?(%VolumeEncryption{mode: :none}), do: false
  def active?(%VolumeEncryption{}), do: true

  @doc """
  Returns whether a key rotation is currently in progress.
  """
  @spec rotating?(t()) :: boolean()
  def rotating?(%VolumeEncryption{rotation: nil}), do: false
  def rotating?(%VolumeEncryption{}), do: true

  @doc """
  Validates a VolumeEncryption configuration.

  Returns `:ok` if valid, or `{:error, reason}` if invalid.

  ## Validation Rules

  - Mode must be `:none` or `:server_side`
  - When mode is `:server_side`, `current_key_version` must be a positive integer
  - When mode is `:none`, no keys should be present
  """
  @spec validate(t()) :: :ok | {:error, String.t()}
  def validate(%VolumeEncryption{mode: :none, keys: keys}) when keys == %{}, do: :ok

  def validate(%VolumeEncryption{mode: :none, keys: keys}) when keys != %{},
    do: {:error, "encryption mode :none must not have keys"}

  def validate(%VolumeEncryption{mode: :server_side, current_key_version: v})
      when not is_integer(v) or v < 1,
      do: {:error, "server_side encryption requires current_key_version to be a positive integer"}

  def validate(%VolumeEncryption{mode: :server_side}), do: :ok

  def validate(%VolumeEncryption{mode: mode}),
    do: {:error, "encryption mode must be :none or :server_side, got: #{inspect(mode)}"}
end
