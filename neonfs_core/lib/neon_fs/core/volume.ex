defmodule NeonFS.Core.Volume do
  @moduledoc """
  Volume configuration structure.

  A volume represents a logical storage namespace with policies for durability,
  tiering, compression, and verification. Phase 1 implements a basic subset;
  full policy configuration comes in Phase 3.
  """

  alias __MODULE__

  @type durability_config :: %{
          type: :replicate,
          factor: pos_integer(),
          min_copies: pos_integer()
        }

  @type compression_config :: %{
          algorithm: :zstd | :none,
          level: 1..22,
          min_size: non_neg_integer()
        }

  @type verification_config :: %{
          on_read: :always | :never | :sampling,
          sampling_rate: float() | nil
        }

  @type write_ack :: :local | :quorum | :all

  @type tier :: :hot | :warm | :cold

  @type t :: %__MODULE__{
          id: binary(),
          name: String.t(),
          owner: String.t() | nil,
          durability: durability_config(),
          write_ack: write_ack(),
          initial_tier: tier(),
          compression: compression_config(),
          verification: verification_config(),
          logical_size: non_neg_integer(),
          physical_size: non_neg_integer(),
          chunk_count: non_neg_integer(),
          created_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  defstruct [
    :id,
    :name,
    :owner,
    :durability,
    :write_ack,
    :initial_tier,
    :compression,
    :verification,
    :logical_size,
    :physical_size,
    :chunk_count,
    :created_at,
    :updated_at
  ]

  @doc """
  Creates a new volume with the given name and optional configuration.

  Returns a volume struct with default settings applied for any missing fields.

  ## Examples

      iex> Volume.new("my-volume")
      %Volume{name: "my-volume", ...}

      iex> Volume.new("my-volume", owner: "alice", write_ack: :quorum)
      %Volume{name: "my-volume", owner: "alice", write_ack: :quorum, ...}
  """
  @spec new(String.t(), keyword()) :: t()
  def new(name, opts \\ []) do
    now = DateTime.utc_now()
    id = UUIDv7.generate()

    %Volume{
      id: id,
      name: name,
      owner: Keyword.get(opts, :owner),
      durability: Keyword.get(opts, :durability, default_durability()),
      write_ack: Keyword.get(opts, :write_ack, :local),
      initial_tier: Keyword.get(opts, :initial_tier, :hot),
      compression: Keyword.get(opts, :compression, default_compression()),
      verification: Keyword.get(opts, :verification, default_verification()),
      logical_size: 0,
      physical_size: 0,
      chunk_count: 0,
      created_at: now,
      updated_at: now
    }
  end

  @doc """
  Returns the default durability configuration.

  Phase 1: 3-way replication with minimum 2 copies for write acknowledgement.
  """
  @spec default_durability() :: durability_config()
  def default_durability do
    %{type: :replicate, factor: 3, min_copies: 2}
  end

  @doc """
  Returns the default compression configuration.

  Zstd level 3, compress chunks >= 4KB.
  """
  @spec default_compression() :: compression_config()
  def default_compression do
    %{algorithm: :zstd, level: 3, min_size: 4096}
  end

  @doc """
  Returns the default verification configuration.

  Never verify on read (trust storage layer).
  """
  @spec default_verification() :: verification_config()
  def default_verification do
    %{on_read: :never, sampling_rate: nil}
  end

  @doc """
  Updates a volume with new configuration values.

  Only allows updating specific fields: owner, durability, write_ack, initial_tier,
  compression, verification. Updates the updated_at timestamp.

  ## Examples

      iex> Volume.update(volume, owner: "bob", write_ack: :quorum)
      %Volume{owner: "bob", write_ack: :quorum, updated_at: ...}
  """
  @spec update(t(), keyword()) :: t()
  def update(volume, opts) do
    allowed_keys = [:owner, :durability, :write_ack, :initial_tier, :compression, :verification]
    updates = Keyword.take(opts, allowed_keys)

    updates
    |> Map.new()
    |> Map.put(:updated_at, DateTime.utc_now())
    |> then(&Map.merge(volume, &1))
  end

  @doc """
  Updates volume statistics (size and chunk count).

  ## Examples

      iex> Volume.update_stats(volume, logical_size: 1024, physical_size: 512, chunk_count: 5)
      %Volume{logical_size: 1024, physical_size: 512, chunk_count: 5, updated_at: ...}
  """
  @spec update_stats(t(), keyword()) :: t()
  def update_stats(volume, opts) do
    %{
      volume
      | logical_size: Keyword.get(opts, :logical_size, volume.logical_size),
        physical_size: Keyword.get(opts, :physical_size, volume.physical_size),
        chunk_count: Keyword.get(opts, :chunk_count, volume.chunk_count),
        updated_at: DateTime.utc_now()
    }
  end

  @doc """
  Validates a volume configuration.

  Returns `:ok` if valid, or `{:error, reason}` if invalid.

  ## Validation Rules

  - Name must be non-empty string
  - Replication factor must be >= 1
  - Minimum copies must be >= 1 and <= factor
  - Write acknowledgement must be one of: :local, :quorum, :all
  - Initial tier must be one of: :hot, :warm, :cold
  - Compression algorithm must be :zstd or :none
  - Compression level must be 1-22 for zstd
  - Verification on_read must be one of: :always, :never, :sampling
  - Sampling rate must be 0.0-1.0 when on_read is :sampling
  """
  @spec validate(t()) :: :ok | {:error, String.t()}
  def validate(%Volume{} = volume) do
    with :ok <- validate_name(volume.name),
         :ok <- validate_durability(volume.durability),
         :ok <- validate_write_ack(volume.write_ack),
         :ok <- validate_tier(volume.initial_tier),
         :ok <- validate_compression(volume.compression) do
      validate_verification(volume.verification)
    end
  end

  defp validate_name(name) when is_binary(name) and byte_size(name) > 0, do: :ok
  defp validate_name(_), do: {:error, "name must be a non-empty string"}

  defp validate_durability(%{type: :replicate, factor: factor, min_copies: min_copies})
       when is_integer(factor) and factor >= 1 and is_integer(min_copies) and min_copies >= 1 and
              min_copies <= factor do
    :ok
  end

  defp validate_durability(_),
    do: {:error, "invalid durability: factor >= 1, min_copies >= 1 and <= factor"}

  defp validate_write_ack(ack) when ack in [:local, :quorum, :all], do: :ok
  defp validate_write_ack(_), do: {:error, "write_ack must be :local, :quorum, or :all"}

  defp validate_tier(tier) when tier in [:hot, :warm, :cold], do: :ok
  defp validate_tier(_), do: {:error, "initial_tier must be :hot, :warm, or :cold"}

  defp validate_compression(%{algorithm: :none}), do: :ok

  defp validate_compression(%{algorithm: :zstd, level: level})
       when is_integer(level) and level >= 1 and level <= 22 do
    :ok
  end

  defp validate_compression(_),
    do: {:error, "compression algorithm must be :none or :zstd with level 1-22"}

  defp validate_verification(%{on_read: :always}), do: :ok
  defp validate_verification(%{on_read: :never}), do: :ok

  defp validate_verification(%{on_read: :sampling, sampling_rate: rate})
       when is_float(rate) and rate >= 0.0 and rate <= 1.0 do
    :ok
  end

  defp validate_verification(%{on_read: :sampling}),
    do: {:error, "sampling verification requires sampling_rate between 0.0 and 1.0"}

  defp validate_verification(_),
    do: {:error, "on_read must be :always, :never, or :sampling"}
end
