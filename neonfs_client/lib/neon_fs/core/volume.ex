defmodule NeonFS.Core.Volume do
  @moduledoc """
  Volume configuration structure.

  A volume represents a logical storage namespace with policies for durability,
  tiering, compression, and verification. Phase 3 adds tiering policies,
  caching configuration, and I/O weighting.
  """

  alias __MODULE__
  alias NeonFS.Core.VolumeEncryption

  @type durability_config ::
          %{
            type: :replicate,
            factor: pos_integer(),
            min_copies: pos_integer()
          }
          | %{
              type: :erasure,
              data_chunks: pos_integer(),
              parity_chunks: pos_integer()
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

  @type tiering_config :: %{
          initial_tier: tier(),
          promotion_threshold: pos_integer(),
          demotion_delay: pos_integer()
        }

  @type caching_config :: %{
          transformed_chunks: boolean(),
          reconstructed_stripes: boolean(),
          remote_chunks: boolean(),
          max_memory: pos_integer()
        }

  @type metadata_consistency_config :: %{
          replicas: pos_integer(),
          read_quorum: pos_integer(),
          write_quorum: pos_integer()
        }

  @type write_ack :: :local | :quorum | :all

  @type tier :: :hot | :warm | :cold

  @type t :: %__MODULE__{
          id: binary(),
          name: String.t(),
          owner: String.t() | nil,
          durability: durability_config(),
          write_ack: write_ack(),
          tiering: tiering_config(),
          caching: caching_config(),
          io_weight: pos_integer(),
          compression: compression_config(),
          verification: verification_config(),
          encryption: VolumeEncryption.t(),
          metadata_consistency: metadata_consistency_config() | nil,
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
    :tiering,
    :caching,
    :io_weight,
    :compression,
    :verification,
    :encryption,
    :metadata_consistency,
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
      tiering: Keyword.get(opts, :tiering, default_tiering()),
      caching: Keyword.get(opts, :caching, default_caching()),
      io_weight: Keyword.get(opts, :io_weight, 100),
      compression: Keyword.get(opts, :compression, default_compression()),
      verification: Keyword.get(opts, :verification, default_verification()),
      encryption: Keyword.get(opts, :encryption, default_encryption()),
      metadata_consistency: Keyword.get(opts, :metadata_consistency),
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
  Returns the default tiering configuration.

  Writes land on the hot tier by default, with promotion threshold of 10 accesses
  and demotion delay of 86400 seconds (24 hours).
  """
  @spec default_tiering() :: tiering_config()
  def default_tiering do
    %{initial_tier: :hot, promotion_threshold: 10, demotion_delay: 86_400}
  end

  @doc """
  Returns the default caching configuration.

  All caching enabled with 256MB max memory.
  """
  @spec default_caching() :: caching_config()
  def default_caching do
    %{
      transformed_chunks: true,
      reconstructed_stripes: true,
      remote_chunks: true,
      max_memory: 268_435_456
    }
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
  Returns the default encryption configuration.

  No encryption by default.
  """
  @spec default_encryption() :: VolumeEncryption.t()
  def default_encryption do
    VolumeEncryption.new()
  end

  @doc """
  Updates a volume with new configuration values.

  Only allows updating specific fields: owner, durability, write_ack, tiering,
  caching, io_weight, compression, verification. Updates the updated_at timestamp.

  ## Examples

      iex> Volume.update(volume, owner: "bob", write_ack: :quorum)
      %Volume{owner: "bob", write_ack: :quorum, updated_at: ...}
  """
  @spec update(t(), keyword()) :: t()
  def update(volume, opts) do
    allowed_keys = [
      :caching,
      :compression,
      :durability,
      :encryption,
      :io_weight,
      :metadata_consistency,
      :owner,
      :tiering,
      :verification,
      :write_ack
    ]

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
  Returns true if the volume uses erasure coding durability.
  """
  @spec erasure?(t()) :: boolean()
  def erasure?(%Volume{durability: %{type: :erasure}}), do: true
  def erasure?(%Volume{}), do: false

  @doc """
  Returns true if the volume has encryption enabled.
  """
  @spec encrypted?(t()) :: boolean()
  def encrypted?(%Volume{encryption: encryption}), do: VolumeEncryption.active?(encryption)

  @doc """
  Validates a volume configuration.

  Returns `:ok` if valid, or `{:error, reason}` if invalid.

  ## Validation Rules

  - Name must be non-empty string
  - Replication factor must be >= 1
  - Minimum copies must be >= 1 and <= factor
  - Write acknowledgement must be one of: :local, :quorum, :all
  - Tiering initial_tier must be one of: :hot, :warm, :cold
  - Tiering promotion_threshold must be positive integer
  - Tiering demotion_delay must be positive integer
  - Caching max_memory must be positive integer
  - io_weight must be positive integer
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
         :ok <- validate_tiering(volume.tiering),
         :ok <- validate_caching(volume.caching),
         :ok <- validate_io_weight(volume.io_weight),
         :ok <- validate_compression(volume.compression),
         :ok <- validate_verification(volume.verification),
         :ok <- validate_encryption(volume.encryption) do
      validate_metadata_consistency(volume.metadata_consistency)
    end
  end

  defp validate_name(name) when is_binary(name) and byte_size(name) > 0, do: :ok
  defp validate_name(_), do: {:error, "name must be a non-empty string"}

  defp validate_durability(%{type: :replicate, factor: factor, min_copies: min_copies})
       when is_integer(factor) and factor >= 1 and is_integer(min_copies) and min_copies >= 1 and
              min_copies <= factor do
    :ok
  end

  defp validate_durability(%{type: :erasure, data_chunks: dc, parity_chunks: pc})
       when is_integer(dc) and dc >= 1 and is_integer(pc) and pc >= 1 do
    :ok
  end

  defp validate_durability(_),
    do:
      {:error,
       "invalid durability: replicate requires factor >= 1, min_copies >= 1 and <= factor; erasure requires data_chunks >= 1, parity_chunks >= 1"}

  defp validate_write_ack(ack) when ack in [:local, :quorum, :all], do: :ok
  defp validate_write_ack(_), do: {:error, "write_ack must be :local, :quorum, or :all"}

  defp validate_tiering(%{
         initial_tier: tier,
         promotion_threshold: pt,
         demotion_delay: dd
       })
       when tier in [:hot, :warm, :cold] and is_integer(pt) and pt > 0 and is_integer(dd) and
              dd > 0 do
    :ok
  end

  defp validate_tiering(_),
    do:
      {:error,
       "invalid tiering: initial_tier must be :hot, :warm, or :cold; promotion_threshold and demotion_delay must be positive integers"}

  defp validate_caching(%{
         transformed_chunks: tc,
         reconstructed_stripes: rs,
         remote_chunks: rc,
         max_memory: mm
       })
       when is_boolean(tc) and is_boolean(rs) and is_boolean(rc) and is_integer(mm) and mm > 0 do
    :ok
  end

  defp validate_caching(_),
    do:
      {:error,
       "invalid caching: transformed_chunks, reconstructed_stripes, remote_chunks must be booleans; max_memory must be positive integer"}

  defp validate_io_weight(w) when is_integer(w) and w > 0, do: :ok
  defp validate_io_weight(_), do: {:error, "io_weight must be a positive integer"}

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

  defp validate_encryption(%VolumeEncryption{} = enc), do: VolumeEncryption.validate(enc)
  defp validate_encryption(_), do: {:error, "encryption must be a VolumeEncryption struct"}

  defp validate_metadata_consistency(nil), do: :ok

  defp validate_metadata_consistency(%{
         replicas: n,
         read_quorum: r,
         write_quorum: w
       })
       when is_integer(n) and n >= 1 and is_integer(r) and r >= 1 and r <= n and is_integer(w) and
              w >= 1 and w <= n do
    if r + w > n do
      :ok
    else
      {:error,
       "metadata_consistency requires R + W > N for strong consistency (got R=#{r}, W=#{w}, N=#{n})"}
    end
  end

  defp validate_metadata_consistency(_),
    do:
      {:error,
       "invalid metadata_consistency: requires replicas >= 1, read_quorum and write_quorum >= 1 and <= replicas"}
end
