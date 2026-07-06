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
          sampling_rate: float() | nil,
          scrub_interval: pos_integer()
        }

  @type tiering_config :: %{
          initial_tier: tier(),
          promotion_threshold: pos_integer(),
          demotion_delay: pos_integer()
        }

  @type caching_config :: %{
          transformed_chunks: boolean(),
          reconstructed_stripes: boolean(),
          remote_chunks: boolean()
        }

  @type metadata_consistency_config :: %{
          replicas: pos_integer(),
          read_quorum: pos_integer(),
          write_quorum: pos_integer()
        }

  @type write_ack :: :local | :quorum | :all

  @type tier :: :hot | :warm | :cold

  @type atime_mode :: :noatime | :relatime

  @type t :: %__MODULE__{
          id: binary(),
          name: String.t(),
          owner: String.t() | :system | nil,
          atime_mode: atime_mode(),
          durability: durability_config(),
          write_ack: write_ack(),
          tiering: tiering_config(),
          caching: caching_config(),
          io_weight: pos_integer(),
          compression: compression_config(),
          verification: verification_config(),
          encryption: VolumeEncryption.t(),
          metadata_consistency: metadata_consistency_config() | nil,
          max_size: non_neg_integer() | nil,
          max_files: non_neg_integer() | nil,
          logical_size: non_neg_integer(),
          physical_size: non_neg_integer(),
          chunk_count: non_neg_integer(),
          file_count: non_neg_integer(),
          created_at: DateTime.t(),
          updated_at: DateTime.t(),
          system: boolean(),
          nfs_export: boolean(),
          nfs_allowed_ips: [String.t()],
          nfs_root_squash: boolean()
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
    :max_size,
    :max_files,
    :logical_size,
    :physical_size,
    :chunk_count,
    :file_count,
    :created_at,
    :updated_at,
    atime_mode: :noatime,
    nfs_export: false,
    nfs_allowed_ips: [],
    nfs_root_squash: true,
    system: false
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
      atime_mode: Keyword.get(opts, :atime_mode, :noatime),
      durability: Keyword.get(opts, :durability, default_durability()),
      write_ack: Keyword.get(opts, :write_ack, :local),
      tiering: Keyword.get(opts, :tiering, default_tiering()),
      caching: Keyword.get(opts, :caching, default_caching()),
      io_weight: Keyword.get(opts, :io_weight, 100),
      compression: Keyword.get(opts, :compression, default_compression()),
      verification: Keyword.get(opts, :verification, default_verification()),
      encryption: Keyword.get(opts, :encryption, default_encryption()),
      metadata_consistency: Keyword.get(opts, :metadata_consistency),
      max_size: Keyword.get(opts, :max_size),
      max_files: Keyword.get(opts, :max_files),
      nfs_allowed_ips: Keyword.get(opts, :nfs_allowed_ips, []),
      nfs_root_squash: Keyword.get(opts, :nfs_root_squash, true),
      logical_size: 0,
      physical_size: 0,
      chunk_count: 0,
      file_count: 0,
      created_at: now,
      updated_at: now
    }
  end

  @doc """
  Returns the default durability configuration.

  Production default is 3-way replication with minimum 2 copies for write
  acknowledgement. Override via `config :neonfs_client, :default_durability,
  %{type: :replicate, factor: 1, min_copies: 1}` in environments where the
  3-replica default can't be satisfied (single-drive integration tests in
  particular — pre-#835 the global metadata quorum hid this from suites that
  declared no durability on `Volume.new/2`; post-#835 the per-volume write
  path refuses to provision).
  """
  @spec default_durability() :: durability_config()
  def default_durability do
    Application.get_env(:neonfs_client, :default_durability, %{
      type: :replicate,
      factor: 3,
      min_copies: 2
    })
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

  All caching enabled.
  """
  @spec default_caching() :: caching_config()
  def default_caching do
    %{
      transformed_chunks: true,
      reconstructed_stripes: true,
      remote_chunks: true
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
    %{on_read: :never, sampling_rate: nil, scrub_interval: 2_592_000}
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
      :atime_mode,
      :caching,
      :compression,
      :durability,
      :encryption,
      :io_weight,
      :metadata_consistency,
      :nfs_export,
      :nfs_allowed_ips,
      :nfs_root_squash,
      :owner,
      :tiering,
      :verification,
      :write_ack
    ]

    updates =
      opts
      |> Keyword.take(allowed_keys)
      |> normalise_caching_opts()

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
        file_count: Keyword.get(opts, :file_count, volume.file_count),
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
  Returns true if the volume is a system volume.
  """
  @spec system?(t()) :: boolean()
  def system?(%Volume{system: true}), do: true
  def system?(%Volume{}), do: false

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
  - io_weight must be positive integer
  - Compression algorithm must be :zstd or :none
  - Compression level must be 1-22 for zstd
  - Verification on_read must be one of: :always, :never, :sampling
  - Sampling rate must be 0.0-1.0 when on_read is :sampling
  - max_size must be nil (unlimited) or a positive integer
  """
  @spec validate(t()) :: :ok | {:error, String.t()}
  def validate(%Volume{} = volume) do
    with :ok <- validate_system_field(volume),
         :ok <- validate_name(volume.name),
         :ok <- validate_atime_mode(volume.atime_mode),
         :ok <- validate_durability(volume.durability),
         :ok <- validate_write_ack(volume.write_ack),
         :ok <- validate_tiering(volume.tiering),
         :ok <- validate_caching(volume.caching),
         :ok <- validate_io_weight(volume.io_weight),
         :ok <- validate_compression(volume.compression),
         :ok <- validate_verification(volume.verification),
         :ok <- validate_encryption(volume.encryption),
         :ok <- validate_nfs_export(volume.nfs_export),
         :ok <- validate_nfs_allowed_ips(volume.nfs_allowed_ips),
         :ok <- validate_nfs_root_squash(volume.nfs_root_squash),
         :ok <- validate_max_size(volume.max_size),
         :ok <- validate_max_files(volume.max_files) do
      validate_metadata_consistency(volume.metadata_consistency)
    end
  end

  defp validate_max_size(nil), do: :ok
  defp validate_max_size(bytes) when is_integer(bytes) and bytes > 0, do: :ok
  defp validate_max_size(_), do: {:error, "max_size must be nil or a positive integer"}

  defp validate_max_files(nil), do: :ok
  defp validate_max_files(count) when is_integer(count) and count > 0, do: :ok
  defp validate_max_files(_), do: {:error, "max_files must be nil or a positive integer"}

  defp validate_system_field(%Volume{system: true, name: "_system"}), do: :ok

  defp validate_system_field(%Volume{system: true}),
    do: {:error, "system: true is only allowed for the _system volume"}

  defp validate_system_field(%Volume{}), do: :ok

  # Volume names are used to build host paths (e.g. the Docker plugin's
  # `Path.join(mount_root, name)` + `File.mkdir_p`), so a name like
  # `../../tmp` would escape the mount root. Reject path separators,
  # traversal segments, NUL and control characters at the source — this
  # also keeps names within the conservative subset valid as S3 bucket
  # names (#1201). File paths inside volumes are already guarded by
  # `FileMeta.validate_path/1`.
  defp validate_name(name) when is_binary(name) and byte_size(name) > 0 do
    cond do
      String.contains?(name, "/") -> {:error, "name must not contain '/'"}
      String.contains?(name, "..") -> {:error, "name must not contain '..'"}
      name == "." -> {:error, "name must not be '.'"}
      has_unsafe_byte?(name) -> {:error, "name must not contain NUL or control characters"}
      true -> :ok
    end
  end

  defp validate_name(_), do: {:error, "name must be a non-empty string"}

  defp has_unsafe_byte?(name) do
    name |> :binary.bin_to_list() |> Enum.any?(&(&1 < 0x20 or &1 == 0x7F))
  end

  defp validate_atime_mode(mode) when mode in [:noatime, :relatime], do: :ok
  defp validate_atime_mode(_), do: {:error, "atime_mode must be :noatime or :relatime"}

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
         remote_chunks: rc
       })
       when is_boolean(tc) and is_boolean(rs) and is_boolean(rc) do
    :ok
  end

  defp validate_caching(_),
    do:
      {:error,
       "invalid caching: transformed_chunks, reconstructed_stripes, remote_chunks must be booleans"}

  defp normalise_caching_opts(opts) do
    case Keyword.fetch(opts, :caching) do
      {:ok, caching} ->
        Keyword.put(opts, :caching, normalise_caching(caching))

      :error ->
        opts
    end
  end

  defp normalise_caching(%{} = caching) do
    Map.drop(caching, [:max_memory, "max_memory"])
  end

  defp validate_io_weight(w) when is_integer(w) and w > 0, do: :ok
  defp validate_io_weight(_), do: {:error, "io_weight must be a positive integer"}

  defp validate_compression(%{algorithm: :none}), do: :ok

  defp validate_compression(%{algorithm: :zstd, level: level})
       when is_integer(level) and level >= 1 and level <= 22 do
    :ok
  end

  defp validate_compression(_),
    do: {:error, "compression algorithm must be :none or :zstd with level 1-22"}

  defp validate_verification(%{on_read: on_read} = config) when on_read in [:always, :never] do
    validate_scrub_interval(config)
  end

  defp validate_verification(%{on_read: :sampling, sampling_rate: rate} = config)
       when is_float(rate) and rate >= 0.0 and rate <= 1.0 do
    validate_scrub_interval(config)
  end

  defp validate_verification(%{on_read: :sampling}),
    do: {:error, "sampling verification requires sampling_rate between 0.0 and 1.0"}

  defp validate_verification(_),
    do: {:error, "on_read must be :always, :never, or :sampling"}

  defp validate_scrub_interval(%{scrub_interval: si}) when is_integer(si) and si > 0, do: :ok

  defp validate_scrub_interval(%{scrub_interval: _}),
    do: {:error, "scrub_interval must be a positive integer"}

  defp validate_scrub_interval(_), do: :ok

  defp validate_encryption(%VolumeEncryption{} = enc), do: VolumeEncryption.validate(enc)
  defp validate_encryption(_), do: {:error, "encryption must be a VolumeEncryption struct"}

  defp validate_nfs_export(flag) when is_boolean(flag), do: :ok
  defp validate_nfs_export(_), do: {:error, "nfs_export must be a boolean"}

  defp validate_nfs_root_squash(flag) when is_boolean(flag), do: :ok
  defp validate_nfs_root_squash(_), do: {:error, "nfs_root_squash must be a boolean"}

  # An empty list means allow-all (the historical posture); otherwise each
  # entry must be a well-formed IP or CIDR string (#1217).
  defp validate_nfs_allowed_ips(list) when is_list(list) do
    if Enum.all?(list, &valid_cidr_or_ip?/1) do
      :ok
    else
      {:error,
       ~s(nfs_allowed_ips must be a list of IP or CIDR strings, e.g. "10.0.0.0/8" or "192.168.1.5")}
    end
  end

  defp validate_nfs_allowed_ips(_), do: {:error, "nfs_allowed_ips must be a list"}

  defp valid_cidr_or_ip?(entry) when is_binary(entry) do
    case String.split(entry, "/") do
      [ip] -> match?({:ok, _}, :inet.parse_address(String.to_charlist(ip)))
      [ip, prefix] -> valid_cidr?(ip, prefix)
      _ -> false
    end
  end

  defp valid_cidr_or_ip?(_), do: false

  defp valid_cidr?(ip, prefix) do
    with {:ok, addr} <- :inet.parse_address(String.to_charlist(ip)),
         {n, ""} <- Integer.parse(prefix) do
      max = if tuple_size(addr) == 4, do: 32, else: 128
      n >= 0 and n <= max
    else
      _ -> false
    end
  end

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
