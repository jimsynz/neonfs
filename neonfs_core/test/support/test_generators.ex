defmodule NeonFS.TestGenerators do
  @moduledoc """
  Shared StreamData generators for property-based testing across NeonFS.

  Provides generators for common types: hashes, UUIDs, paths, volumes,
  file metadata, compression modes, tiers, and binary data.

  ## Usage

      use ExUnitProperties

      property "chunk hashes are 32 bytes" do
        check all(hash <- NeonFS.TestGenerators.chunk_hash()) do
          assert byte_size(hash) == 32
        end
      end
  """

  use ExUnitProperties

  import Bitwise

  alias NeonFS.Core.FileMeta
  alias NeonFS.Core.Volume

  @doc """
  Generates a 32-byte binary (SHA-256 hash format).
  """
  @spec chunk_hash() :: StreamData.t(binary())
  def chunk_hash do
    StreamData.binary(length: 32)
  end

  @doc """
  Generates a valid UUID v4 string (lowercase hex with hyphens).
  """
  @spec volume_id() :: StreamData.t(String.t())
  def volume_id do
    StreamData.map(StreamData.binary(length: 16), fn bytes ->
      <<a::32, b::16, c::16, d::16, e::48>> = bytes

      :io_lib.format(
        "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
        [a, b, band(c, 0x0FFF) ||| 0x4000, band(d, 0x3FFF) ||| 0x8000, e]
      )
      |> IO.iodata_to_binary()
      |> String.downcase()
    end)
  end

  @doc """
  Generates a valid POSIX path with 1-5 segments of alphanumeric characters.

  Paths always start with `/`, have no empty segments, and do not contain `..`.
  """
  @spec file_path() :: StreamData.t(String.t())
  def file_path do
    segment =
      StreamData.string(:alphanumeric, min_length: 1, max_length: 12)
      |> StreamData.filter(&(&1 != "" and &1 != "." and &1 != ".."))

    StreamData.list_of(segment, min_length: 1, max_length: 5)
    |> StreamData.map(fn segments -> "/" <> Enum.join(segments, "/") end)
  end

  @doc """
  Generates a valid `NeonFS.Core.Volume` struct using `Volume.new/2`.

  Produces volumes with randomised names and default configuration that
  passes `Volume.validate/1`.
  """
  @spec volume_config() :: StreamData.t(Volume.t())
  def volume_config do
    gen all(
          name <- volume_name(),
          write_ack <- StreamData.member_of([:local, :quorum, :all]),
          initial_tier <- tier(),
          io_weight <- StreamData.positive_integer(),
          algorithm <- StreamData.member_of([:zstd, :none]),
          atime_mode <- StreamData.member_of([:noatime, :relatime])
        ) do
      compression =
        case algorithm do
          :zstd -> %{algorithm: :zstd, level: 3, min_size: 4096}
          :none -> %{algorithm: :none}
        end

      Volume.new(name,
        atime_mode: atime_mode,
        write_ack: write_ack,
        tiering: %{initial_tier: initial_tier, promotion_threshold: 10, demotion_delay: 86_400},
        io_weight: io_weight,
        compression: compression
      )
    end
  end

  @doc """
  Generates a valid `NeonFS.Core.FileMeta` struct using `FileMeta.new/3`.
  """
  @spec file_meta() :: StreamData.t(FileMeta.t())
  def file_meta do
    gen all(
          vol_id <- volume_id(),
          path <- file_path(),
          size <- StreamData.non_negative_integer(),
          mode <- posix_mode(),
          uid <- StreamData.integer(0..65_535),
          gid <- StreamData.integer(0..65_535)
        ) do
      FileMeta.new(vol_id, path, size: size, mode: mode, uid: uid, gid: gid)
    end
  end

  @doc """
  Generates a compression mode atom: `:none`, `:lz4`, or `:zstd`.
  """
  @spec compression_mode() :: StreamData.t(:none | :lz4 | :zstd)
  def compression_mode do
    StreamData.member_of([:none, :lz4, :zstd])
  end

  @doc """
  Generates a storage tier atom: `:hot`, `:warm`, or `:cold`.
  """
  @spec tier() :: StreamData.t(:hot | :warm | :cold)
  def tier do
    StreamData.member_of([:hot, :warm, :cold])
  end

  @doc """
  Generates an arbitrary binary between 1 byte and 64 KB.
  """
  @spec binary_data() :: StreamData.t(binary())
  def binary_data do
    StreamData.binary(min_length: 1, max_length: 65_536)
  end

  # Private helpers

  defp volume_name do
    StreamData.string(:alphanumeric, min_length: 1, max_length: 20)
    |> StreamData.filter(&(&1 != ""))
  end

  defp posix_mode do
    StreamData.integer(0o000..0o7777)
  end
end
