defmodule NeonFS.TestGeneratorsTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Core.FileMeta
  alias NeonFS.Core.Volume
  alias NeonFS.TestGenerators

  describe "chunk_hash/0" do
    property "always produces 32-byte binaries" do
      check all(hash <- TestGenerators.chunk_hash()) do
        assert is_binary(hash)
        assert byte_size(hash) == 32
      end
    end
  end

  describe "volume_id/0" do
    property "always produces valid UUID format" do
      check all(id <- TestGenerators.volume_id()) do
        assert is_binary(id)
        assert byte_size(id) == 36

        assert Regex.match?(
                 ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
                 id
               )
      end
    end
  end

  describe "file_path/0" do
    property "always starts with / and has no empty segments" do
      check all(path <- TestGenerators.file_path()) do
        assert String.starts_with?(path, "/")
        refute String.contains?(path, "//")
        refute String.contains?(path, "..")
        assert :ok == FileMeta.validate_path(path)
      end
    end

    property "has 1-5 path segments" do
      check all(path <- TestGenerators.file_path()) do
        segments = path |> String.split("/") |> Enum.reject(&(&1 == ""))
        count = length(segments)
        assert count >= 1 and count <= 5
      end
    end
  end

  describe "volume_config/0" do
    property "produces volumes that pass validation" do
      check all(volume <- TestGenerators.volume_config()) do
        assert %Volume{} = volume
        assert is_binary(volume.name)
        assert byte_size(volume.name) > 0
        assert :ok == Volume.validate(volume)
      end
    end
  end

  describe "file_meta/0" do
    property "produces valid FileMeta structs" do
      check all(meta <- TestGenerators.file_meta()) do
        assert %FileMeta{} = meta
        assert is_binary(meta.id)
        assert is_binary(meta.volume_id)
        assert String.starts_with?(meta.path, "/")
        assert is_integer(meta.size) and meta.size >= 0
        assert is_integer(meta.mode)
        assert is_integer(meta.uid) and meta.uid >= 0
        assert is_integer(meta.gid) and meta.gid >= 0
        assert meta.version == 1
        assert %DateTime{} = meta.created_at
      end
    end
  end

  describe "compression_mode/0" do
    property "is one of :none, :lz4, :zstd" do
      check all(mode <- TestGenerators.compression_mode()) do
        assert mode in [:none, :lz4, :zstd]
      end
    end
  end

  describe "tier/0" do
    property "is one of :hot, :warm, :cold" do
      check all(t <- TestGenerators.tier()) do
        assert t in [:hot, :warm, :cold]
      end
    end
  end

  describe "binary_data/0" do
    property "produces binaries between 1 byte and 64 KB" do
      check all(data <- TestGenerators.binary_data()) do
        assert is_binary(data)
        assert byte_size(data) >= 1
        assert byte_size(data) <= 65_536
      end
    end
  end
end
