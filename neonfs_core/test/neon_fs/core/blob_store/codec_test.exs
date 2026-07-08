defmodule NeonFS.Core.BlobStore.CodecTest do
  @moduledoc """
  Unit tests for the pure codec-resolution helpers extracted from
  `NeonFS.Core.BlobStore` in #1207.
  """

  use ExUnit.Case, async: true

  alias NeonFS.Core.BlobStore.Codec

  describe "opts_for_chunk/1" do
    test "returns empty opts when compression and crypto are absent" do
      assert Codec.opts_for_chunk(%{}) == []
    end

    test "maps :none and :zstd compression atoms through" do
      assert Codec.opts_for_chunk(%{compression: :none}) == [compression: :none]
      assert Codec.opts_for_chunk(%{compression: :zstd}) == [compression: :zstd]
    end

    test "passes through a non-atom compression value verbatim" do
      assert Codec.opts_for_chunk(%{compression: "zstd:9"}) == [compression: "zstd:9"]
    end

    test "emits a placeholder key alongside the real nonce for encrypted chunks" do
      nonce = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12>>

      assert Codec.opts_for_chunk(%{compression: :zstd, crypto: %{nonce: nonce}}) ==
               [compression: :zstd, key: <<0::256>>, nonce: nonce]
    end

    test "encryption without compression yields just the key/nonce placeholder pair" do
      nonce = <<0::96>>
      assert Codec.opts_for_chunk(%{crypto: %{nonce: nonce}}) == [key: <<0::256>>, nonce: nonce]
    end

    test "ignores a crypto map without a binary nonce" do
      assert Codec.opts_for_chunk(%{compression: :none, crypto: %{}}) == [compression: :none]
    end
  end

  describe "compression_args/1" do
    test "absent or empty compression maps to the plain codec" do
      assert Codec.compression_args([]) == {"", 0}
      assert Codec.compression_args(compression: "") == {"", 0}
    end

    test "none maps to {\"none\", 0} from atom and string" do
      assert Codec.compression_args(compression: :none) == {"none", 0}
      assert Codec.compression_args(compression: "none") == {"none", 0}
    end

    test "zstd uses the default level when none is given" do
      assert Codec.compression_args(compression: :zstd) == {"zstd", 3}
      assert Codec.compression_args(compression: "zstd") == {"zstd", 3}
    end

    test "zstd honours an explicit :compression_level" do
      assert Codec.compression_args(compression: :zstd, compression_level: 9) == {"zstd", 9}
    end

    test "the {:zstd, level} tuple form carries its own level" do
      assert Codec.compression_args(compression: {:zstd, 12}) == {"zstd", 12}
    end

    test "the \"zstd:N\" string form parses the embedded level" do
      assert Codec.compression_args(compression: "zstd:7") == {"zstd", 7}
    end

    test "a malformed \"zstd:N\" level falls back to the default" do
      assert Codec.compression_args(compression: "zstd:oops") == {"zstd", 3}
      assert Codec.compression_args(compression: "zstd:oops", compression_level: 5) == {"zstd", 5}
    end
  end
end
