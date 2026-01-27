defmodule NeonFS.Core.Blob.NativeTest do
  use ExUnit.Case

  alias NeonFS.Core.Blob.Native

  describe "add/2" do
    test "adds two positive integers" do
      assert Native.add(1, 2) == 3
    end

    test "adds negative integers" do
      assert Native.add(-5, 3) == -2
    end

    test "adds zero" do
      assert Native.add(0, 42) == 42
    end
  end

  describe "compute_hash/1" do
    # SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    @empty_hash_hex "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    # SHA-256("hello world") = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
    @hello_world_hash_hex "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

    test "returns 32-byte binary" do
      hash = Native.compute_hash("test")
      assert is_binary(hash)
      assert byte_size(hash) == 32
    end

    test "computes correct hash for empty string" do
      hash = Native.compute_hash("")
      assert Base.encode16(hash, case: :lower) == @empty_hash_hex
    end

    test "computes correct hash for hello world" do
      hash = Native.compute_hash("hello world")
      assert Base.encode16(hash, case: :lower) == @hello_world_hash_hex
    end

    test "matches Elixir :crypto.hash/2" do
      data = "some test data for hashing"
      nif_hash = Native.compute_hash(data)
      erlang_hash = :crypto.hash(:sha256, data)
      assert nif_hash == erlang_hash
    end

    test "handles empty binary" do
      hash = Native.compute_hash(<<>>)
      assert is_binary(hash)
      assert byte_size(hash) == 32
    end

    test "handles large binary (1MB+)" do
      # Create 1MB of data
      large_data = :binary.copy(<<0>>, 1_048_576)
      hash = Native.compute_hash(large_data)
      assert byte_size(hash) == 32
      # Verify against Erlang's crypto
      assert hash == :crypto.hash(:sha256, large_data)
    end

    test "produces different hashes for different inputs" do
      hash1 = Native.compute_hash("hello")
      hash2 = Native.compute_hash("world")
      assert hash1 != hash2
    end

    test "produces same hash for same input (deterministic)" do
      data = "deterministic test"
      hash1 = Native.compute_hash(data)
      hash2 = Native.compute_hash(data)
      assert hash1 == hash2
    end

    test "handles binary with null bytes" do
      data = <<0, 1, 2, 0, 3, 4, 0>>
      hash = Native.compute_hash(data)
      assert hash == :crypto.hash(:sha256, data)
    end

    test "handles unicode binary" do
      data = "hello in Japanese: "
      hash = Native.compute_hash(data)
      assert hash == :crypto.hash(:sha256, data)
    end
  end
end
