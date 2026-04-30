defmodule NeonFS.Containerd.DigestTest do
  use ExUnit.Case, async: true
  doctest NeonFS.Containerd.Digest

  alias NeonFS.Containerd.Digest

  describe "to_path/1" do
    test "shards a valid sha256 digest into <ab>/<cd>/<rest>" do
      digest =
        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

      assert {:ok, "sha256/01/23/456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"} ==
               Digest.to_path(digest)
    end

    test "rejects non-hex characters" do
      digest =
        "sha256:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"

      assert {:error, :invalid_digest} = Digest.to_path(digest)
    end

    test "rejects wrong-length hex" do
      assert {:error, :invalid_digest} = Digest.to_path("sha256:abcd")
    end

    test "rejects unknown algorithms" do
      assert {:error, :unsupported_algorithm} = Digest.to_path("sha512:abcdef")
      assert {:error, :unsupported_algorithm} = Digest.to_path("md5:abc")
    end

    test "rejects malformed digest strings" do
      assert {:error, :invalid_digest} = Digest.to_path("nope")
      assert {:error, :invalid_digest} = Digest.to_path("")
    end
  end
end
