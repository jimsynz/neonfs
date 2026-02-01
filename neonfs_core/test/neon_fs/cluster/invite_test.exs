defmodule NeonFS.Cluster.InviteTest do
  use ExUnit.Case, async: false

  @moduletag :ra

  alias NeonFS.Cluster.{Init, Invite, State}

  setup do
    # Clean up state file before each test
    state_file = State.state_file_path()

    on_exit(fn ->
      if File.exists?(state_file) do
        File.rm!(state_file)
      end
    end)

    # Initialize cluster for tests
    cluster_name = "test_cluster_#{:rand.uniform(1_000_000)}"
    {:ok, _cluster_id} = Init.init_cluster(cluster_name)

    {:ok, cluster_name: cluster_name}
  end

  describe "create_invite/1" do
    test "creates a valid invite token" do
      assert {:ok, token} = Invite.create_invite(3600)
      assert is_binary(token)
      assert String.starts_with?(token, "nfs_inv_")

      # Token should have 5 parts: prefix, "inv", random, expiry, signature
      parts = String.split(token, "_")
      assert length(parts) == 5
    end

    test "creates tokens with different expiration times" do
      {:ok, token1} = Invite.create_invite(60)
      {:ok, token2} = Invite.create_invite(3600)

      # Tokens should be different
      assert token1 != token2

      # Extract expiry from tokens
      [_, _, _, expiry1, _] = String.split(token1, "_")
      [_, _, _, expiry2, _] = String.split(token2, "_")

      expiry1_int = String.to_integer(expiry1)
      expiry2_int = String.to_integer(expiry2)

      # Second token should expire later
      assert expiry2_int > expiry1_int
    end

    test "returns error when cluster not initialized" do
      # Remove state file
      state_file = State.state_file_path()
      File.rm!(state_file)

      assert {:error, :cluster_not_initialized} = Invite.create_invite(3600)
    end

    test "creates unique tokens" do
      {:ok, token1} = Invite.create_invite(3600)
      {:ok, token2} = Invite.create_invite(3600)

      assert token1 != token2
    end
  end

  describe "validate_invite/1" do
    test "validates a valid token" do
      {:ok, token} = Invite.create_invite(3600)
      assert :ok = Invite.validate_invite(token)
    end

    test "rejects expired token" do
      # Create token that expires in 1 second
      {:ok, token} = Invite.create_invite(1)

      # Wait for expiration
      Process.sleep(1100)

      assert {:error, :expired} = Invite.validate_invite(token)
    end

    test "rejects token with invalid format" do
      assert {:error, :invalid_format} = Invite.validate_invite("invalid_token")
      assert {:error, :invalid_format} = Invite.validate_invite("nfs_inv_only_three_parts")

      assert {:error, :invalid_format} =
               Invite.validate_invite("nfs_inv_random_notanumber_signature")
    end

    test "rejects token with invalid signature" do
      # Create a valid token
      {:ok, token} = Invite.create_invite(3600)

      # Tamper with the signature (last part)
      [prefix, inv, random, expiry, _signature] = String.split(token, "_")
      tampered_token = "#{prefix}_#{inv}_#{random}_#{expiry}_tampered"

      assert {:error, :invalid_signature} = Invite.validate_invite(tampered_token)
    end

    test "rejects token when cluster not initialized" do
      # Create a valid token first
      {:ok, token} = Invite.create_invite(3600)

      # Remove state file
      state_file = State.state_file_path()
      File.rm!(state_file)

      assert {:error, :cluster_not_initialized} = Invite.validate_invite(token)
    end

    test "tokens from different clusters are not valid" do
      # Create token from first cluster
      {:ok, token1} = Invite.create_invite(3600)

      # Reinitialize with different cluster
      state_file = State.state_file_path()
      File.rm!(state_file)
      {:ok, _} = Init.init_cluster("different_cluster")

      # Token from first cluster should be invalid
      assert {:error, :invalid_signature} = Invite.validate_invite(token1)
    end
  end

  describe "token format" do
    test "token has correct structure" do
      {:ok, token} = Invite.create_invite(3600)

      [prefix, inv, random, expiry, signature] = String.split(token, "_")

      # Check prefix
      assert prefix == "nfs"
      assert inv == "inv"

      # Check random part (should be 16 chars)
      assert String.length(random) == 16

      # Check expiry is a valid timestamp
      assert String.to_integer(expiry) > 0

      # Check signature (should be 16 chars)
      assert String.length(signature) == 16
    end

    test "expiry timestamp is in the future" do
      {:ok, token} = Invite.create_invite(3600)

      [_, _, _, expiry_str, _] = String.split(token, "_")
      expiry = String.to_integer(expiry_str)
      now = DateTime.utc_now() |> DateTime.to_unix()

      assert expiry > now
    end
  end
end
