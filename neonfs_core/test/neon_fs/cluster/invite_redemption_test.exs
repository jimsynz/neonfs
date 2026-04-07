defmodule NeonFS.Cluster.InviteRedemptionTest do
  use ExUnit.Case, async: true

  alias NeonFS.Cluster.InviteRedemption

  describe "encrypt/decrypt roundtrip" do
    test "decryption recovers original plaintext" do
      token = "nfs_inv_abc123_9999999999_sig456"

      response = %{
        "ca_cert_pem" => "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----",
        "node_cert_pem" => "-----BEGIN CERTIFICATE-----\nfake2\n-----END CERTIFICATE-----",
        "cookie" => "test_cookie_12345",
        "via_node" => "neonfs_core@node1"
      }

      plaintext = response |> :json.encode() |> IO.iodata_to_binary()

      # Use the internal encrypt function via the module's public decrypt_response
      key = :crypto.mac(:hmac, :sha256, "neonfs-invite-response", token)
      iv = :crypto.strong_rand_bytes(12)

      {ciphertext, tag} =
        :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, plaintext, <<>>, true)

      encrypted = iv <> tag <> ciphertext

      assert {:ok, decrypted} = InviteRedemption.decrypt_response(encrypted, token)
      assert decrypted["cookie"] == "test_cookie_12345"
      assert decrypted["via_node"] == "neonfs_core@node1"
      assert decrypted["ca_cert_pem"] =~ "BEGIN CERTIFICATE"
    end

    test "decryption fails with wrong token" do
      token = "nfs_inv_abc123_9999999999_sig456"
      wrong_token = "nfs_inv_xyz789_9999999999_other00"

      plaintext = :json.encode(%{"cookie" => "secret"}) |> IO.iodata_to_binary()

      key = :crypto.mac(:hmac, :sha256, "neonfs-invite-response", token)
      iv = :crypto.strong_rand_bytes(12)

      {ciphertext, tag} =
        :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, plaintext, <<>>, true)

      encrypted = iv <> tag <> ciphertext

      assert {:error, :decryption_failed} =
               InviteRedemption.decrypt_response(encrypted, wrong_token)
    end

    test "decryption fails with truncated data" do
      assert {:error, :invalid_response} = InviteRedemption.decrypt_response(<<1, 2, 3>>, "token")
    end
  end

  describe "redeem/1 validation" do
    test "returns error for missing params" do
      assert {:error, :invalid_params} = InviteRedemption.redeem(%{})
    end

    test "returns error for non-map input" do
      assert {:error, :invalid_params} = InviteRedemption.redeem("not a map")
    end
  end
end
