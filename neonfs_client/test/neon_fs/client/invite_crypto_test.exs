defmodule NeonFS.Client.InviteCryptoTest do
  use ExUnit.Case, async: true

  alias NeonFS.Client.InviteCrypto

  describe "encrypt/decrypt roundtrip" do
    test "decryption recovers original plaintext" do
      token = "nfs_inv_abc123_9999999999_sig456"

      response = %{
        "ca_cert_pem" => "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----",
        "node_cert_pem" => "-----BEGIN CERTIFICATE-----\nfake2\n-----END CERTIFICATE-----",
        "via_node" => "neonfs_core@node1"
      }

      plaintext = response |> :json.encode() |> IO.iodata_to_binary()
      encrypted = InviteCrypto.encrypt_response(plaintext, token)

      assert {:ok, decrypted} = InviteCrypto.decrypt_response(encrypted, token)
      assert decrypted["node_cert_pem"] =~ "BEGIN CERTIFICATE"
      assert decrypted["via_node"] == "neonfs_core@node1"
      assert decrypted["ca_cert_pem"] =~ "BEGIN CERTIFICATE"
    end

    test "decryption fails with wrong token" do
      token = "nfs_inv_abc123_9999999999_sig456"
      wrong_token = "nfs_inv_xyz789_9999999999_other00"

      plaintext = :json.encode(%{"ca_cert_pem" => "secret"}) |> IO.iodata_to_binary()
      encrypted = InviteCrypto.encrypt_response(plaintext, token)

      assert {:error, :decryption_failed} =
               InviteCrypto.decrypt_response(encrypted, wrong_token)
    end

    test "decryption fails with truncated data" do
      assert {:error, :invalid_response} = InviteCrypto.decrypt_response(<<1, 2, 3>>, "token")
    end
  end
end
