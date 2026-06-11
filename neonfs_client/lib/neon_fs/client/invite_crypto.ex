defmodule NeonFS.Client.InviteCrypto do
  @moduledoc """
  AES-256-GCM encryption of the invite-redemption response, keyed from
  the invite token.

  Both halves live here so the wire format has one source of truth:
  the serving side (`NeonFS.Cluster.InviteRedemption` in core) encrypts
  with `encrypt_response/2`; the joining side (`NeonFS.Client.Join`)
  decrypts with `decrypt_response/2`. Only the invite-token holder can
  decrypt — the token itself never traverses the network.
  """

  @response_salt "neonfs-invite-response"

  @doc """
  Encrypts a JSON plaintext with a key derived from the invite token.

  Returns `iv <> tag <> ciphertext`.
  """
  @spec encrypt_response(binary(), String.t()) :: binary()
  def encrypt_response(plaintext, token) do
    key = derive_key(token)
    iv = :crypto.strong_rand_bytes(12)

    {ciphertext, tag} =
      :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, plaintext, <<>>, true)

    iv <> tag <> ciphertext
  end

  @doc """
  Decrypts an invite redemption response using the invite token.

  Called on the joining node after receiving the encrypted blob from
  the via node's HTTP endpoint.
  """
  @spec decrypt_response(binary(), String.t()) :: {:ok, map()} | {:error, atom()}
  def decrypt_response(encrypted_blob, token) when byte_size(encrypted_blob) > 28 do
    key = derive_key(token)

    <<iv::binary-12, tag::binary-16, ciphertext::binary>> = encrypted_blob

    case :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, ciphertext, <<>>, tag, false) do
      plaintext when is_binary(plaintext) ->
        {:ok, :json.decode(plaintext)}

      :error ->
        {:error, :decryption_failed}
    end
  end

  def decrypt_response(_blob, _token), do: {:error, :invalid_response}

  defp derive_key(token) do
    :crypto.mac(:hmac, :sha256, @response_salt, token)
  end
end
