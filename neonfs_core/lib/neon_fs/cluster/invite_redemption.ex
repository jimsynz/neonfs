defmodule NeonFS.Cluster.InviteRedemption do
  @moduledoc """
  Handles invite token redemption for the HTTP join endpoint.

  When a node wants to join a cluster, its daemon POSTs to an existing
  cluster member's `/api/cluster/redeem-invite` endpoint. The request
  contains a CSR and proof of invite token possession — the token itself
  never traverses the network.

  The response (containing the cluster CA cert, signed node cert, and
  cookie) is encrypted with AES-256-GCM using a key derived from the
  invite token.

  ## Security properties

  - **Token never sent**: The joining node proves possession via
    `HMAC-SHA256(csr_pem, full_token)`, without transmitting the token.
  - **Response encrypted**: Only the token holder can decrypt the response.
  - **CSR binding**: The proof is bound to the specific CSR, preventing
    replay with a different CSR.
  """

  import Bitwise

  alias NeonFS.Cluster.State
  alias NeonFS.Core.CertificateAuthority
  alias NeonFS.Transport.TLS

  require Logger

  @type redemption_params :: %{
          String.t() => String.t()
        }

  @response_salt "neonfs-invite-response"

  @doc """
  Redeems an invite token and returns encrypted cluster credentials.

  ## Parameters

  A map with string keys:
  - `"csr_pem"` — PEM-encoded CSR from the joining node
  - `"token_random"` — random component of the invite token
  - `"token_expiry"` — expiry timestamp (string) of the invite token
  - `"proof"` — Base64-encoded HMAC-SHA256(csr_pem, full_token)
  - `"node_name"` — Erlang node name of the joining node

  ## Returns

  - `{:ok, encrypted_blob}` — AES-256-GCM encrypted response
  - `{:error, reason}` — validation or signing failure
  """
  @spec redeem(redemption_params()) :: {:ok, binary()} | {:error, atom()}
  def redeem(%{
        "csr_pem" => csr_pem,
        "token_random" => token_random,
        "token_expiry" => token_expiry_str,
        "proof" => proof_b64,
        "node_name" => node_name
      })
      when is_binary(csr_pem) and is_binary(token_random) and
             is_binary(token_expiry_str) and is_binary(proof_b64) and
             is_binary(node_name) do
    with {:ok, state} <- load_state(),
         {:ok, token} <- reconstruct_token(state.master_key, token_random, token_expiry_str),
         :ok <- check_expiry(token_expiry_str),
         :ok <- verify_proof(csr_pem, token, proof_b64),
         {:ok, csr} <- decode_and_validate_csr(csr_pem),
         {:ok, node_cert_pem, ca_cert_pem} <- sign_csr(csr, node_name),
         {:ok, cookie} <- get_cookie() do
      response = build_response(ca_cert_pem, node_cert_pem, cookie)
      encrypted = encrypt_response(response, token)

      Logger.info("Invite redeemed successfully", node_name: node_name)
      {:ok, encrypted}
    end
  end

  def redeem(_params), do: {:error, :invalid_params}

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

  # Private functions

  defp load_state do
    case State.load() do
      {:ok, state} -> {:ok, state}
      {:error, :not_found} -> {:error, :cluster_not_initialized}
      {:error, reason} -> {:error, reason}
    end
  end

  defp reconstruct_token(master_key, random, expiry_str) do
    payload = "#{random}_#{expiry_str}"

    signature =
      :crypto.mac(:hmac, :sha256, master_key, payload)
      |> Base.encode32(case: :lower, padding: false)
      |> binary_part(0, 16)

    {:ok, "nfs_inv_#{random}_#{expiry_str}_#{signature}"}
  end

  defp check_expiry(expiry_str) do
    case Integer.parse(expiry_str) do
      {expiry, ""} ->
        now = DateTime.utc_now() |> DateTime.to_unix()
        if now < expiry, do: :ok, else: {:error, :expired}

      _ ->
        {:error, :invalid_format}
    end
  end

  defp verify_proof(csr_pem, token, proof_b64) do
    case Base.decode64(proof_b64) do
      {:ok, provided_proof} ->
        expected_proof = :crypto.mac(:hmac, :sha256, token, csr_pem)

        if secure_compare(expected_proof, provided_proof) do
          :ok
        else
          {:error, :invalid_proof}
        end

      :error ->
        {:error, :invalid_proof}
    end
  end

  defp decode_and_validate_csr(csr_pem) do
    csr = TLS.decode_csr!(csr_pem)

    if TLS.valid_csr_format?(csr) and TLS.validate_csr(csr) do
      {:ok, csr}
    else
      {:error, :invalid_csr}
    end
  rescue
    _ -> {:error, :invalid_csr}
  end

  defp sign_csr(csr, node_name) do
    hostname =
      node_name
      |> String.split("@")
      |> List.last()

    case CertificateAuthority.sign_node_csr(csr, hostname) do
      {:ok, node_cert, ca_cert} ->
        {:ok, TLS.encode_cert(node_cert), TLS.encode_cert(ca_cert)}

      {:error, reason} ->
        {:error, {:cert_signing_failed, reason}}
    end
  end

  defp get_cookie do
    cookie = Node.get_cookie() |> Atom.to_string()
    {:ok, cookie}
  end

  defp build_response(ca_cert_pem, node_cert_pem, cookie) do
    %{
      "ca_cert_pem" => ca_cert_pem,
      "node_cert_pem" => node_cert_pem,
      "cookie" => cookie,
      "via_node" => Atom.to_string(Node.self()),
      "via_dist_port" => local_dist_port()
    }
    |> :json.encode()
    |> IO.iodata_to_binary()
  end

  defp local_dist_port do
    case System.get_env("NEONFS_DIST_PORT") do
      nil -> 0
      port_str -> String.to_integer(port_str)
    end
  rescue
    _ -> 0
  end

  defp encrypt_response(plaintext, token) do
    key = derive_key(token)
    iv = :crypto.strong_rand_bytes(12)

    {ciphertext, tag} =
      :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, plaintext, <<>>, true)

    iv <> tag <> ciphertext
  end

  defp derive_key(token) do
    :crypto.mac(:hmac, :sha256, @response_salt, token)
  end

  defp secure_compare(a, b) when byte_size(a) != byte_size(b), do: false

  defp secure_compare(a, b) do
    a_bytes = :binary.bin_to_list(a)
    b_bytes = :binary.bin_to_list(b)

    result =
      Enum.zip(a_bytes, b_bytes)
      |> Enum.reduce(0, fn {x, y}, acc -> acc ||| Bitwise.bxor(x, y) end)

    result == 0
  end
end
