defmodule NeonFS.Cluster.Invite do
  @moduledoc """
  Cluster invite token management.

  Invite tokens are time-limited credentials that allow new nodes
  to join an existing cluster. They follow the format:

      nfs_inv_<random>_<expiry_timestamp>_<signature>

  The signature is computed using HMAC-SHA256 with the cluster's master key.
  """

  import Bitwise

  alias NeonFS.Cluster.State

  @token_prefix "nfs_inv"

  @type invite_token :: String.t()
  @type duration :: pos_integer()

  @doc """
  Creates a new invite token valid for the specified duration.

  ## Parameters
  - `expires_in` - Duration in seconds the token is valid for

  ## Returns
  - `{:ok, token}` on success
  - `{:error, :cluster_not_initialized}` if cluster state doesn't exist

  ## Examples

      iex> NeonFS.Cluster.Invite.create_invite(3600)
      {:ok, "nfs_inv_abc123_1234567890_def456"}
  """
  @spec create_invite(duration()) :: {:ok, invite_token()} | {:error, :cluster_not_initialized}
  def create_invite(expires_in) when is_integer(expires_in) and expires_in > 0 do
    case State.load() do
      {:ok, state} ->
        token = generate_token(state.master_key, expires_in)
        {:ok, token}

      {:error, :not_found} ->
        {:error, :cluster_not_initialized}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Validates an invite token.

  ## Parameters
  - `token` - The invite token to validate

  ## Returns
  - `:ok` if token is valid and not expired
  - `{:error, reason}` if validation fails

  ## Examples

      iex> NeonFS.Cluster.Invite.validate_invite("nfs_inv_abc123_1234567890_def456")
      :ok
  """
  @spec validate_invite(invite_token()) ::
          :ok
          | {:error,
             :invalid_format
             | :expired
             | :invalid_signature
             | :cluster_not_initialized
             | term()}
  def validate_invite(token) when is_binary(token) do
    with {:ok, state} <- load_cluster_state(),
         {:ok, {random, expiry, signature}} <- parse_token(token),
         :ok <- check_expiry(expiry) do
      verify_signature(state.master_key, random, expiry, signature)
    end
  end

  # Private functions

  defp load_cluster_state do
    case State.load() do
      {:ok, state} -> {:ok, state}
      {:error, :not_found} -> {:error, :cluster_not_initialized}
      {:error, reason} -> {:error, reason}
    end
  end

  defp generate_token(master_key, expires_in) do
    random = generate_random_part()
    expiry = DateTime.utc_now() |> DateTime.add(expires_in, :second) |> DateTime.to_unix()
    signature = compute_signature(master_key, random, expiry)

    "#{@token_prefix}_#{random}_#{expiry}_#{signature}"
  end

  defp generate_random_part do
    :crypto.strong_rand_bytes(16)
    |> Base.encode32(case: :lower, padding: false)
    |> binary_part(0, 16)
  end

  defp compute_signature(master_key, random, expiry) do
    expiry_str = Integer.to_string(expiry)
    payload = "#{random}_#{expiry_str}"

    :crypto.mac(:hmac, :sha256, master_key, payload)
    |> Base.encode32(case: :lower, padding: false)
    |> binary_part(0, 16)
  end

  defp parse_token(token) do
    case String.split(token, "_") do
      ["nfs", "inv", random, expiry_str, signature] ->
        case Integer.parse(expiry_str) do
          {expiry, ""} -> {:ok, {random, expiry, signature}}
          _ -> {:error, :invalid_format}
        end

      _ ->
        {:error, :invalid_format}
    end
  end

  defp check_expiry(expiry) do
    now = DateTime.utc_now() |> DateTime.to_unix()

    if now < expiry do
      :ok
    else
      {:error, :expired}
    end
  end

  defp verify_signature(master_key, random, expiry, provided_signature) do
    expected_signature = compute_signature(master_key, random, expiry)

    if secure_compare(expected_signature, provided_signature) do
      :ok
    else
      {:error, :invalid_signature}
    end
  end

  # Constant-time string comparison to prevent timing attacks
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
