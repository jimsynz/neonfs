defmodule NeonFS.Cluster.Invite do
  @moduledoc """
  Cluster invite token management.

  Invite tokens are time-limited credentials that allow new nodes
  to join an existing cluster. They follow the format:

      nfs_inv_<random>_<expiry_timestamp>_<uses>_<signature>

  The signature is computed using HMAC-SHA256 with the cluster's master key.

  ## The redemption budget

  `uses` is how many times the token may be redeemed, and it is inside the
  signed payload rather than recorded when the token is minted. That keeps
  minting stateless — issuing a token still only reads the master key, and
  reaches no consensus — while leaving the budget untamperable by whoever
  holds the token. Enforcement is `NeonFS.Cluster.InviteRedemption`'s, through
  a single Ra apply that counts redemptions against the signed budget.

  A budget exists because one token cannot serve a fleet. A Helm chart that
  creates a DaemonSet plus replicated controllers needs one redemption per
  host, and single-use tokens admit exactly one.

  The default is `1`, which is the historical behaviour and stays the default
  everywhere: a token minted without asking for a budget is single-use, and
  replaying it still answers `:already_redeemed` rather than the budgeted
  `:budget_exhausted`.
  """

  import Bitwise

  alias NeonFS.Cluster.State

  @token_prefix "nfs_inv"

  @type invite_token :: String.t()
  @type duration :: pos_integer()
  @type uses :: pos_integer()

  @default_uses 1

  @doc """
  Creates a new invite token valid for the specified duration.

  ## Parameters
  - `expires_in` - Duration in seconds the token is valid for
  - `uses` - How many times the token may be redeemed (default `1`)

  ## Returns
  - `{:ok, token}` on success
  - `{:error, :cluster_not_initialized}` if cluster state doesn't exist

  ## Examples

      iex> NeonFS.Cluster.Invite.create_invite(3600)
      {:ok, "nfs_inv_abc123_1234567890_1_def456"}
  """
  @spec create_invite(duration(), uses()) ::
          {:ok, invite_token()} | {:error, :cluster_not_initialized}
  def create_invite(expires_in, uses \\ @default_uses)
      when is_integer(expires_in) and expires_in > 0 and is_integer(uses) and uses > 0 do
    case State.load() do
      {:ok, state} ->
        token = generate_token(state.master_key, expires_in, uses)
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

      iex> NeonFS.Cluster.Invite.validate_invite("nfs_inv_abc123_1234567890_1_def456")
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
         {:ok, {random, expiry, uses, signature}} <- parse_token(token),
         :ok <- check_expiry(expiry) do
      verify_signature(state.master_key, random, expiry, uses, signature)
    end
  end

  @doc """
  Parses a token into its components without verifying it.

  Public so redemption can read the budget it has to enforce; the signature
  check that makes those components trustworthy is `validate_invite/1`.
  """
  @spec parse(invite_token()) ::
          {:ok, {String.t(), integer(), uses(), String.t()}} | {:error, :invalid_format}
  def parse(token) when is_binary(token), do: parse_token(token)

  # Private functions

  defp load_cluster_state do
    case State.load() do
      {:ok, state} -> {:ok, state}
      {:error, :not_found} -> {:error, :cluster_not_initialized}
      {:error, reason} -> {:error, reason}
    end
  end

  defp generate_token(master_key, expires_in, uses) do
    random = generate_random_part()
    expiry = DateTime.utc_now() |> DateTime.add(expires_in, :second) |> DateTime.to_unix()
    signature = compute_signature(master_key, random, expiry, uses)

    "#{@token_prefix}_#{random}_#{expiry}_#{uses}_#{signature}"
  end

  defp generate_random_part do
    :crypto.strong_rand_bytes(16)
    |> Base.encode32(case: :lower, padding: false)
    |> binary_part(0, 16)
  end

  defp compute_signature(master_key, random, expiry, uses) do
    payload = signing_payload(random, Integer.to_string(expiry), Integer.to_string(uses))

    :crypto.mac(:hmac, :sha256, master_key, payload)
    |> Base.encode32(case: :lower, padding: false)
    |> binary_part(0, 16)
  end

  defp parse_token(token) do
    case String.split(token, "_") do
      ["nfs", "inv", random, expiry_str, uses_str, signature] ->
        with {expiry, ""} <- Integer.parse(expiry_str),
             {uses, ""} when uses > 0 <- Integer.parse(uses_str) do
          {:ok, {random, expiry, uses, signature}}
        else
          _ -> {:error, :invalid_format}
        end

      _ ->
        {:error, :invalid_format}
    end
  end

  @doc """
  The HMAC payload a token's signature covers.

  Shared with `NeonFS.Cluster.InviteRedemption`, which reconstructs the token
  from the components a joining node sends. The two have to agree byte for
  byte — a payload that differs by a separator produces a token whose
  response the joining node cannot decrypt, and the symptom is a decryption
  failure rather than anything naming the signature.
  """
  @spec signing_payload(String.t(), String.t(), String.t()) :: String.t()
  def signing_payload(random, expiry_str, uses_str) do
    "#{random}_#{expiry_str}_#{uses_str}"
  end

  defp check_expiry(expiry) do
    now = DateTime.utc_now() |> DateTime.to_unix()

    if now < expiry do
      :ok
    else
      {:error, :expired}
    end
  end

  defp verify_signature(master_key, random, expiry, uses, provided_signature) do
    expected_signature = compute_signature(master_key, random, expiry, uses)

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
