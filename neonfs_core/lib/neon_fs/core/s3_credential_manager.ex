defmodule NeonFS.Core.S3CredentialManager do
  @moduledoc """
  Stateless facade over Ra-backed S3 credentials.

  Reads go through `RaSupervisor.local_query/2` against the
  `MetadataStateMachine`; writes go through `RaSupervisor.command/2`.
  There is no ETS cache and no DETS snapshot — state lives entirely
  in Ra and every read reflects the locally-committed view.

  Maps S3 access key IDs to secret keys and user identities, enabling
  AWS SigV4 authentication in the S3-compatible API.
  """

  alias NeonFS.Core.{MetadataStateMachine, RaSupervisor}

  @type access_key_id :: String.t()
  @type secret_access_key :: String.t()

  @type credential :: %{
          access_key_id: access_key_id(),
          secret_access_key: secret_access_key(),
          identity: term(),
          created_at: DateTime.t()
        }

  @doc """
  Creates a new S3 credential for the given user identity.

  Generates a random access key ID and secret access key, persists
  them via Ra, and returns the full credential (including the secret
  — only shown once).
  """
  @spec create(term()) :: {:ok, credential()} | {:error, term()}
  def create(identity) do
    credential = %{
      access_key_id: generate_access_key_id(),
      secret_access_key: generate_secret_access_key(),
      identity: identity,
      created_at: DateTime.utc_now()
    }

    case ra_command({:put_s3_credential, credential}) do
      :ok -> {:ok, credential}
      {:error, _} = error -> error
    end
  end

  @doc """
  Deletes an S3 credential by its access key ID.
  """
  @spec delete(access_key_id()) :: :ok | {:error, term()}
  def delete(access_key_id) do
    case lookup(access_key_id) do
      {:ok, _} -> ra_command({:delete_s3_credential, access_key_id})
      {:error, :not_found} -> {:error, :not_found}
    end
  end

  @doc """
  Rotates the secret access key for an existing S3 credential.

  Generates a new secret key while keeping the same access key ID and
  identity. Returns the updated credential including the new secret
  (shown once). Concurrent rotates on the same key are last-write-wins.
  """
  @spec rotate(access_key_id()) :: {:ok, credential()} | {:error, term()}
  def rotate(access_key_id) do
    with {:ok, cred} <- lookup(access_key_id) do
      rotated = %{cred | secret_access_key: generate_secret_access_key()}

      case ra_command({:put_s3_credential, rotated}) do
        :ok -> {:ok, rotated}
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Lists all S3 credentials, optionally filtered by identity.

  Returns credentials without their secret keys for security.
  """
  @spec list(keyword()) :: [map()]
  def list(opts \\ []) do
    identity_filter = Keyword.get(opts, :identity)

    case read_credentials() do
      {:ok, creds_map} ->
        creds_map
        |> Map.values()
        |> Enum.map(&map_to_credential/1)
        |> Enum.map(&Map.delete(&1, :secret_access_key))
        |> filter_by_identity(identity_filter)
        |> Enum.sort_by(& &1.created_at, DateTime)

      {:error, _} ->
        []
    end
  end

  @doc """
  Looks up a credential by access key ID.

  Returns the full credential including the secret key (needed for
  signature verification).
  """
  @spec lookup(access_key_id()) :: {:ok, credential()} | {:error, :not_found}
  def lookup(access_key_id) do
    case read_credential(access_key_id) do
      {:ok, cred_map} when is_map(cred_map) -> {:ok, map_to_credential(cred_map)}
      {:ok, nil} -> {:error, :not_found}
      {:error, _} -> {:error, :not_found}
    end
  end

  # Private

  defp read_credential(access_key_id) do
    RaSupervisor.local_query(&MetadataStateMachine.get_s3_credential(&1, access_key_id))
  catch
    :exit, _ -> {:error, :ra_not_available}
  end

  defp read_credentials do
    RaSupervisor.local_query(&MetadataStateMachine.get_s3_credentials/1)
  catch
    :exit, _ -> {:error, :ra_not_available}
  end

  defp filter_by_identity(creds, nil), do: creds

  defp filter_by_identity(creds, identity) do
    Enum.filter(creds, fn c -> c.identity == identity end)
  end

  defp map_to_credential(cred_map) when is_map(cred_map) do
    %{
      access_key_id: cred_map.access_key_id,
      secret_access_key: cred_map.secret_access_key,
      identity: cred_map[:identity],
      created_at: cred_map[:created_at] || DateTime.utc_now()
    }
  end

  defp generate_access_key_id do
    "NEONFS" <> random_alphanumeric(14)
  end

  defp generate_secret_access_key do
    :crypto.strong_rand_bytes(30) |> Base.encode64(padding: false)
  end

  defp random_alphanumeric(length) do
    alphabet = ~c"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    :crypto.strong_rand_bytes(length)
    |> :binary.bin_to_list()
    |> Enum.map(fn byte -> Enum.at(alphabet, rem(byte, length(alphabet))) end)
    |> List.to_string()
  end

  defp ra_command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, :ok, _leader} -> :ok
      {:ok, {:error, reason}, _leader} -> {:error, reason}
      {:ok, other, _leader} -> {:error, {:unexpected_reply, other}}
      {:error, :noproc} -> {:error, :ra_not_available}
      {:error, reason} -> {:error, reason}
      {:timeout, _node} -> {:error, :timeout}
    end
  catch
    :exit, _ -> {:error, :ra_not_available}
  end
end
