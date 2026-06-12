defmodule NeonFS.CLI.Handler.S3 do
  @moduledoc """
  CLI command handlers for the S3-compatible interface: access-key
  credential lifecycle and the volumes-as-buckets views.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `handle_s3_*` RPC entry points here, so the CLI wire
  contract is unchanged.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.{AuditLog, S3CredentialManager, VolumeRegistry}
  alias NeonFS.Error.NotFound

  @doc """
  Creates a new S3 credential bound to `identity`. Returns the
  credential including the secret key (shown once).
  """
  @spec handle_s3_create_credential(term()) :: {:ok, map()} | {:error, term()}
  def handle_s3_create_credential(identity) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, credential} <- S3CredentialManager.create(identity) do
      AuditLog.log_event(
        event_type: :s3_credential_created,
        actor_uid: 0,
        resource: credential.access_key_id,
        details: %{identity: identity}
      )

      {:ok, credential_to_serialisable(credential)}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Lists S3 credentials, optionally filtered by `:identity` (secrets
  redacted).
  """
  @spec handle_s3_list_credentials(map()) :: {:ok, [map()]}
  def handle_s3_list_credentials(filters \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      opts =
        case Map.get(filters, "identity") || Map.get(filters, :identity) do
          nil -> []
          id -> [identity: id]
        end

      credentials =
        S3CredentialManager.list(opts)
        |> Enum.map(&credential_to_serialisable/1)

      {:ok, credentials}
    end
  end

  @doc """
  Deletes an S3 credential by access key ID.
  """
  @spec handle_s3_delete_credential(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_s3_delete_credential(access_key_id) when is_binary(access_key_id) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         :ok <- S3CredentialManager.delete(access_key_id) do
      AuditLog.log_event(
        event_type: :s3_credential_deleted,
        actor_uid: 0,
        resource: access_key_id,
        details: %{}
      )

      {:ok, %{}}
    else
      {:error, :not_found} ->
        {:error, NotFound.exception(message: "S3 credential '#{access_key_id}' not found")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Rotates the secret access key for an S3 credential. Returns the
  updated credential with the new secret (shown once).
  """
  @spec handle_s3_rotate_credential(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_s3_rotate_credential(access_key_id) when is_binary(access_key_id) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, credential} <- S3CredentialManager.rotate(access_key_id) do
      AuditLog.log_event(
        event_type: :s3_credential_rotated,
        actor_uid: 0,
        resource: access_key_id,
        details: %{}
      )

      {:ok, credential_to_serialisable(credential)}
    else
      {:error, :not_found} ->
        {:error, NotFound.exception(message: "S3 credential '#{access_key_id}' not found")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Shows details of a single S3 credential by access key ID (secret
  redacted).
  """
  @spec handle_s3_show_credential(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_s3_show_credential(access_key_id) when is_binary(access_key_id) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, credential} <- S3CredentialManager.lookup(access_key_id) do
      {:ok, credential |> Map.delete(:secret_access_key) |> credential_to_serialisable()}
    else
      {:error, :not_found} ->
        {:error, NotFound.exception(message: "S3 credential '#{access_key_id}' not found")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Lists all volumes available as S3 buckets.
  """
  @spec handle_s3_list_buckets() :: {:ok, [map()]}
  def handle_s3_list_buckets do
    set_cli_metadata()

    with :ok <- require_cluster() do
      buckets =
        VolumeRegistry.list()
        |> Enum.map(&volume_to_bucket/1)
        |> Enum.sort_by(& &1.name)

      {:ok, buckets}
    end
  end

  @doc """
  Shows details of a single S3 bucket (volume).
  """
  @spec handle_s3_show_bucket(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_s3_show_bucket(bucket_name) when is_binary(bucket_name) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case VolumeRegistry.get_by_name(bucket_name) do
        {:ok, volume} ->
          {:ok, volume_to_bucket(volume)}

        {:error, :not_found} ->
          {:error, NotFound.exception(message: "Bucket '#{bucket_name}' not found")}
      end
    end
  end

  # Private

  defp volume_to_bucket(volume) do
    %{
      name: volume.name,
      created_at: DateTime.to_iso8601(volume.created_at),
      durability: volume.durability,
      compression: volume.compression,
      logical_size: volume.logical_size,
      physical_size: volume.physical_size
    }
  end

  defp credential_to_serialisable(credential) do
    Map.take(credential, [:access_key_id, :secret_access_key, :identity, :created_at])
  end
end
