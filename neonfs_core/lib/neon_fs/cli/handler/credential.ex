defmodule NeonFS.CLI.Handler.Credential do
  @moduledoc """
  CLI command handlers for the interface-agnostic credential store.

  Credentials are access-key / secret-key pairs bound to an identity and
  usable by every interface that authenticates against the shared store
  (S3 SigV4, WebDAV Basic auth). The lifecycle commands here back the
  `neonfs credential` command group; `NeonFS.CLI.Handler` delegates its
  `handle_credential_*` RPC entry points to this module.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.{AuditLog, CredentialManager}
  alias NeonFS.Error.NotFound

  @doc """
  Creates a new credential bound to `identity`. Returns the credential
  including the secret key (shown once).
  """
  @spec handle_credential_create(term()) :: {:ok, map()} | {:error, term()}
  def handle_credential_create(identity) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, credential} <- CredentialManager.create(identity) do
      AuditLog.log_event(
        event_type: :credential_created,
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
  Lists credentials, optionally filtered by `:identity` (secrets
  redacted).
  """
  @spec handle_credential_list(map()) :: {:ok, [map()]}
  def handle_credential_list(filters \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      opts =
        case Map.get(filters, "identity") || Map.get(filters, :identity) do
          nil -> []
          id -> [identity: id]
        end

      credentials =
        CredentialManager.list(opts)
        |> Enum.map(&credential_to_serialisable/1)

      {:ok, credentials}
    end
  end

  @doc """
  Deletes a credential by access key ID.
  """
  @spec handle_credential_delete(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_credential_delete(access_key_id) when is_binary(access_key_id) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         :ok <- CredentialManager.delete(access_key_id) do
      AuditLog.log_event(
        event_type: :credential_deleted,
        actor_uid: 0,
        resource: access_key_id,
        details: %{}
      )

      {:ok, %{}}
    else
      {:error, :not_found} ->
        {:error, NotFound.exception(message: "Credential '#{access_key_id}' not found")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Rotates the secret access key for a credential. Returns the updated
  credential with the new secret (shown once).
  """
  @spec handle_credential_rotate(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_credential_rotate(access_key_id) when is_binary(access_key_id) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, credential} <- CredentialManager.rotate(access_key_id) do
      AuditLog.log_event(
        event_type: :credential_rotated,
        actor_uid: 0,
        resource: access_key_id,
        details: %{}
      )

      {:ok, credential_to_serialisable(credential)}
    else
      {:error, :not_found} ->
        {:error, NotFound.exception(message: "Credential '#{access_key_id}' not found")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Shows details of a single credential by access key ID (secret
  redacted).
  """
  @spec handle_credential_show(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_credential_show(access_key_id) when is_binary(access_key_id) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, credential} <- CredentialManager.lookup(access_key_id) do
      {:ok, credential |> Map.delete(:secret_access_key) |> credential_to_serialisable()}
    else
      {:error, :not_found} ->
        {:error, NotFound.exception(message: "Credential '#{access_key_id}' not found")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  # Private

  defp credential_to_serialisable(credential) do
    Map.take(credential, [:access_key_id, :secret_access_key, :identity, :created_at])
  end
end
