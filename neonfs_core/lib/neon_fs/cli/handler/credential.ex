defmodule NeonFS.CLI.Handler.Credential do
  @moduledoc """
  CLI command handlers for the interface-agnostic credential store.

  Credentials are access-key / secret-key pairs bound to an identity and
  a POSIX uid/gids, usable by every interface that authenticates against
  the shared store (S3 SigV4, WebDAV Basic auth). The lifecycle commands
  here back the `neonfs credential` command group; `NeonFS.CLI.Handler`
  delegates its `handle_credential_*` RPC entry points to this module.

  The identity is a label; the uid and gids are what requests are
  authorised as. `NeonFS.Core.CredentialManager` records why.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.{AuditLog, CredentialManager}
  alias NeonFS.Error.{Invalid, NotFound}

  @doc """
  Creates a new credential bound to `identity`. Returns the credential
  including the secret key (shown once).

  `opts` carries `"uid"` and `"gids"` — the POSIX identity requests made
  with this credential are authorised as. A credential created without a
  uid authenticates and is then refused every operation, deliberately:
  core reads an absent uid as 0, which `NeonFS.Core.Authorise.check/4`
  passes unconditionally.
  """
  @spec handle_credential_create(term(), map()) :: {:ok, map()} | {:error, term()}
  def handle_credential_create(identity, opts \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, posix} <- posix_identity(opts),
         {:ok, credential} <- CredentialManager.create(identity, posix) do
      AuditLog.log_event(
        event_type: :credential_created,
        actor_uid: 0,
        resource: credential.access_key_id,
        details: %{identity: identity, uid: credential.uid, gids: credential.gids}
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
    Map.take(credential, [
      :access_key_id,
      :secret_access_key,
      :identity,
      :uid,
      :gids,
      :created_at
    ])
  end

  # A uid that is present but not a non-negative integer is a typo the
  # operator can still fix; one that reaches the store is a credential
  # authorised as something they did not mean.
  defp posix_identity(opts) do
    with {:ok, uid} <- validate_uid(Map.get(opts, "uid") || Map.get(opts, :uid)),
         {:ok, gids} <- validate_gids(Map.get(opts, "gids") || Map.get(opts, :gids) || []) do
      {:ok, uid: uid, gids: gids}
    end
  end

  defp validate_uid(nil), do: {:ok, nil}
  defp validate_uid(uid) when is_integer(uid) and uid >= 0, do: {:ok, uid}

  defp validate_uid(_uid),
    do: {:error, Invalid.exception(message: "uid must be a non-negative integer")}

  defp validate_gids(gids) when is_list(gids) do
    if Enum.all?(gids, &(is_integer(&1) and &1 >= 0)) do
      {:ok, gids}
    else
      {:error, Invalid.exception(message: "gids must be non-negative integers")}
    end
  end

  defp validate_gids(_gids),
    do: {:error, Invalid.exception(message: "gids must be a list of non-negative integers")}
end
