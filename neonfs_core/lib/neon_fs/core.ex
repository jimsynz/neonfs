defmodule NeonFS.Core do
  @moduledoc """
  Public RPC facade for NeonFS core operations.

  Non-core nodes (S3, FUSE, NFS) call functions in this module via
  `NeonFS.Client.Router`, which dispatches `:rpc.call/5` to a core node.
  """

  alias NeonFS.Core.S3CredentialManager

  @doc """
  Looks up an S3 credential by access key ID.

  Called by the S3 backend during SigV4 authentication.

  Returns `{:ok, %{secret_access_key: ..., identity: ...}}` on success,
  or `{:error, :not_found}` if the access key is unknown.
  """
  @spec lookup_s3_credential(String.t()) :: {:ok, map()} | {:error, :not_found}
  def lookup_s3_credential(access_key_id) do
    case S3CredentialManager.lookup(access_key_id) do
      {:ok, credential} ->
        {:ok, %{secret_access_key: credential.secret_access_key, identity: credential.identity}}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end
end
