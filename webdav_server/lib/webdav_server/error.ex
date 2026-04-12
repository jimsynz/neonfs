defmodule WebdavServer.Error do
  @moduledoc """
  WebDAV error with HTTP status code mapping.

  Backend callbacks return `{:error, %WebdavServer.Error{}}` to signal failures.
  The `code` atom maps to an HTTP status code via `status_code/1`.
  """

  @type t :: %__MODULE__{
          code: atom(),
          message: String.t() | nil
        }

  @enforce_keys [:code]
  defstruct [:code, :message]

  @status_codes %{
    bad_request: 400,
    unauthorized: 401,
    forbidden: 403,
    not_found: 404,
    method_not_allowed: 405,
    conflict: 409,
    precondition_failed: 412,
    unsupported_media_type: 415,
    locked: 423,
    failed_dependency: 424,
    insufficient_storage: 507
  }

  @doc """
  Returns the HTTP status code for the given error code atom.
  """
  @spec status_code(atom()) :: pos_integer()
  def status_code(code) when is_map_key(@status_codes, code), do: Map.fetch!(@status_codes, code)
  def status_code(_code), do: 500
end
