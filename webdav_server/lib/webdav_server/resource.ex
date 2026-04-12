defmodule WebdavServer.Resource do
  @moduledoc """
  Represents a WebDAV resource returned by the backend.

  The backend populates standard metadata fields that the WebDAV server needs
  for property responses and conditional request handling. The `backend_data`
  field carries opaque data passed back to the backend on subsequent calls.
  """

  @type t :: %__MODULE__{
          path: [String.t()],
          type: :file | :collection,
          etag: String.t() | nil,
          content_type: String.t() | nil,
          content_length: non_neg_integer() | nil,
          last_modified: DateTime.t() | nil,
          creation_date: DateTime.t() | nil,
          display_name: String.t() | nil,
          backend_data: term()
        }

  @enforce_keys [:path, :type]
  defstruct [
    :path,
    :type,
    :etag,
    :content_type,
    :content_length,
    :last_modified,
    :creation_date,
    :display_name,
    :backend_data
  ]
end
