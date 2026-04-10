defmodule S3Server.Object do
  @moduledoc """
  Represents an S3 object returned by GetObject.
  """

  @type t :: %__MODULE__{
          body: iodata(),
          content_type: String.t(),
          content_length: non_neg_integer(),
          etag: String.t(),
          last_modified: DateTime.t(),
          metadata: %{String.t() => String.t()}
        }

  @enforce_keys [:body, :content_length, :etag, :last_modified]
  defstruct [
    :body,
    :content_length,
    :etag,
    :last_modified,
    content_type: "application/octet-stream",
    metadata: %{}
  ]
end
