defmodule NeonFS.Client.ChunkReader.StreamError do
  @moduledoc """
  Raised when a chunk or stripe fetch fails partway through a
  `NeonFS.Client.ChunkReader` streaming read.

  The streaming read APIs hand back a lazy `Stream`, so a mid-stream
  fetch failure cannot be reported as a return value once consumption
  has begun — it is raised instead. Halting the stream silently would
  give the consumer a short read indistinguishable from a clean
  end-of-file: a truncated layer for the containerd content proxy, a
  truncated object body for S3/WebDAV, a short NFS READ (#1353).

  The `:reason` field carries the underlying fetch error.
  """
  defexception [:reason]

  @type t :: %__MODULE__{reason: term()}

  @impl true
  def message(%__MODULE__{reason: reason}) do
    "chunk stream fetch failed mid-stream: #{inspect(reason)}"
  end
end
