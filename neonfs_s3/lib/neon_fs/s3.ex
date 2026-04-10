defmodule NeonFS.S3 do
  @moduledoc """
  S3-compatible API interface for NeonFS volumes.

  Maps S3 buckets to NeonFS volumes and S3 object keys to file paths,
  communicating with core nodes via `NeonFS.Client.Router`.
  """
end
