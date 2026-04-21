defmodule S3Server.Backend do
  @moduledoc """
  Behaviour for S3 storage backends.

  Implement this behaviour to provide actual storage operations behind the
  S3-compatible HTTP interface. All callbacks receive an auth context map
  containing the authenticated identity from `lookup_credential/1`.

  Multipart upload callbacks are optional — the server returns 501 Not
  Implemented for multipart operations if they are not defined.
  """

  @type auth_context :: %{
          access_key_id: String.t(),
          identity: term()
        }

  @type bucket :: String.t()
  @type key :: String.t()
  @type s3_error :: S3Server.Error.t()

  # Credential lookup (required for SigV4 verification)
  @callback lookup_credential(access_key_id :: String.t()) ::
              {:ok, S3Server.Credential.t()} | {:error, :not_found}

  # Bucket operations
  @callback list_buckets(auth_context()) ::
              {:ok, [S3Server.Bucket.t()]} | {:error, s3_error()}

  @callback create_bucket(auth_context(), bucket()) ::
              :ok | {:error, s3_error()}

  @callback delete_bucket(auth_context(), bucket()) ::
              :ok | {:error, s3_error()}

  @callback head_bucket(auth_context(), bucket()) ::
              :ok | {:error, s3_error()}

  @callback get_bucket_location(auth_context(), bucket()) ::
              {:ok, String.t()} | {:error, s3_error()}

  # Object operations
  @callback get_object(auth_context(), bucket(), key(), S3Server.GetOpts.t()) ::
              {:ok, S3Server.Object.t()} | {:error, s3_error()}

  @callback put_object(auth_context(), bucket(), key(), body :: iodata(), S3Server.PutOpts.t()) ::
              {:ok, etag :: String.t()} | {:error, s3_error()}

  # Streaming PutObject (optional). When implemented the Plug routes
  # request bodies through this callback instead of `put_object/5` so
  # multi-GB uploads are not buffered in memory.
  @callback put_object_stream(
              auth_context(),
              bucket(),
              key(),
              body :: Enumerable.t(),
              S3Server.PutOpts.t()
            ) ::
              {:ok, etag :: String.t()} | {:error, s3_error()}

  @callback delete_object(auth_context(), bucket(), key()) ::
              :ok | {:error, s3_error()}

  @callback delete_objects(auth_context(), bucket(), [key()]) ::
              {:ok, S3Server.DeleteResult.t()} | {:error, s3_error()}

  @callback head_object(auth_context(), bucket(), key()) ::
              {:ok, S3Server.ObjectMeta.t()} | {:error, s3_error()}

  @callback list_objects_v2(auth_context(), bucket(), S3Server.ListOpts.t()) ::
              {:ok, S3Server.ListResult.t()} | {:error, s3_error()}

  @callback copy_object(
              auth_context(),
              bucket(),
              key(),
              source_bucket :: bucket(),
              source_key :: key()
            ) ::
              {:ok, S3Server.CopyResult.t()} | {:error, s3_error()}

  # Multipart upload (optional)
  @callback create_multipart_upload(auth_context(), bucket(), key(), opts :: map()) ::
              {:ok, upload_id :: String.t()} | {:error, s3_error()}

  @callback upload_part(
              auth_context(),
              bucket(),
              key(),
              upload_id :: String.t(),
              part_number :: pos_integer(),
              body :: iodata()
            ) ::
              {:ok, etag :: String.t()} | {:error, s3_error()}

  # Streaming UploadPart (optional). Same rationale as `put_object_stream/5`
  # — S3 parts can be up to 5 GiB and must not be buffered in memory.
  @callback upload_part_stream(
              auth_context(),
              bucket(),
              key(),
              upload_id :: String.t(),
              part_number :: pos_integer(),
              body :: Enumerable.t()
            ) ::
              {:ok, etag :: String.t()} | {:error, s3_error()}

  @callback complete_multipart_upload(
              auth_context(),
              bucket(),
              key(),
              upload_id :: String.t(),
              parts :: [{pos_integer(), String.t()}]
            ) ::
              {:ok, S3Server.CompleteResult.t()} | {:error, s3_error()}

  @callback abort_multipart_upload(auth_context(), bucket(), key(), upload_id :: String.t()) ::
              :ok | {:error, s3_error()}

  @callback list_multipart_uploads(auth_context(), bucket(), opts :: map()) ::
              {:ok, S3Server.MultipartList.t()} | {:error, s3_error()}

  @callback list_parts(auth_context(), bucket(), key(), upload_id :: String.t(), opts :: map()) ::
              {:ok, S3Server.PartList.t()} | {:error, s3_error()}

  @optional_callbacks [
    create_multipart_upload: 4,
    upload_part: 6,
    upload_part_stream: 6,
    complete_multipart_upload: 5,
    abort_multipart_upload: 4,
    list_multipart_uploads: 3,
    list_parts: 5,
    put_object_stream: 5
  ]
end
