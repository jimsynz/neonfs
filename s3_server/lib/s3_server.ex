defmodule S3Server do
  @moduledoc """
  A Plug-based library for building S3-compatible object storage APIs.

  S3Server handles the S3 HTTP protocol — signature verification, XML response
  formatting, operation routing, multipart upload orchestration — and delegates
  actual storage operations to a user-provided backend module implementing the
  `S3Server.Backend` behaviour.

  ## Quick start

  1. Implement the `S3Server.Backend` behaviour
  2. Mount `S3Server.Plug` in your router

  ```elixir
  forward "/s3", S3Server.Plug, backend: MyApp.S3Backend, region: "us-east-1"
  ```
  """
end
