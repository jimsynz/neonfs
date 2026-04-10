# S3Server

A Plug-based library for building S3-compatible object storage APIs in Elixir.

S3Server handles the S3 HTTP protocol — signature verification, XML response
formatting, operation routing, multipart upload orchestration — and delegates
actual storage operations to a user-provided backend module implementing the
`S3Server.Backend` behaviour.

## Installation

Add `s3_server` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:s3_server, "~> 0.1.0"}
  ]
end
```

## Usage

Define a backend module implementing `S3Server.Backend`:

```elixir
defmodule MyApp.S3Backend do
  @behaviour S3Server.Backend

  @impl true
  def lookup_credential(access_key_id) do
    # Return {:ok, %S3Server.Credential{}} or {:error, :not_found}
  end

  @impl true
  def list_buckets(_context) do
    {:ok, [%S3Server.Bucket{name: "my-bucket", creation_date: ~U[2024-01-01 00:00:00Z]}]}
  end

  # ... implement remaining callbacks
end
```

Mount the plug in your router:

```elixir
forward "/s3", S3Server.Plug, backend: MyApp.S3Backend, region: "us-east-1"
```
