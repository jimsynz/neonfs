defmodule WebdavServer do
  @moduledoc """
  A Plug-based library for building WebDAV-compatible file servers in Elixir.

  WebdavServer handles the WebDAV HTTP protocol (RFC 4918) — PROPFIND/PROPPATCH
  XML processing, multistatus responses, COPY/MOVE semantics, locking, and client
  compatibility — and delegates actual storage operations to a user-provided
  backend module implementing the `WebdavServer.Backend` behaviour.

  ## Quick start

  1. Implement the `WebdavServer.Backend` behaviour
  2. Mount `WebdavServer.Plug` in your router

  ```elixir
  forward "/dav", WebdavServer.Plug, backend: MyApp.DavBackend
  ```
  """
end
