# WebdavServer

A Plug-based library for building WebDAV-compatible file servers in Elixir.

WebdavServer handles the WebDAV HTTP protocol (RFC 4918) — PROPFIND/PROPPATCH
XML processing, multistatus responses, COPY/MOVE semantics, locking, and client
compatibility — and delegates actual storage operations to a user-provided
backend module implementing the `WebdavServer.Backend` behaviour.

## Quick start

1. Add `webdav_server` to your dependencies:

```elixir
def deps do
  [{:webdav_server, "~> 0.1.0"}]
end
```

2. Implement the `WebdavServer.Backend` behaviour
3. Mount `WebdavServer.Plug` in your router:

```elixir
forward "/dav", WebdavServer.Plug, backend: MyApp.DavBackend
```

## Features

- **Class 1 and 2 WebDAV compliance** — all required methods including locking
- **Pluggable backend** — implement a behaviour to connect any storage system
- **Client compatibility** — handles macOS Finder, Windows Explorer, and davfs2 quirks
- **Namespace-aware XML** — correct handling of DAV: namespace with arbitrary prefixes
- **Pluggable lock store** — built-in ETS store or provide your own for distributed locking

## Supported methods

| Method | Description |
|--------|-------------|
| OPTIONS | Capability discovery |
| GET / HEAD | Read file content |
| PUT | Create or replace files |
| DELETE | Remove resources (recursive for collections) |
| MKCOL | Create collections (directories) |
| COPY | Copy resources |
| MOVE | Move/rename resources |
| PROPFIND | Retrieve resource properties |
| PROPPATCH | Modify resource properties |
| LOCK | Acquire write locks |
| UNLOCK | Release write locks |

## References

- [RFC 4918 — WebDAV](https://www.rfc-editor.org/rfc/rfc4918)
