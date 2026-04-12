defmodule WebdavServer.Test.WebdavClient do
  @moduledoc """
  Thin Req-based WebDAV client for integration tests.

  Wraps standard and WebDAV-specific HTTP methods so tests exercise the
  full HTTP stack (Bandit → Plug → Handler → Backend) rather than
  calling through Plug.Test.
  """

  @spec options(pos_integer(), String.t()) :: Req.Response.t()
  def options(port, path) do
    request!(port, method: :options, url: path)
  end

  @spec get(pos_integer(), String.t()) :: Req.Response.t()
  def get(port, path) do
    request!(port, method: :get, url: path)
  end

  @spec head(pos_integer(), String.t()) :: Req.Response.t()
  def head(port, path) do
    request!(port, method: :head, url: path)
  end

  @spec put(pos_integer(), String.t(), iodata(), keyword()) :: Req.Response.t()
  def put(port, path, body, opts \\ []) do
    content_type = Keyword.get(opts, :content_type, "application/octet-stream")

    request!(port,
      method: :put,
      url: path,
      body: body,
      headers: [{"content-type", content_type}]
    )
  end

  @spec delete(pos_integer(), String.t()) :: Req.Response.t()
  def delete(port, path) do
    request!(port, method: :delete, url: path)
  end

  @spec mkcol(pos_integer(), String.t()) :: Req.Response.t()
  def mkcol(port, path) do
    request!(port, method: :mkcol, url: path)
  end

  @spec copy(pos_integer(), String.t(), String.t(), keyword()) :: Req.Response.t()
  def copy(port, source, destination, opts \\ []) do
    overwrite = if Keyword.get(opts, :overwrite, true), do: "T", else: "F"

    request!(port,
      method: :copy,
      url: source,
      headers: [
        {"destination", "http://localhost:#{port}#{destination}"},
        {"overwrite", overwrite}
      ]
    )
  end

  @spec move(pos_integer(), String.t(), String.t(), keyword()) :: Req.Response.t()
  def move(port, source, destination, opts \\ []) do
    overwrite = if Keyword.get(opts, :overwrite, true), do: "T", else: "F"

    request!(port,
      method: :move,
      url: source,
      headers: [
        {"destination", "http://localhost:#{port}#{destination}"},
        {"overwrite", overwrite}
      ]
    )
  end

  @spec propfind(pos_integer(), String.t(), keyword()) :: Req.Response.t()
  def propfind(port, path, opts \\ []) do
    depth = Keyword.get(opts, :depth, "0")
    body = Keyword.get(opts, :body, nil)

    headers = [{"depth", depth}, {"content-type", "application/xml"}]

    request!(port, method: :propfind, url: path, body: body, headers: headers)
  end

  @spec proppatch(pos_integer(), String.t(), String.t()) :: Req.Response.t()
  def proppatch(port, path, body) do
    request!(port,
      method: :proppatch,
      url: path,
      body: body,
      headers: [{"content-type", "application/xml"}]
    )
  end

  @spec lock(pos_integer(), String.t(), keyword()) :: Req.Response.t()
  def lock(port, path, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, "Second-1800")

    body =
      Keyword.get_lazy(opts, :body, fn ->
        scope = Keyword.get(opts, :scope, "exclusive")

        """
        <?xml version="1.0" encoding="utf-8"?>
        <D:lockinfo xmlns:D="DAV:">
          <D:lockscope><D:#{scope}/></D:lockscope>
          <D:locktype><D:write/></D:locktype>
        </D:lockinfo>
        """
      end)

    request!(port,
      method: :lock,
      url: path,
      body: body,
      headers: [{"content-type", "application/xml"}, {"timeout", timeout}]
    )
  end

  @spec unlock(pos_integer(), String.t(), String.t()) :: Req.Response.t()
  def unlock(port, path, lock_token) do
    request!(port,
      method: :unlock,
      url: path,
      headers: [{"lock-token", lock_token}]
    )
  end

  @standard_methods ~w(get post put patch delete head options)a

  defp request!(port, opts) do
    url = Keyword.fetch!(opts, :url)
    method = Keyword.fetch!(opts, :method)
    opts = opts |> Keyword.delete(:url) |> Keyword.delete(:method)

    # Finch only accepts standard HTTP methods as atoms; WebDAV methods
    # (PROPFIND, MKCOL, etc.) must be passed as uppercase strings.
    method =
      if method in @standard_methods do
        method
      else
        method |> Atom.to_string() |> String.upcase()
      end

    Req.request!(
      [
        base_url: "http://localhost:#{port}",
        url: url,
        method: method,
        redirect: false,
        retry: false,
        decode_body: false
      ] ++ opts
    )
  end
end
