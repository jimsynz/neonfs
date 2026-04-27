defmodule NeonFS.WebDAV.HealthPlug do
  @moduledoc """
  Plug that wraps Davy.Plug with health endpoint and degraded mode handling.

  Intercepts `GET /health` to return cluster health as JSON. For all other
  requests, checks cluster status and either blocks write operations with
  `503 Service Unavailable` when the cluster is degraded/unavailable, or
  adds `X-NeonFS-Status` headers and delegates to the inner WebDAV plug.

  ## `If-None-Match: *` capture

  RFC 7232 conditional creates need to thread through to
  `WriteOperation.write_file_streamed/4` as `create_only: true`, but
  `Davy.Plug` only forwards `:content_type` to the backend's
  `put_content_stream/4`. We capture the `If-None-Match` request
  header here (each Bandit request runs in its own process, so the
  process dictionary is request-scoped) and `Backend.put_content_stream/4`
  reads it back when assembling write_opts. See sub-issue #593.
  """

  @behaviour Plug

  alias NeonFS.WebDAV.HealthCheck

  @write_methods ~w(PUT DELETE MKCOL COPY MOVE PROPPATCH LOCK UNLOCK)
  @if_none_match_key :webdav_if_none_match_star

  @doc """
  Process-dict key the request pipeline uses to forward
  `If-None-Match: *` to `Backend.put_content_stream/4`. Backend reads
  this key; tests poke it directly to drive the create_only branch
  without spinning up the full Plug pipeline.
  """
  @spec if_none_match_star_key() :: atom()
  def if_none_match_star_key, do: @if_none_match_key

  @impl Plug
  @spec init(keyword()) :: map()
  def init(opts) do
    inner_opts = Davy.Plug.init(opts)
    %{inner: inner_opts, core_nodes_fn: Keyword.get(opts, :core_nodes_fn)}
  end

  @impl Plug
  @spec call(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def call(%Plug.Conn{method: "GET", request_path: "/health"} = conn, opts) do
    status = cluster_status(opts)
    http_status = if status.status == :ok, do: 200, else: 503

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(http_status, encode_health(status))
  end

  def call(conn, opts) do
    capture_if_none_match(conn)
    status = cluster_status(opts)

    case status.status do
      :ok ->
        Davy.Plug.call(conn, opts.inner)

      degraded_or_unavailable when conn.method in @write_methods ->
        reason = status.reason || to_string(degraded_or_unavailable)

        conn
        |> Plug.Conn.put_resp_header("retry-after", "30")
        |> Plug.Conn.put_resp_header("x-neonfs-status", reason)
        |> Plug.Conn.send_resp(503, "")

      _degraded_or_unavailable ->
        conn
        |> Plug.Conn.put_resp_header("x-neonfs-status", status.reason || "degraded")
        |> Davy.Plug.call(opts.inner)
    end
  end

  defp capture_if_none_match(conn) do
    if Plug.Conn.get_req_header(conn, "if-none-match") == ["*"] do
      Process.put(@if_none_match_key, true)
    else
      Process.delete(@if_none_match_key)
    end
  end

  defp cluster_status(%{core_nodes_fn: fun}) when is_function(fun, 0) do
    HealthCheck.cluster_status(fun)
  end

  defp cluster_status(_opts), do: HealthCheck.cluster_status()

  defp encode_health(status) do
    reason_json = if status.reason, do: ~s("#{status.reason}"), else: "null"

    ~s({"status":"#{status.status}","writable":#{status.writable},"readable":#{status.readable},"reason":#{reason_json},"quorum_reachable":#{status.quorum_reachable}})
  end
end
