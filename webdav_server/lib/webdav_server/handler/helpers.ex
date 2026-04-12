defmodule WebdavServer.Handler.Helpers do
  @moduledoc false

  alias WebdavServer.Error

  @doc """
  Extract the resource path from the conn as a list of decoded URI segments.
  """
  @spec resource_path(Plug.Conn.t()) :: [String.t()]
  def resource_path(conn) do
    conn.path_info
    |> Enum.map(&URI.decode/1)
  end

  @doc """
  Build the full href for a resource path, including the script_name prefix.
  """
  @spec href(Plug.Conn.t(), [String.t()]) :: String.t()
  def href(conn, path) do
    prefix = Enum.map_join(conn.script_name, "/", &URI.encode/1)
    suffix = Enum.map_join(path, "/", &URI.encode/1)
    "/#{prefix}/#{suffix}" |> String.replace("//", "/")
  end

  @doc """
  Parse the `Depth` header. Returns 0, 1, or :infinity.
  """
  @spec parse_depth(Plug.Conn.t(), 0 | 1 | :infinity) :: 0 | 1 | :infinity
  def parse_depth(conn, default \\ :infinity) do
    case Plug.Conn.get_req_header(conn, "depth") do
      ["0"] -> 0
      ["1"] -> 1
      ["infinity"] -> :infinity
      _ -> default
    end
  end

  @doc """
  Parse the `Destination` header into a path segment list relative to the mount point.
  """
  @spec parse_destination(Plug.Conn.t()) :: {:ok, [String.t()]} | {:error, :bad_request}
  def parse_destination(conn) do
    case Plug.Conn.get_req_header(conn, "destination") do
      [dest_uri] ->
        uri = URI.parse(dest_uri)
        path = uri.path || "/"
        prefix = "/" <> Enum.join(conn.script_name, "/")

        stripped =
          if String.starts_with?(path, prefix) do
            String.trim_leading(path, prefix)
          else
            path
          end

        segments =
          stripped
          |> String.split("/", trim: true)
          |> Enum.map(&URI.decode/1)

        {:ok, segments}

      [] ->
        {:error, :bad_request}
    end
  end

  @doc """
  Parse the `Overwrite` header. Defaults to true per RFC 4918.
  """
  @spec parse_overwrite(Plug.Conn.t()) :: boolean()
  def parse_overwrite(conn) do
    case Plug.Conn.get_req_header(conn, "overwrite") do
      ["F"] -> false
      _ -> true
    end
  end

  @doc """
  Parse the `Timeout` header for lock requests.
  Returns timeout in seconds, defaulting to the configured lock timeout.
  """
  @spec parse_timeout(Plug.Conn.t(), pos_integer()) :: pos_integer()
  def parse_timeout(conn, default) do
    case Plug.Conn.get_req_header(conn, "timeout") do
      [value] -> parse_timeout_value(value, default)
      _ -> default
    end
  end

  defp parse_timeout_value("Infinite", _default), do: 3600
  defp parse_timeout_value("infinite", _default), do: 3600

  defp parse_timeout_value("Second-" <> seconds, default) do
    case Integer.parse(seconds) do
      {n, ""} when n > 0 -> n
      _ -> default
    end
  end

  defp parse_timeout_value(_, default), do: default

  @doc """
  Extract lock tokens from the `If` header.
  Returns a list of extracted lock tokens.
  """
  @spec extract_lock_tokens(Plug.Conn.t()) :: [String.t()]
  def extract_lock_tokens(conn) do
    conn
    |> Plug.Conn.get_req_header("if")
    |> Enum.flat_map(&parse_if_header/1)
  end

  defp parse_if_header(value) do
    # Extract opaquelocktoken URIs from If header
    # Handles both <opaquelocktoken:xxx> and bare opaquelocktoken:xxx (Windows quirk)
    ~r/<?opaquelocktoken:([^>)\s]+)>?/
    |> Regex.scan(value)
    |> Enum.map(fn [_, token] -> token end)
  end

  @doc """
  Send an error response based on a `WebdavServer.Error`.
  """
  @spec send_error(Plug.Conn.t(), Error.t()) :: Plug.Conn.t()
  def send_error(conn, %Error{code: code, message: message}) do
    status = Error.status_code(code)
    body = message || to_string(code)
    Plug.Conn.send_resp(conn, status, body)
  end

  @doc """
  Check lock tokens on a write operation.
  Returns `:ok` if the path is not locked or if a valid token is provided.
  """
  @spec check_lock(Plug.Conn.t(), [String.t()], module()) :: :ok | {:error, :locked}
  def check_lock(conn, path, lock_store) do
    locks = lock_store.get_locks(path)

    if locks == [] do
      :ok
    else
      tokens = extract_lock_tokens(conn)

      has_valid_token =
        Enum.any?(tokens, fn token ->
          lock_store.check_token(path, token) == :ok
        end)

      if has_valid_token, do: :ok, else: {:error, :locked}
    end
  end
end
