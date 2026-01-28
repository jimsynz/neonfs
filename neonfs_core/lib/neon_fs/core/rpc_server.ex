defmodule NeonFS.Core.RpcServer do
  @moduledoc """
  TCP RPC server for CLI communication.

  Listens on a TCP port and accepts ETF-encoded RPC requests from the CLI.
  Request format: `{:rpc, cookie, module, function, args}`
  Response format: `{:ok, result}` or `{:error, reason}`
  """
  use GenServer
  require Logger

  alias NeonFS.CLI.Handler

  @default_port 4370
  @cookie_path_env "RELEASE_COOKIE"

  # Client API

  @doc """
  Starts the RPC server.

  ## Options

    * `:port` - TCP port to listen on (default: 4370)
    * `:port_file` - Path to write port number to (default: /run/neonfs/rpc_port)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Server callbacks

  @impl true
  def init(opts) do
    port = Keyword.get(opts, :port, @default_port)
    port_file = Keyword.get(opts, :port_file, "/run/neonfs/rpc_port")

    case :gen_tcp.listen(port, [:binary, packet: 4, active: false, reuseaddr: true]) do
      {:ok, listen_socket} ->
        # Write port file for CLI discovery
        write_port_file(port_file, port)

        # Start acceptor process
        acceptor_pid = spawn_link(fn -> accept_loop(listen_socket) end)

        Logger.info("RPC server listening on port #{port}")

        {:ok, %{socket: listen_socket, port: port, acceptor: acceptor_pid, port_file: port_file}}

      {:error, reason} ->
        Logger.error("Failed to start RPC server on port #{port}: #{inspect(reason)}")
        {:stop, {:tcp_listen_failed, reason}}
    end
  end

  @impl true
  def terminate(_reason, state) do
    # Close listening socket
    :gen_tcp.close(state.socket)

    # Remove port file
    if File.exists?(state.port_file) do
      File.rm(state.port_file)
    end

    :ok
  end

  # Private functions

  defp write_port_file(path, port) do
    dir = Path.dirname(path)

    unless File.dir?(dir) do
      File.mkdir_p!(dir)
    end

    File.write!(path, to_string(port))
  rescue
    e ->
      Logger.warning("Failed to write port file #{path}: #{Exception.message(e)}")
  end

  defp accept_loop(listen_socket) do
    case :gen_tcp.accept(listen_socket) do
      {:ok, client_socket} ->
        # Spawn handler for this client
        spawn(fn -> handle_client(client_socket) end)
        accept_loop(listen_socket)

      {:error, :closed} ->
        # Server shutting down
        :ok

      {:error, reason} ->
        Logger.error("Accept failed: #{inspect(reason)}")
        # Brief pause before retrying
        Process.sleep(100)
        accept_loop(listen_socket)
    end
  end

  defp handle_client(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        response = process_request(data)
        :gen_tcp.send(socket, response)
        handle_client(socket)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Logger.debug("Client connection error: #{inspect(reason)}")
        :ok
    end
  end

  defp process_request(data) do
    case :erlang.binary_to_term(data, [:safe]) do
      {:rpc, cookie, module_name, function_name, args}
      when is_binary(cookie) and is_binary(module_name) and is_binary(function_name) and
             is_list(args) ->
        if valid_cookie?(cookie) do
          result = apply_rpc(module_name, function_name, args)
          :erlang.term_to_binary(result)
        else
          Logger.warning("Invalid cookie in RPC request")
          :erlang.term_to_binary({:error, "invalid_cookie"})
        end

      _ ->
        Logger.warning("Invalid RPC request format")
        :erlang.term_to_binary({:error, "invalid_request"})
    end
  rescue
    e ->
      Logger.error("Error processing RPC request: #{Exception.message(e)}")
      :erlang.term_to_binary({:error, "internal_error"})
  end

  defp valid_cookie?(cookie) do
    expected = get_expected_cookie()
    expected != nil and cookie == expected
  end

  defp get_expected_cookie do
    # Try environment variable first
    case System.get_env(@cookie_path_env) do
      nil ->
        # Fall back to reading .erlang.cookie
        cookie_path = Path.join(System.get_env("HOME", "/var/lib/neonfs"), ".erlang.cookie")

        case File.read(cookie_path) do
          {:ok, cookie} -> String.trim(cookie)
          {:error, _} -> nil
        end

      cookie ->
        cookie
    end
  end

  defp apply_rpc("Elixir.NeonFS.CLI.Handler", "cluster_status", []) do
    Handler.cluster_status()
  end

  defp apply_rpc("Elixir.NeonFS.CLI.Handler", "list_volumes", []) do
    Handler.list_volumes()
  end

  defp apply_rpc("Elixir.NeonFS.CLI.Handler", "create_volume", [name, config_map])
       when is_map(config_map) do
    # Convert to format expected by handler: top-level string keys, nested atom keys
    config = prepare_volume_config(config_map)
    Handler.create_volume(name, config)
  end

  defp apply_rpc("Elixir.NeonFS.CLI.Handler", "delete_volume", [name]) do
    Handler.delete_volume(name)
  end

  defp apply_rpc("Elixir.NeonFS.CLI.Handler", "get_volume", [name]) do
    Handler.get_volume(name)
  end

  defp apply_rpc("Elixir.NeonFS.CLI.Handler", "mount", [volume, mountpoint, options_map])
       when is_map(options_map) do
    # Convert top-level keys to strings for handler
    options = atomize_map_keys(options_map)
    Handler.mount(volume, mountpoint, options)
  end

  defp apply_rpc("Elixir.NeonFS.CLI.Handler", "unmount", [mountpoint]) do
    Handler.unmount(mountpoint)
  end

  defp apply_rpc("Elixir.NeonFS.CLI.Handler", "list_mounts", []) do
    Handler.list_mounts()
  end

  defp apply_rpc(_module, _function, _args) do
    {:error, "unknown_function"}
  end

  # Prepare volume config: top-level string keys (for handler's map_to_opts), nested atom keys (for validation)
  defp prepare_volume_config(map) when is_map(map) do
    map
    |> Enum.map(fn {k, v} -> {to_string(k), atomize_map_keys(v)} end)
    |> Map.new()
  end

  # Recursively convert map keys to atoms while preserving non-map values
  defp atomize_map_keys(map) when is_map(map) do
    map
    |> Enum.map(fn {k, v} -> {key_to_atom(k), atomize_map_keys(v)} end)
    |> Map.new()
  end

  defp atomize_map_keys(value), do: value

  # Convert keys to atoms
  defp key_to_atom(key) when is_atom(key), do: key

  defp key_to_atom(key) when is_binary(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> String.to_atom(key)
  end
end
