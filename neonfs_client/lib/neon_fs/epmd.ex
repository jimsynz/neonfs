defmodule NeonFS.Epmd do
  @moduledoc """
  Custom EPMD module that replaces the standard EPMD daemon.

  Activated via `-start_epmd false -epmd_module Elixir.NeonFS.Epmd` in VM args.
  All callbacks are invoked before OTP applications start, so this module
  must not rely on Application config, GenServers, or ETS tables.

  Port resolution sources (in priority order):
  1. `NEONFS_PEER_PORTS` env var — `"name@host:port,name@host:port"` format,
     set dynamically during join/formation before `Node.connect/1`
  2. `cluster.json` — persisted cluster state with peer dist_port fields
  3. `NEONFS_DIST_PORT` env var — for the local node's own port

  The local node's listen port is determined by `NEONFS_DIST_PORT` env var,
  which is set by the daemon wrapper script on startup.
  """

  @dist_version 5

  @doc false
  def start_link do
    :ignore
  end

  @doc false
  def listen_port_please(_name, _host) do
    case read_dist_port_env() do
      {:ok, port} -> {:ok, port}
      :error -> {:ok, 0}
    end
  end

  @doc false
  def register_node(_name, _port, _driver) do
    {:ok, 1}
  end

  @doc false
  def address_please(name, host, address_family) do
    name_str = stringify(name)
    host_str = stringify(host)
    full_name = name_str <> "@" <> host_str

    case resolve_peer_port(full_name) do
      {:ok, port} ->
        case :inet.getaddr(String.to_charlist(host_str), address_family) do
          {:ok, ip} -> {:ok, ip, port, @dist_version}
          {:error, _} -> {:error, :nxdomain}
        end

      :error ->
        {:error, :nxdomain}
    end
  end

  @doc false
  def names(_hostname) do
    local =
      case read_dist_port_env() do
        {:ok, port} ->
          case :os.getenv(~c"RELEASE_NODE") do
            false -> []
            node_name -> [{short_name(List.to_string(node_name)), port}]
          end

        :error ->
          []
      end

    entries =
      (local ++ peers_from_env() ++ peers_from_cluster_state())
      |> Enum.uniq_by(fn {name, _port} -> name end)

    {:ok, entries}
  end

  # --- Private helpers ---

  defp resolve_peer_port(full_name) do
    case lookup_peer_ports_env(full_name) do
      {:ok, port} -> {:ok, port}
      :error -> lookup_cluster_state(full_name)
    end
  end

  defp read_dist_port_env do
    case :os.getenv(~c"NEONFS_DIST_PORT") do
      false ->
        :error

      value ->
        case :erlang.list_to_integer(value) do
          port when port > 0 -> {:ok, port}
          _ -> :error
        end
    end
  rescue
    _ -> :error
  end

  defp lookup_peer_ports_env(full_name) do
    case :os.getenv(~c"NEONFS_PEER_PORTS") do
      false -> :error
      value -> find_peer_port(List.to_string(value), full_name)
    end
  end

  defp find_peer_port(env_value, full_name) do
    env_value
    |> String.split(",", trim: true)
    |> Enum.find_value(:error, fn entry ->
      case parse_peer_entry(entry) do
        {^full_name, port} -> {:ok, port}
        _ -> nil
      end
    end)
  end

  defp peers_from_env do
    case :os.getenv(~c"NEONFS_PEER_PORTS") do
      false -> []
      value -> parse_all_peer_entries(List.to_string(value))
    end
  end

  defp parse_all_peer_entries(env_value) do
    env_value
    |> String.split(",", trim: true)
    |> Enum.flat_map(fn entry ->
      case parse_peer_entry(entry) do
        {name, port} -> [{short_name(name), port}]
        _ -> []
      end
    end)
  end

  # Parses "name@host:port" into {"name@host", port}
  defp parse_peer_entry(entry) do
    entry = String.trim(entry)

    case String.split(entry, "@", parts: 2) do
      [_name_part, host_and_port] ->
        case rsplit_colon(host_and_port) do
          {_host, port} when port > 0 ->
            {rsplit_colon_name(entry), port}

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  # Split "host:port" on the last colon (handles IPv6 hostnames)
  defp rsplit_colon(string) do
    case :binary.matches(string, ":") do
      [] ->
        {string, 0}

      matches ->
        {pos, 1} = List.last(matches)
        host = binary_part(string, 0, pos)
        port_str = binary_part(string, pos + 1, byte_size(string) - pos - 1)

        case Integer.parse(port_str) do
          {port, ""} -> {host, port}
          _ -> {string, 0}
        end
    end
  end

  # Strip the trailing ":port" from "name@host:port"
  defp rsplit_colon_name(entry) do
    case :binary.matches(entry, ":") do
      [] -> entry
      matches -> binary_part(entry, 0, elem(List.last(matches), 0))
    end
  end

  defp lookup_cluster_state(full_name) do
    case read_cluster_state() do
      {:ok, data} -> find_port_in_cluster_data(data, full_name)
      :error -> :error
    end
  end

  defp peers_from_cluster_state do
    case read_cluster_state() do
      {:ok, data} ->
        this_node_entry =
          case data do
            %{"this_node" => %{"name" => name, "dist_port" => port}}
            when is_integer(port) and port > 0 ->
              [{short_name(name), port}]

            _ ->
              []
          end

        peer_entries =
          case data do
            %{"known_peers" => peers} when is_list(peers) ->
              Enum.flat_map(peers, fn
                %{"name" => name, "dist_port" => port} when is_integer(port) and port > 0 ->
                  [{short_name(name), port}]

                _ ->
                  []
              end)

            _ ->
              []
          end

        this_node_entry ++ peer_entries

      :error ->
        []
    end
  end

  defp read_cluster_state do
    path = cluster_state_path()

    case :file.read_file(String.to_charlist(path)) do
      {:ok, content} ->
        case :json.decode(content) do
          data when is_map(data) -> {:ok, data}
          _ -> :error
        end

      {:error, _} ->
        :error
    end
  rescue
    _ -> :error
  end

  defp find_port_in_cluster_data(data, full_name) do
    case data do
      %{"this_node" => %{"name" => ^full_name, "dist_port" => port}}
      when is_integer(port) and port > 0 ->
        {:ok, port}

      _ ->
        find_port_in_peers(data["known_peers"], full_name)
    end
  end

  defp find_port_in_peers(nil, _full_name), do: :error
  defp find_port_in_peers([], _full_name), do: :error

  defp find_port_in_peers([%{"name" => name, "dist_port" => port} | _rest], full_name)
       when name == full_name and is_integer(port) and port > 0 do
    {:ok, port}
  end

  defp find_port_in_peers([_ | rest], full_name), do: find_port_in_peers(rest, full_name)

  defp cluster_state_path do
    meta_dir =
      case :os.getenv(~c"NEONFS_META_DIR") do
        false -> "/var/lib/neonfs/meta"
        dir -> List.to_string(dir)
      end

    Path.join(meta_dir, "cluster.json")
  end

  defp short_name(full_name) do
    case String.split(full_name, "@", parts: 2) do
      [short | _] -> String.to_charlist(short)
      _ -> String.to_charlist(full_name)
    end
  end

  defp stringify(value) when is_list(value), do: List.to_string(value)
  defp stringify(value) when is_binary(value), do: value
  defp stringify(value) when is_atom(value), do: Atom.to_string(value)
end
