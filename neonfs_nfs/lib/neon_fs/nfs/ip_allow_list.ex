defmodule NeonFS.NFS.IpAllowList do
  @moduledoc """
  Per-export client IP allow-list matching for NFS (#1217).

  An export carries `allowed_ips` — a list of IP or CIDR strings
  (`"10.0.0.0/8"`, `"192.168.1.5"`, `"2001:db8::/32"`). A client's source
  address (an `:inet.ip_address()` tuple, surfaced as `ctx.peer`) is
  permitted when the list is empty (allow-all, the historical posture) or
  matches at least one entry. A `nil` peer with a non-empty list is
  denied — without a verifiable source address the allow-list can't be
  satisfied.

  Matching is integer-and-mask over the address family; IPv4 and IPv6
  entries only match peers of the same family.
  """

  import Bitwise

  @doc """
  Whether `peer` is permitted by `allow_list`. Empty list ⇒ always true.
  """
  @spec allowed?(:inet.ip_address() | nil, [String.t()]) :: boolean()
  def allowed?(_peer, []), do: true
  def allowed?(nil, _allow_list), do: false

  def allowed?(peer, allow_list) when is_tuple(peer) and is_list(allow_list) do
    Enum.any?(allow_list, &peer_matches?(peer, &1))
  end

  def allowed?(_peer, _allow_list), do: false

  defp peer_matches?(peer, entry) do
    case parse_entry(entry) do
      {:ok, net, prefix} -> in_network?(peer, net, prefix)
      :error -> false
    end
  end

  defp parse_entry(entry) when is_binary(entry) do
    case String.split(entry, "/") do
      [ip] ->
        case :inet.parse_address(String.to_charlist(ip)) do
          {:ok, addr} -> {:ok, addr, total_bits(addr)}
          _ -> :error
        end

      [ip, prefix] ->
        with {:ok, addr} <- :inet.parse_address(String.to_charlist(ip)),
             {n, ""} when n >= 0 <- Integer.parse(prefix),
             true <- n <= total_bits(addr) do
          {:ok, addr, n}
        else
          _ -> :error
        end

      _ ->
        :error
    end
  end

  # Same address family only (IPv4 tuple is size 4, IPv6 size 8).
  defp in_network?(peer, net, prefix) when tuple_size(peer) == tuple_size(net) do
    total = total_bits(peer)
    mask = network_mask(total, prefix)
    (to_int(peer) &&& mask) == (to_int(net) &&& mask)
  end

  defp in_network?(_peer, _net, _prefix), do: false

  defp network_mask(_total, 0), do: 0
  defp network_mask(total, prefix), do: (1 <<< total) - 1 - ((1 <<< (total - prefix)) - 1)

  defp total_bits(tuple) when tuple_size(tuple) == 4, do: 32
  defp total_bits(tuple) when tuple_size(tuple) == 8, do: 128

  defp to_int(tuple) do
    shift = if tuple_size(tuple) == 4, do: 8, else: 16
    Enum.reduce(Tuple.to_list(tuple), 0, fn part, acc -> acc <<< shift ||| part end)
  end
end
