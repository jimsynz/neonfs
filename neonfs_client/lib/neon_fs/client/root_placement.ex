defmodule NeonFS.Client.RootPlacement do
  @moduledoc """
  Caches, per volume, the core nodes that hold the volume's root metadata
  segment so the `Router` can dispatch metadata writes to a node that can
  perform them locally (#1046).

  Without this, metadata writes are routed by `CostFunction` with no root
  awareness; a write that lands on a non-root-holding node pays an extra
  remote re-dispatch hop on the core side (the `MetadataWriter` fallback,
  #1045) on *every* operation. Resolving the root nodes once and caching
  them turns that fallback back into the rare case.

  The cache is authoritative-source-backed, not a copy: it stores only the
  *node list* derived from `root_entry.drive_locations`
  (`NeonFS.Core.volume_root_nodes/1`), with a short TTL. A stale entry is
  self-correcting — it routes to a node that may no longer hold the root,
  and the core-side fallback handles that until the entry expires.

  Resolution runs in the *calling* process (a fast ETS read on a hit, an
  RPC only on a miss); the GenServer merely owns the public ETS table, so a
  slow resolution never blocks other lookups.
  """

  use GenServer

  alias NeonFS.Client.Router

  @table :neonfs_root_placement
  @default_ttl_ms 30_000

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns the cached or freshly-resolved root-holding nodes for `volume_name`.

  On a cache miss the volume's root nodes are resolved via
  `NeonFS.Core.volume_root_nodes/1` (routed through `Router`) and cached for
  the TTL. `:resolver` (a `(String.t() -> {:ok, [node()]} | {:error, term()})`
  fun) overrides the resolver — used by tests.
  """
  @spec get(String.t(), keyword()) :: {:ok, [node()]} | {:error, term()}
  def get(volume_name, opts \\ []) when is_binary(volume_name) do
    case lookup_fresh(volume_name) do
      {:ok, _nodes} = hit -> hit
      :miss -> resolve_and_cache(volume_name, opts)
    end
  end

  @doc """
  Drops any cached entry for `volume_name`, forcing the next `get/2` to
  re-resolve.
  """
  @spec invalidate(String.t()) :: :ok
  def invalidate(volume_name) when is_binary(volume_name) do
    :ets.delete(@table, volume_name)
    :ok
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, %{}}
  end

  defp lookup_fresh(volume_name) do
    now = System.monotonic_time(:millisecond)

    case safe_lookup(volume_name) do
      [{^volume_name, nodes, expires_at}] when expires_at > now -> {:ok, nodes}
      _ -> :miss
    end
  end

  # The table only exists once the GenServer has started; tolerate the
  # window before init/1 (or a node where the client isn't running) by
  # treating a missing table as a miss rather than crashing the caller.
  defp safe_lookup(volume_name) do
    :ets.lookup(@table, volume_name)
  rescue
    ArgumentError -> []
  end

  defp resolve_and_cache(volume_name, opts) do
    resolver = Keyword.get(opts, :resolver, &default_resolver/1)

    case resolver.(volume_name) do
      {:ok, nodes} when is_list(nodes) ->
        cache(volume_name, nodes, Keyword.get(opts, :ttl_ms, @default_ttl_ms))
        {:ok, nodes}

      {:error, _} = error ->
        error

      other ->
        {:error, {:unexpected_root_nodes_reply, other}}
    end
  end

  defp default_resolver(volume_name) do
    Router.call(NeonFS.Core, :volume_root_nodes, [volume_name])
  end

  defp cache(volume_name, nodes, ttl_ms) do
    expires_at = System.monotonic_time(:millisecond) + ttl_ms
    :ets.insert(@table, {volume_name, nodes, expires_at})
  rescue
    ArgumentError -> :ok
  end
end
