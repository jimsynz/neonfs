defmodule NeonFS.Core.MetadataRing do
  @moduledoc """
  Consistent hashing ring for distributing metadata across cluster nodes.

  Maps arbitrary keys (chunk hashes, file IDs, directory paths) to segments,
  and segments to replica sets. Uses SHA-256 for ring positions and virtual
  nodes (default 64 per physical node) for even distribution.

  This is a pure data structure — no GenServer, no side effects. The
  QuorumCoordinator (task 0084) uses this ring to determine routing.

  ## Segments

  Each virtual node on the ring defines a segment — the arc of key space from
  the previous virtual node's position (exclusive) to this virtual node's
  position (inclusive). The segment ID is the SHA-256 hash that determines the
  virtual node's ring position. The total number of segments equals
  `node_count * virtual_nodes_per_physical`.

  ## Replica Sets

  For a given key, `locate/2` walks clockwise from the key's ring position
  and collects the first N distinct physical nodes. This ensures replicas are
  spread across different machines.
  """

  @default_virtual_nodes_per_physical 64
  @default_replicas 3

  @type segment_id :: binary()
  @type replica_set :: [node()]

  @type t :: %__MODULE__{
          sorted_ring: [{segment_id(), node()}],
          node_set: MapSet.t(node()),
          virtual_nodes_per_physical: pos_integer(),
          replicas: pos_integer()
        }

  @enforce_keys [:sorted_ring, :node_set, :virtual_nodes_per_physical, :replicas]
  defstruct [:sorted_ring, :node_set, :virtual_nodes_per_physical, :replicas]

  @doc """
  Builds a ring from a node list and options.

  ## Options

    * `:virtual_nodes_per_physical` — number of virtual nodes per physical node
      (default: #{@default_virtual_nodes_per_physical})
    * `:replicas` — replica set size, capped at cluster size when locating keys
      (default: #{@default_replicas})
  """
  @spec new([node()], keyword()) :: t()
  def new(node_list, opts \\ []) when is_list(node_list) do
    vnodes_per =
      Keyword.get(opts, :virtual_nodes_per_physical, @default_virtual_nodes_per_physical)

    replicas = Keyword.get(opts, :replicas, @default_replicas)
    node_set = MapSet.new(node_list)

    sorted_ring =
      node_list
      |> Enum.flat_map(fn node ->
        for i <- 0..(vnodes_per - 1)//1, do: {hash_vnode(node, i), node}
      end)
      |> Enum.sort_by(&elem(&1, 0))

    %__MODULE__{
      sorted_ring: sorted_ring,
      node_set: node_set,
      virtual_nodes_per_physical: vnodes_per,
      replicas: replicas
    }
  end

  @doc """
  Locates the segment and replica set for a key.

  Hashes the key with SHA-256, walks clockwise from the resulting ring
  position, and collects up to `replicas` distinct physical nodes.

  Returns `{segment_id, replica_set}` where `segment_id` is the ring position
  of the first virtual node found clockwise, and `replica_set` is an ordered
  list of distinct physical nodes.
  """
  @spec locate(t(), binary()) :: {segment_id(), replica_set()}
  def locate(%__MODULE__{sorted_ring: []}, _key), do: {<<>>, []}

  def locate(%__MODULE__{} = ring, key) when is_binary(key) do
    position = :crypto.hash(:sha256, key)
    locate_by_position(ring, position)
  end

  @doc """
  Adds a node to the ring.

  Returns `{new_ring, affected_segments}` where `affected_segments` is the
  list of segment IDs in the new ring whose replica sets differ from what the
  old ring assigned for the same positions. Approximately 1/N of total
  segments are affected when the effective replica count does not change.
  """
  @spec add_node(t(), node()) :: {t(), [segment_id()]}
  def add_node(%__MODULE__{} = ring, node) do
    if MapSet.member?(ring.node_set, node) do
      {ring, []}
    else
      new_ring =
        new([node | MapSet.to_list(ring.node_set)],
          virtual_nodes_per_physical: ring.virtual_nodes_per_physical,
          replicas: ring.replicas
        )

      affected = compute_affected_segments(ring, new_ring)
      {new_ring, affected}
    end
  end

  @doc """
  Removes a node from the ring.

  Returns `{new_ring, affected_segments}` where `affected_segments` is the
  list of segment IDs in the new ring whose replica sets differ from what the
  old ring assigned for the same positions.
  """
  @spec remove_node(t(), node()) :: {t(), [segment_id()]}
  def remove_node(%__MODULE__{} = ring, node) do
    if MapSet.member?(ring.node_set, node) do
      new_ring =
        new(MapSet.to_list(MapSet.delete(ring.node_set, node)),
          virtual_nodes_per_physical: ring.virtual_nodes_per_physical,
          replicas: ring.replicas
        )

      affected = compute_affected_segments(ring, new_ring)
      {new_ring, affected}
    else
      {ring, []}
    end
  end

  @doc """
  Returns all segment IDs and their replica sets.
  """
  @spec segments(t()) :: [{segment_id(), replica_set()}]
  def segments(%__MODULE__{sorted_ring: []}), do: []

  def segments(%__MODULE__{} = ring) do
    effective = effective_replicas(ring)
    ring_tuple = List.to_tuple(ring.sorted_ring)
    ring_size = tuple_size(ring_tuple)

    ring.sorted_ring
    |> Enum.with_index()
    |> Enum.map(fn {{position, _node}, idx} ->
      {_seg_id, replicas} = collect_replicas(ring_tuple, idx, ring_size, effective)
      {position, replicas}
    end)
  end

  @doc """
  Returns all physical nodes in the ring, sorted for deterministic output.
  """
  @spec nodes(t()) :: [node()]
  def nodes(%__MODULE__{node_set: node_set}) do
    node_set |> MapSet.to_list() |> Enum.sort()
  end

  @doc """
  Returns the total number of segments (one per virtual node).
  """
  @spec segment_count(t()) :: non_neg_integer()
  def segment_count(%__MODULE__{sorted_ring: sorted_ring}) do
    length(sorted_ring)
  end

  # --- Private Functions ---

  defp effective_replicas(%__MODULE__{replicas: replicas, node_set: node_set}) do
    min(replicas, MapSet.size(node_set))
  end

  defp hash_vnode(node, index) do
    :crypto.hash(:sha256, "#{node}:#{index}")
  end

  defp locate_by_position(%__MODULE__{sorted_ring: sorted_ring} = ring, position) do
    effective = effective_replicas(ring)

    if effective == 0 do
      {<<>>, []}
    else
      ring_tuple = List.to_tuple(sorted_ring)
      ring_size = tuple_size(ring_tuple)
      start_idx = find_start_index(sorted_ring, position)
      collect_replicas(ring_tuple, start_idx, ring_size, effective)
    end
  end

  defp find_start_index(sorted_ring, position) do
    Enum.find_index(sorted_ring, fn {pos, _} -> pos >= position end) || 0
  end

  defp collect_replicas(ring_tuple, start_idx, ring_size, target_replicas) do
    do_collect(ring_tuple, start_idx, ring_size, target_replicas, [], nil, 0)
  end

  defp do_collect(_ring_tuple, _idx, _ring_size, 0, acc, segment_id, _visited) do
    {segment_id, Enum.reverse(acc)}
  end

  defp do_collect(_ring_tuple, _idx, ring_size, _remaining, acc, segment_id, visited)
       when visited >= ring_size do
    {segment_id, Enum.reverse(acc)}
  end

  defp do_collect(ring_tuple, idx, ring_size, remaining, acc, segment_id, visited) do
    actual_idx = rem(idx, ring_size)
    {pos, node} = elem(ring_tuple, actual_idx)
    segment_id = segment_id || pos

    if node in acc do
      do_collect(ring_tuple, idx + 1, ring_size, remaining, acc, segment_id, visited + 1)
    else
      do_collect(
        ring_tuple,
        idx + 1,
        ring_size,
        remaining - 1,
        [node | acc],
        segment_id,
        visited + 1
      )
    end
  end

  defp compute_affected_segments(old_ring, new_ring) do
    new_ring.sorted_ring
    |> Enum.filter(fn {position, _node} ->
      {_new_seg, new_replicas} = locate_by_position(new_ring, position)
      {_old_seg, old_replicas} = locate_by_position(old_ring, position)
      new_replicas != old_replicas
    end)
    |> Enum.map(&elem(&1, 0))
  end
end
