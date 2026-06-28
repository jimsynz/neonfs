defmodule NeonFS.Core.CordonStopCheck do
  @moduledoc """
  Read-only pre-shutdown safety analysis for stopping a cordoned core
  node (#1417). Reports whether taking `node` offline would compromise
  durability or quorum, so an operator can gate `systemctl stop` on it
  (the `neonfs cluster cordon-stop-check` CLI command).

  Three refusal reasons, any of which makes the stop unsafe:

    * `:quorum` — the surviving Ra members can't form a majority of the
      configured membership once `node` is offline.
    * `:stranded` — a chunk has a replica on `node` and **no** trusted
      replica anywhere else, so stopping `node` makes it unreadable.
    * `:below_min_copies` — a chunk would drop below its volume's
      `min_copies` (counting only `:trusted` replicas, per #1375) once
      `node`'s replicas are gone.

  Only `:trusted` replicas count toward durability — an `:unverified`
  copy elsewhere does not rescue a chunk from being stranded.

  The data sources are injectable via `opts` so each reason can be unit
  tested in isolation; the defaults read the live cluster.
  """

  alias NeonFS.Core.{ChunkIndex, DriveTrust, RaSupervisor, VolumeRegistry}

  @type reason ::
          %{kind: :quorum, surviving: non_neg_integer(), majority: pos_integer()}
          | %{kind: :stranded, chunks: non_neg_integer(), sample: [binary()]}
          | %{
              kind: :below_min_copies,
              chunks: non_neg_integer(),
              sample: [binary()]
            }

  @type result :: %{node: node(), safe: boolean(), reasons: [reason()]}

  @sample_limit 5

  @doc """
  Analyses whether stopping `node` is safe. Returns `%{node, safe,
  reasons}`; `safe` is `true` exactly when `reasons` is empty.
  """
  @spec check(node(), keyword()) :: result()
  def check(node, opts \\ []) when is_atom(node) do
    reasons = quorum_reasons(node, opts) ++ durability_reasons(node, opts)
    %{node: node, safe: reasons == [], reasons: reasons}
  end

  defp quorum_reasons(node, opts) do
    members = Keyword.get_lazy(opts, :members, &default_members/0)
    reachable? = Keyword.get(opts, :reachable?, &default_reachable?/1)

    total = length(members)
    majority = div(total, 2) + 1

    surviving =
      members
      |> Enum.reject(&(&1 == node))
      |> Enum.count(reachable?)

    if total > 0 and surviving < majority do
      [%{kind: :quorum, surviving: surviving, majority: majority}]
    else
      []
    end
  end

  defp durability_reasons(node, opts) do
    chunk_lister = Keyword.get(opts, :chunk_lister, &ChunkIndex.list_by_node/1)
    unverified = MapSet.new(Keyword.get_lazy(opts, :unverified, &DriveTrust.unverified/0))
    volume_getter = Keyword.get(opts, :volume_getter, &VolumeRegistry.get/1)

    {stranded, below} =
      node
      |> chunk_lister.()
      |> Enum.reduce({[], []}, fn chunk, {stranded, below} ->
        trusted_elsewhere = trusted_replicas_elsewhere(chunk, node, unverified)

        cond do
          trusted_elsewhere == 0 ->
            {[chunk.hash | stranded], below}

          trusted_elsewhere < min_copies(chunk, volume_getter) ->
            {stranded, [chunk.hash | below]}

          true ->
            {stranded, below}
        end
      end)

    aggregate(:stranded, stranded) ++ aggregate(:below_min_copies, below)
  end

  defp trusted_replicas_elsewhere(chunk, node, unverified) do
    Enum.count(chunk.locations, fn loc ->
      loc.node != node and not MapSet.member?(unverified, {loc.node, loc.drive_id})
    end)
  end

  # The chunk may be pinned by several volumes; require the most
  # stringent `min_copies` among them. Unresolvable volumes fall through
  # to 1 so an undeterminable threshold never raises a false
  # below-min-copies alarm — `:stranded` still guards the data-loss case.
  defp min_copies(chunk, volume_getter) do
    chunk.volume_ids
    |> Enum.map(fn vid ->
      case volume_getter.(vid) do
        {:ok, %{durability: durability}} -> min_copies_for(durability)
        _ -> 1
      end
    end)
    |> Enum.max(fn -> 1 end)
  end

  defp min_copies_for(%{type: :replicate, min_copies: m}), do: m
  defp min_copies_for(%{type: :erasure, data_chunks: d}), do: d
  defp min_copies_for(_), do: 1

  defp aggregate(_kind, []), do: []

  defp aggregate(kind, hashes) do
    [%{kind: kind, chunks: length(hashes), sample: Enum.take(hashes, @sample_limit)}]
  end

  defp default_members do
    case :ra.members(RaSupervisor.server_id(), 2_000) do
      {:ok, members, _leader} -> Enum.map(members, fn {_name, node} -> node end)
      _ -> []
    end
  end

  defp default_reachable?(node), do: node == Node.self() or node in Node.list()
end
