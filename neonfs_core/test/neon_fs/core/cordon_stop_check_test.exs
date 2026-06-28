defmodule NeonFS.Core.CordonStopCheckTest do
  @moduledoc """
  Unit tests for the cordon-stop safety analysis (#1417). Data sources
  are injected via `opts` so each refusal reason is exercised in
  isolation without a running cluster.
  """
  use ExUnit.Case, async: true

  alias NeonFS.Core.CordonStopCheck

  @target :n1@host

  defp loc(node, drive_id \\ "d0"), do: %{node: node, drive_id: drive_id, tier: :hot}

  defp chunk(hash, locations, volume_ids \\ ["vol-1"]) do
    %{hash: hash, locations: locations, volume_ids: MapSet.new(volume_ids)}
  end

  # No chunks on the node and quorum intact unless overridden.
  defp opts(extra) do
    Keyword.merge(
      [
        members: [@target, :n2@host, :n3@host],
        reachable?: fn _ -> true end,
        chunk_lister: fn _node -> [] end,
        unverified: [],
        volume_getter: fn _ -> {:ok, %{durability: %{type: :replicate, min_copies: 2}}} end
      ],
      extra
    )
  end

  describe "quorum" do
    test "safe when a majority survives the node going offline" do
      result = CordonStopCheck.check(@target, opts([]))
      assert result.safe
      assert result.reasons == []
    end

    test "refuses when stopping the node leaves no majority (2-node cluster)" do
      result = CordonStopCheck.check(@target, opts(members: [@target, :n2@host]))

      assert result.safe == false
      assert [%{kind: :quorum, surviving: 1, majority: 2}] = result.reasons
    end

    test "refuses when another member is already unreachable" do
      # 3 members, but n3 is already down — stopping the target leaves
      # only n2 reachable (1 < majority 2).
      result =
        CordonStopCheck.check(
          @target,
          opts(reachable?: fn node -> node == :n2@host end)
        )

      assert result.safe == false
      assert [%{kind: :quorum, surviving: 1, majority: 2}] = result.reasons
    end
  end

  describe "durability" do
    test "stranded: the node holds the only replica" do
      chunks = [chunk("h-stranded", [loc(@target)])]

      result =
        CordonStopCheck.check(
          @target,
          opts(chunk_lister: fn _ -> chunks end)
        )

      assert result.safe == false
      assert [%{kind: :stranded, chunks: 1, sample: ["h-stranded"]}] = result.reasons
    end

    test "an :unverified replica elsewhere does not rescue a chunk from stranding" do
      chunks = [chunk("h1", [loc(@target, "d0"), loc(:n2@host, "warm0")])]

      result =
        CordonStopCheck.check(
          @target,
          opts(
            chunk_lister: fn _ -> chunks end,
            unverified: [{:n2@host, "warm0"}]
          )
        )

      assert [%{kind: :stranded, chunks: 1}] = result.reasons
    end

    test "below_min_copies: a trusted replica survives but fewer than min_copies" do
      # One trusted replica elsewhere (n2), min_copies 2 → 1 < 2.
      chunks = [chunk("h-under", [loc(@target), loc(:n2@host)])]

      result =
        CordonStopCheck.check(
          @target,
          opts(chunk_lister: fn _ -> chunks end)
        )

      assert result.safe == false
      assert [%{kind: :below_min_copies, chunks: 1}] = result.reasons
    end

    test "safe when enough trusted replicas survive elsewhere" do
      chunks = [chunk("h-ok", [loc(@target), loc(:n2@host), loc(:n3@host)])]

      result =
        CordonStopCheck.check(
          @target,
          opts(chunk_lister: fn _ -> chunks end)
        )

      assert result.safe
      assert result.reasons == []
    end

    test "counts a chunk with several replicas on the node once" do
      chunks = [chunk("h-dup", [loc(@target, "d0"), loc(@target, "d1")])]

      result =
        CordonStopCheck.check(
          @target,
          opts(chunk_lister: fn _ -> chunks end)
        )

      assert [%{kind: :stranded, chunks: 1}] = result.reasons
    end

    test "uses the most stringent min_copies across a chunk's volumes" do
      chunks = [
        chunk("h-multi", [loc(@target), loc(:n2@host), loc(:n3@host)], ["vol-a", "vol-b"])
      ]

      getter = fn
        "vol-a" -> {:ok, %{durability: %{type: :replicate, min_copies: 2}}}
        "vol-b" -> {:ok, %{durability: %{type: :replicate, min_copies: 3}}}
      end

      result =
        CordonStopCheck.check(
          @target,
          opts(chunk_lister: fn _ -> chunks end, volume_getter: getter)
        )

      # Two trusted replicas survive (n2, n3); the stricter min_copies 3
      # makes that insufficient.
      assert [%{kind: :below_min_copies, chunks: 1}] = result.reasons
    end
  end
end
