defmodule NeonFS.Integration.WriteOperationCreateOnlyTest do
  @moduledoc """
  Peer-cluster integration test for
  `NeonFS.Core.WriteOperation.write_file_streamed/4` with
  `create_only: true` (sub-issue #592 of #303).

  Two concurrent streamed writes for the same new path, originating
  from different nodes, race through the namespace coordinator.
  Exactly one wins (`{:ok, _file_meta}`); the other gets
  `{:error, :exists}`. The successful write's bytes are observable
  cluster-wide via `NeonFS.Core.read_file/2`.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.WriteOperation
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000
  @moduletag :integration
  @moduletag nodes: 2
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "create-only-test")
    :ok
  end

  describe "create_only across nodes" do
    test "exactly one of two concurrent streamed writes succeeds", %{cluster: cluster} do
      volume_name = "create-only-vol-#{System.unique_integer([:positive])}"

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [volume_name, %{}])

      assert_eventually timeout: 10_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
               volume_name
             ]) do
          {:ok, _} -> true
          _ -> false
        end
      end

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [volume_name])

      path = "/atomic-create-#{System.unique_integer([:positive])}.bin"

      # Spawn the two writes in parallel from this test process so the
      # Ra leader sees both `:claim_namespace_create` commands close
      # together. Real clients may interleave them in any order; the
      # primitive's job is to ensure exactly one wins regardless.
      parent = self()

      spawn_link(fn ->
        result =
          PeerCluster.rpc(cluster, :node1, WriteOperation, :write_file_streamed, [
            volume.id,
            path,
            ["from-node1"],
            [create_only: true]
          ])

        send(parent, {:result, :node1, result})
      end)

      spawn_link(fn ->
        result =
          PeerCluster.rpc(cluster, :node2, WriteOperation, :write_file_streamed, [
            volume.id,
            path,
            ["from-node2"],
            [create_only: true]
          ])

        send(parent, {:result, :node2, result})
      end)

      r1 = receive_result(:node1)
      r2 = receive_result(:node2)

      {winner_node, winner_bytes} =
        case {r1, r2} do
          {{:ok, _meta}, {:error, :exists}} ->
            {:node1, "from-node1"}

          {{:error, :exists}, {:ok, _meta}} ->
            {:node2, "from-node2"}

          other ->
            flunk("expected exactly one :ok and one :exists, got #{inspect(other)}")
        end

      # The winner's bytes are visible cluster-wide.
      for read_node <- [:node1, :node2] do
        assert {:ok, contents} =
                 PeerCluster.rpc(cluster, read_node, NeonFS.Core, :read_file, [
                   volume_name,
                   path
                 ])

        assert contents == winner_bytes,
               "node=#{inspect(read_node)} read mismatched bytes for winner=#{inspect(winner_node)}: " <>
                 inspect(contents)
      end

      # A subsequent create_only write must also fail — the file is now
      # committed to FileIndex, even after the in-flight claim was
      # released.
      assert {:error, :exists} =
               PeerCluster.rpc(cluster, :node2, WriteOperation, :write_file_streamed, [
                 volume.id,
                 path,
                 ["after-the-fact"],
                 [create_only: true]
               ])
    end
  end

  defp receive_result(node_name) do
    receive do
      {:result, ^node_name, r} -> r
    after
      30_000 -> flunk("#{inspect(node_name)} write_file_streamed did not return within 30s")
    end
  end
end
