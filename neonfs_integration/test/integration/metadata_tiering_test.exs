defmodule NeonFS.Integration.MetadataTieringTest do
  @moduledoc """
  Phase 5 integration tests for metadata tiering architecture.

  Tests directory operations, cross-segment atomicity, crash recovery,
  clock skew detection, concurrent writer detection, and mixed volume
  type scenarios on a multi-node cluster.
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.Intent

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "tiering-test")
    %{}
  end

  describe "directory listing via DirectoryEntry" do
    @tag :pending_903
    test "list_dir returns all children", %{cluster: cluster} do
      :ok = init_tiering_cluster(cluster, "dir-vol")

      # Create files in root and a subdirectory
      files = [
        {"/alpha.txt", "alpha content"},
        {"/beta.txt", "beta content"},
        {"/docs/readme.md", "# README"},
        {"/docs/guide.md", "# Guide"},
        {"/docs/faq.md", "# FAQ"}
      ]

      for {path, content} <- files do
        {:ok, _} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
            "dir-vol",
            path,
            content
          ])
      end

      # List root directory
      {:ok, root_children} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :list_dir, [
          "dir-vol",
          "/"
        ])

      root_names = Map.keys(root_children)
      assert "alpha.txt" in root_names
      assert "beta.txt" in root_names
      assert "docs" in root_names

      # List subdirectory
      {:ok, docs_children} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :list_dir, [
          "dir-vol",
          "/docs"
        ])

      docs_names = Map.keys(docs_children)
      assert "readme.md" in docs_names
      assert "guide.md" in docs_names
      assert "faq.md" in docs_names
      assert length(docs_names) == 3

      # Verify directory listing works from another node too
      {:ok, docs_from_node2} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :list_dir, [
          "dir-vol",
          "/docs"
        ])

      assert Map.keys(docs_from_node2) |> Enum.sort() == Enum.sort(docs_names)
    end
  end

  describe "cross-segment file creation atomicity" do
    @tag :pending_903
    test "file creation writes both FileMeta and DirectoryEntry", %{cluster: cluster} do
      :ok = init_tiering_cluster(cluster, "atomic-vol")

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, ["atomic-vol"])

      # Subscribe on node2, write on node1, wait for event to prove replication
      {:ok, file} =
        subscribe_then_act(
          cluster,
          :node2,
          volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "atomic-vol",
              "/atomic-test.txt",
              "atomic content"
            ])
          end,
          timeout: 15_000
        )

      # Verify FileMeta exists (via get_file)
      {:ok, retrieved} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :get_file, [
          "atomic-vol",
          "/atomic-test.txt"
        ])

      assert retrieved.id == file.id

      # Verify DirectoryEntry contains the file (via list_dir)
      {:ok, root_children} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :list_dir, [
          "atomic-vol",
          "/"
        ])

      assert Map.has_key?(root_children, "atomic-test.txt")

      # Cross-node read — event proved replication, so data is available
      {:ok, f} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :get_file, [
          "atomic-vol",
          "/atomic-test.txt"
        ])

      assert f.id == file.id

      {:ok, root_from_node2} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :list_dir, [
          "atomic-vol",
          "/"
        ])

      assert Map.has_key?(root_from_node2, "atomic-test.txt")
    end
  end

  describe "crash recovery" do
    test "expired intent allows new creation for same path", %{cluster: cluster} do
      :ok = init_tiering_cluster(cluster, "crash-vol")

      # Get the volume ID for intent conflict key
      {:ok, volume_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :get_volume, ["crash-vol"])

      volume_id = volume_info[:id]

      # Simulate an intent from a crashed writer whose TTL has already elapsed:
      # inject `started_at` in the past so `expires_at = started_at + ttl_seconds`
      # is also in the past. Avoids `Process.sleep` — no wall-clock wait is
      # needed because there is nothing to synchronise on.
      intent_id = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
      started_at = DateTime.add(DateTime.utc_now(), -2, :second)

      intent =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.Intent, :new, [
          [
            id: intent_id,
            operation: :file_create,
            conflict_key: {:create, volume_id, "/", "crash_test.txt"},
            started_at: started_at,
            ttl_seconds: 1
          ]
        ])

      {:ok, ^intent_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.IntentLog, :try_acquire, [intent])

      # The stored intent's `state` is `:pending` — that's the lifecycle field,
      # independent of time-based expiry.
      {:ok, stored_intent} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.IntentLog, :get, [intent_id])

      assert stored_intent.state == :pending

      # `Intent.expired?/1` compares `expires_at` to `DateTime.utc_now()` —
      # already true because `started_at` was pre-dated.
      assert Intent.expired?(stored_intent)

      # Now creating a file at the same path should succeed
      # (expired intent is overwritten by new intent)
      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "crash-vol",
          "/crash_test.txt",
          "recovered content"
        ])

      assert file.size == byte_size("recovered content")

      # Verify the file is actually readable
      {:ok, content} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "crash-vol",
          "/crash_test.txt"
        ])

      assert content == "recovered content"
    end
  end

  describe "clock skew detection" do
    test "quarantines node with excessive skew", %{cluster: cluster} do
      :ok = init_tiering_cluster(cluster, "clock-vol")

      node2_atom = PeerCluster.get_node!(cluster, :node2).node
      node2_str = Atom.to_string(node2_atom)

      # Replace the existing ClockMonitor's time_fetcher to inject skew for node2.
      # Use Code.eval_string so the closure is defined on node1's heap and doesn't
      # need to survive an RPC boundary.
      setup_code = """
      target_node = String.to_existing_atom("#{node2_str}")

      :sys.replace_state(NeonFS.Core.ClockMonitor, fn state ->
        %{state |
          quarantine_threshold: 1000,
          time_fetcher: fn node ->
            case :rpc.call(node, System, :system_time, [:millisecond]) do
              {:badrpc, reason} ->
                {:error, reason}

              time when is_integer(time) ->
                if node == target_node do
                  {:ok, time + 5000}
                else
                  {:ok, time}
                end
            end
          end
        }
      end)

      :ok
      """

      {:ok, _bindings} =
        PeerCluster.rpc(cluster, :node1, Code, :eval_string, [setup_code])

      # Trigger a clock check
      clock_pid =
        PeerCluster.rpc(cluster, :node1, Process, :whereis, [NeonFS.Core.ClockMonitor])

      PeerCluster.rpc(cluster, :node1, Kernel, :send, [clock_pid, :check_clocks])

      # Wait for quarantine to be applied
      assert_eventually timeout: 5_000 do
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ClockMonitor, :quarantined?, [node2_atom])
      end

      # Verify quarantined_nodes includes node2
      quarantined =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ClockMonitor, :quarantined_nodes, [])

      assert node2_atom in quarantined
    end
  end

  describe "concurrent writer detection" do
    test "second writer for same path gets conflict error", %{cluster: cluster} do
      :ok = init_tiering_cluster(cluster, "conflict-vol")

      {:ok, volume_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :get_volume, ["conflict-vol"])

      volume_id = volume_info[:id]

      # Node1 acquires intent for creating /conflict.txt
      intent1_id = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)

      intent1 =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.Intent, :new, [
          [
            id: intent1_id,
            operation: :file_create,
            conflict_key: {:create, volume_id, "/", "conflict.txt"}
          ]
        ])

      {:ok, ^intent1_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.IntentLog, :try_acquire, [intent1])

      # Node2 tries to acquire intent for the same path — should conflict
      intent2_id = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)

      intent2 =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.Intent, :new, [
          [
            id: intent2_id,
            operation: :file_create,
            conflict_key: {:create, volume_id, "/", "conflict.txt"}
          ]
        ])

      result =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.IntentLog, :try_acquire, [intent2])

      assert {:error, :conflict, existing} = result
      assert existing.id == intent1_id

      # Cleanup: complete the first intent
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.IntentLog, :complete, [intent1_id])
    end
  end

  describe "erasure-coded volume with quorum metadata" do
    test "write and read on erasure volume in multi-node cluster", %{cluster: cluster} do
      :ok = init_tiering_cluster(cluster, "ec-quorum-vol", durability: "erasure:2:1")

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
          "ec-quorum-vol"
        ])

      test_data = :crypto.strong_rand_bytes(4096)

      # Subscribe on node2, write on node1, wait for event to prove replication
      {:ok, file} =
        subscribe_then_act(
          cluster,
          :node2,
          volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "ec-quorum-vol",
              "/erasure.bin",
              test_data
            ])
          end,
          timeout: 15_000
        )

      assert is_list(file.stripes)
      assert file.stripes != []

      # Read from node1
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "ec-quorum-vol",
          "/erasure.bin"
        ])

      assert read_data == test_data

      # Event proved replication — stripe metadata is now accessible from node2
      [%{stripe_id: sid} | _] = file.stripes

      {:ok, stripe} =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.StripeIndex, :get, [
          file.volume_id,
          sid
        ])

      assert stripe.id == sid
    end
  end

  describe "mixed volume types" do
    @tag :pending_903
    test "replicated and erasure-coded volumes coexist on multi-node cluster", %{
      cluster: cluster
    } do
      # Create both volume types
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "rep-vol",
          %{"durability" => "replicate:1"}
        ])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "ec-vol",
          %{"durability" => "erasure:2:1"}
        ])

      {:ok, rep_volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, ["rep-vol"])

      {:ok, ec_volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, ["ec-vol"])

      rep_data = :crypto.strong_rand_bytes(2048)
      ec_data = :crypto.strong_rand_bytes(2048)

      # Write to replicated volume and wait for replication event on node2
      {:ok, rep_file} =
        subscribe_then_act(
          cluster,
          :node2,
          rep_volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "rep-vol",
              "/rep.bin",
              rep_data
            ])
          end,
          timeout: 15_000
        )

      # Write to erasure volume and wait for replication event on node2
      {:ok, ec_file} =
        subscribe_then_act(
          cluster,
          :node2,
          ec_volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "ec-vol",
              "/ec.bin",
              ec_data
            ])
          end,
          timeout: 15_000
        )

      # Verify replicated file has chunks but no stripes
      assert rep_file.chunks != []

      # Verify erasure file has stripes
      assert is_list(ec_file.stripes)
      assert ec_file.stripes != []

      # Events proved replication — read replicated file from node2
      {:ok, read_rep} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          "rep-vol",
          "/rep.bin"
        ])

      assert read_rep == rep_data

      # EC file metadata accessible from node2 via quorum
      {:ok, ec_from_node2} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :get_file, [
          "ec-vol",
          "/ec.bin"
        ])

      assert ec_from_node2.id == ec_file.id
    end
  end

  # ─── Helpers ──────────────────────────────────────────────────────────

  defp init_tiering_cluster(cluster, volume_name, opts \\ []) do
    durability = Keyword.get(opts, :durability, "replicate:1")

    volume_opts =
      if durability != "replicate:1" do
        %{"durability" => durability}
      else
        %{}
      end

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume_name,
        volume_opts
      ])

    :ok
  end
end
