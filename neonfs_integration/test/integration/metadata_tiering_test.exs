defmodule NeonFS.Integration.MetadataTieringTest do
  @moduledoc """
  Phase 5 integration tests for metadata tiering architecture.

  Tests directory operations, cross-segment atomicity, crash recovery,
  clock skew detection, concurrent writer detection, and mixed volume
  type scenarios on a multi-node cluster.
  """
  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Core.Intent

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 3

  # Short RPC timeout for use inside retry loops — allows multiple attempts
  # within the assert_eventually window instead of blocking on a single slow call
  @retry_rpc_timeout 10_000

  describe "directory listing via DirectoryEntry" do
    test "list_dir returns all children", %{cluster: cluster} do
      :ok = init_multi_node_cluster(cluster, "dir-vol")

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
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
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
    test "file creation writes both FileMeta and DirectoryEntry", %{cluster: cluster} do
      :ok = init_multi_node_cluster(cluster, "atomic-vol")

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "atomic-vol",
          "/atomic-test.txt",
          "atomic content"
        ])

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

      # Verify from a different node — both FileMeta and DirectoryEntry
      # should be accessible via quorum
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.TestHelpers,
               :get_file,
               [
                 "atomic-vol",
                 "/atomic-test.txt"
               ],
               @retry_rpc_timeout
             ) do
          {:ok, f} -> f.id == file.id
          _ -> false
        end
      end

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
      :ok = init_multi_node_cluster(cluster, "crash-vol")

      # Get the volume ID for intent conflict key
      {:ok, volume_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :get_volume, ["crash-vol"])

      volume_id = volume_info[:id]

      # Acquire an intent with very short TTL (simulating a writer that will crash)
      intent_id = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)

      intent =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.Intent, :new, [
          [
            id: intent_id,
            operation: :file_create,
            conflict_key: {:create, volume_id, "/", "crash_test.txt"},
            ttl_seconds: 2
          ]
        ])

      {:ok, ^intent_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.IntentLog, :try_acquire, [intent])

      # Verify the intent is active
      {:ok, active_intent} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.IntentLog, :get, [intent_id])

      assert active_intent.state == :pending

      # Wait for TTL to expire
      Process.sleep(3_000)

      # Verify the intent is now expired
      {:ok, expired_intent} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.IntentLog, :get, [intent_id])

      assert Intent.expired?(expired_intent)

      # Now creating a file at the same path should succeed
      # (expired intent is overwritten by new intent)
      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
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
      :ok = init_multi_node_cluster(cluster, "clock-vol")

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
      :ok = init_multi_node_cluster(cluster, "conflict-vol")

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
      :ok = init_multi_node_cluster(cluster, "ec-quorum-vol", durability: "erasure:2:1")

      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "ec-quorum-vol",
          "/erasure.bin",
          test_data
        ])

      assert is_list(file.stripes)
      assert file.stripes != []

      # Read from node1
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "ec-quorum-vol",
          "/erasure.bin"
        ])

      assert read_data == test_data

      # Verify stripe metadata is accessible from node2 via quorum
      [%{stripe_id: sid} | _] = file.stripes

      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.Core.StripeIndex,
               :get,
               [sid],
               @retry_rpc_timeout
             ) do
          {:ok, stripe} -> stripe.id == sid
          _ -> false
        end
      end
    end
  end

  describe "mixed volume types" do
    test "replicated and erasure-coded volumes coexist on multi-node cluster", %{
      cluster: cluster
    } do
      :ok = init_multi_node_cluster_base(cluster)

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

      rep_data = :crypto.strong_rand_bytes(2048)
      ec_data = :crypto.strong_rand_bytes(2048)

      # Write to both volumes
      {:ok, rep_file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "rep-vol",
          "/rep.bin",
          rep_data
        ])

      {:ok, ec_file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "ec-vol",
          "/ec.bin",
          ec_data
        ])

      # Verify replicated file has chunks but no stripes
      assert rep_file.chunks != []

      # Verify erasure file has stripes
      assert is_list(ec_file.stripes)
      assert ec_file.stripes != []

      # Read replicated file from node2 (full data read)
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.TestHelpers,
               :read_file,
               [
                 "rep-vol",
                 "/rep.bin"
               ],
               @retry_rpc_timeout
             ) do
          {:ok, ^rep_data} -> true
          _ -> false
        end
      end

      # Verify EC file metadata is accessible from node2 via quorum
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.TestHelpers,
               :get_file,
               [
                 "ec-vol",
                 "/ec.bin"
               ],
               @retry_rpc_timeout
             ) do
          {:ok, file} -> file.id == ec_file.id
          _ -> false
        end
      end
    end
  end

  # ─── Helpers ──────────────────────────────────────────────────────────

  defp init_multi_node_cluster_base(cluster) do
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["tiering-test"])

    {:ok, %{"token" => token}} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [3600])

    node1_info = PeerCluster.get_node!(cluster, :node1)
    node1_str = Atom.to_string(node1_info.node)

    # Join nodes sequentially with waits between — Ra rejects concurrent cluster changes
    {:ok, _} =
      PeerCluster.rpc(cluster, :node2, NeonFS.CLI.Handler, :join_cluster, [token, node1_str])

    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, []) do
            {:ok, _status} -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

    {:ok, _} =
      PeerCluster.rpc(cluster, :node3, NeonFS.CLI.Handler, :join_cluster, [token, node1_str])

    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, []) do
            {:ok, _status} -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

    # Wait for ALL peer nodes to see each other AND have MetadataStore running.
    # discover_core_nodes() filters Node.list() by MetadataStore presence,
    # so the ring will be incomplete if MetadataStore isn't ready on all peers.
    peer_nodes = Enum.map([:node1, :node2, :node3], &PeerCluster.get_node!(cluster, &1).node)

    assert_eventually timeout: 30_000 do
      Enum.all?(peer_nodes, fn peer ->
        node_list = :rpc.call(peer, Node, :list, [])
        other_peers = Enum.filter(node_list, &(&1 in peer_nodes))
        all_connected = length(other_peers) >= 2

        has_metadata_store =
          case :rpc.call(peer, Process, :whereis, [NeonFS.Core.MetadataStore]) do
            pid when is_pid(pid) -> true
            _ -> false
          end

        all_connected and has_metadata_store
      end)
    end

    # Rebuild quorum ring on all nodes now that full membership is confirmed
    for node_name <- [:node1, :node2, :node3] do
      PeerCluster.rpc(cluster, node_name, NeonFS.Core.Supervisor, :rebuild_quorum_ring, [])
    end

    :ok
  end

  defp init_multi_node_cluster(cluster, volume_name, opts \\ []) do
    :ok = init_multi_node_cluster_base(cluster)

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
