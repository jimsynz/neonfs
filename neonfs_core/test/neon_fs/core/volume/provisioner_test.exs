defmodule NeonFS.Core.Volume.ProvisionerTest do
  use ExUnit.Case, async: true

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.Volume
  alias NeonFS.Core.Volume.Provisioner
  alias NeonFS.Core.Volume.RootSegment

  describe "provision/2" do
    test "returns {:ok, hash} on the happy path" do
      volume = sample_volume()

      drives = [
        drive("drv-1", :n1@h),
        drive("drv-2", :n2@h),
        drive("drv-3", :n3@h)
      ]

      registered_commands = :ets.new(:registered_commands, [:public, :duplicate_bag])
      replicator = stub_replicator("hash-bytes")

      assert {:ok, "hash-bytes"} =
               Provisioner.provision(volume,
                 cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
                 drive_lister: fn -> {:ok, drives} end,
                 chunk_replicator: replicator,
                 bootstrap_registrar: capture_registrar(registered_commands)
               )

      [{:cmd, command}] = :ets.lookup(registered_commands, :cmd)

      {:register_volume_root, entry} = command
      assert entry.volume_id == volume.id
      assert entry.root_chunk_hash == "hash-bytes"
      assert entry.durability_cache == volume.durability
      assert length(entry.drive_locations) == volume.durability.factor
      assert %DateTime{} = entry.updated_at
    end

    test "decodable root segment is what gets replicated" do
      volume = sample_volume()
      drives = [drive("drv-1", :n1@h), drive("drv-2", :n2@h), drive("drv-3", :n3@h)]
      captured = capture_replicator()

      _ =
        Provisioner.provision(volume,
          cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
          drive_lister: fn -> {:ok, drives} end,
          chunk_replicator: captured.module,
          bootstrap_registrar: fn _ -> {:ok, :ok} end
        )

      [encoded_chunk] = captured.calls.()
      assert {:ok, segment} = RootSegment.decode(encoded_chunk)
      assert segment.volume_id == volume.id
      assert segment.cluster_id == "clust-test"
      assert segment.durability == volume.durability
      assert segment.index_roots == %{file_index: nil, chunk_index: nil, stripe_index: nil}
    end

    test "min_copies for :replicate matches durability.min_copies" do
      volume = %{sample_volume() | durability: %{type: :replicate, factor: 3, min_copies: 2}}
      captured = capture_replicator()

      _ =
        Provisioner.provision(volume,
          cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
          drive_lister: fn ->
            {:ok, [drive("drv-1", :n1@h), drive("drv-2", :n2@h), drive("drv-3", :n3@h)]}
          end,
          chunk_replicator: captured.module,
          bootstrap_registrar: fn _ -> {:ok, :ok} end
        )

      [opts] = captured.opts.()
      assert opts[:min_copies] == 2
    end

    test "min_copies for :erasure is data_chunks" do
      volume = %{
        sample_volume()
        | durability: %{type: :erasure, data_chunks: 3, parity_chunks: 2}
      }

      captured = capture_replicator()

      _ =
        Provisioner.provision(volume,
          cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
          drive_lister: fn ->
            {:ok, for(n <- 1..5, do: drive("drv-#{n}", :"n#{n}@h"))}
          end,
          chunk_replicator: captured.module,
          bootstrap_registrar: fn _ -> {:ok, :ok} end
        )

      [opts] = captured.opts.()
      assert opts[:min_copies] == 3
    end

    test "surfaces cluster_state_unavailable error" do
      volume = sample_volume()

      assert {:error, {:cluster_state_unavailable, :no_state}} =
               Provisioner.provision(volume,
                 cluster_state_loader: fn -> {:error, :no_state} end,
                 drive_lister: fn -> {:ok, []} end,
                 chunk_replicator: stub_replicator("h"),
                 bootstrap_registrar: fn _ -> {:ok, :ok} end
               )
    end

    test "surfaces drive_query_failed error" do
      volume = sample_volume()

      assert {:error, {:drive_query_failed, :timeout}} =
               Provisioner.provision(volume,
                 cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
                 drive_lister: fn -> {:error, :timeout} end,
                 chunk_replicator: stub_replicator("h"),
                 bootstrap_registrar: fn _ -> {:ok, :ok} end
               )
    end

    test "surfaces insufficient_drives error from DriveSelector" do
      volume = sample_volume()

      assert {:error, :insufficient_drives, %{available: 1, needed: 2}} =
               Provisioner.provision(volume,
                 cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
                 drive_lister: fn -> {:ok, [drive("drv-1", :n1@h)]} end,
                 chunk_replicator: stub_replicator("h"),
                 bootstrap_registrar: fn _ -> {:ok, :ok} end
               )
    end

    test "surfaces insufficient_replicas error from ChunkReplicator" do
      volume = sample_volume()

      drives = [drive("drv-1", :n1@h), drive("drv-2", :n2@h), drive("drv-3", :n3@h)]

      replicator =
        stub_replicator_failure(:insufficient_replicas, %{successful: [], failed: [], needed: 2})

      assert {:error, :insufficient_replicas, _} =
               Provisioner.provision(volume,
                 cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
                 drive_lister: fn -> {:ok, drives} end,
                 chunk_replicator: replicator,
                 bootstrap_registrar: fn _ -> {:ok, :ok} end
               )
    end

    test "surfaces bootstrap_register_failed error from registrar" do
      volume = sample_volume()
      drives = [drive("drv-1", :n1@h), drive("drv-2", :n2@h), drive("drv-3", :n3@h)]

      assert {:error, {:bootstrap_register_failed, :ra_timeout}} =
               Provisioner.provision(volume,
                 cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
                 drive_lister: fn -> {:ok, drives} end,
                 chunk_replicator: stub_replicator("h"),
                 bootstrap_registrar: fn _ -> {:error, :ra_timeout} end
               )
    end
  end

  ## Helpers

  defp sample_volume do
    Volume.new("test-vol", durability: %{type: :replicate, factor: 3, min_copies: 2})
  end

  defp sample_cluster_state do
    %ClusterState{
      cluster_id: "clust-test",
      cluster_name: "test-cluster",
      created_at: DateTime.utc_now(),
      master_key: <<0::256>>,
      this_node: node()
    }
  end

  defp drive(drive_id, node) do
    %{
      drive_id: drive_id,
      node: node,
      cluster_id: "clust-test",
      on_disk_format_version: 1,
      registered_at: DateTime.utc_now()
    }
  end

  # A stub replicator module that always returns {:ok, hash, summary}.
  defmodule SuccessReplicator do
    def write_chunk(_data, drives, _opts) do
      {:ok, "fixed-hash", %{successful: Enum.map(drives, & &1.drive_id), failed: []}}
    end
  end

  defp stub_replicator(hash) do
    name =
      String.to_atom(
        "Elixir.NeonFS.Core.Volume.ProvisionerTest.SR#{:erlang.unique_integer([:positive])}"
      )

    Module.create(
      name,
      quote do
        def write_chunk(_data, drives, _opts) do
          {:ok, unquote(hash), %{successful: Enum.map(drives, & &1.drive_id), failed: []}}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    name
  end

  defp stub_replicator_failure(reason, info) do
    name =
      String.to_atom(
        "Elixir.NeonFS.Core.Volume.ProvisionerTest.FR#{:erlang.unique_integer([:positive])}"
      )

    {reason_code, info_term} = {reason, info}

    Module.create(
      name,
      quote do
        @reason unquote(Macro.escape(reason_code))
        @info unquote(Macro.escape(info_term))
        def write_chunk(_data, _drives, _opts), do: {:error, @reason, @info}
      end,
      Macro.Env.location(__ENV__)
    )

    name
  end

  defp capture_replicator do
    {:ok, agent} = Agent.start_link(fn -> %{calls: [], opts: []} end)
    name = String.to_atom("Elixir.NeonFS.Core.Volume.ProvisionerTest.CR#{:erlang.phash2(agent)}")

    # The stub module looks up the agent at compile time via :persistent_term —
    # we use a unique key per module so async tests don't collide.
    pt_key = {name, :agent}
    :persistent_term.put(pt_key, agent)

    Module.create(
      name,
      quote do
        @pt_key unquote(Macro.escape(pt_key))
        def write_chunk(data, drives, opts) do
          agent = :persistent_term.get(@pt_key)

          Agent.update(agent, fn s ->
            %{calls: [data | s.calls], opts: [opts | s.opts]}
          end)

          {:ok, "captured-hash", %{successful: Enum.map(drives, & &1.drive_id), failed: []}}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    %{
      module: name,
      calls: fn -> Agent.get(agent, & &1.calls) |> Enum.reverse() end,
      opts: fn -> Agent.get(agent, & &1.opts) |> Enum.reverse() end
    }
  end

  defp capture_registrar(table) do
    fn command ->
      :ets.insert(table, {:cmd, command})
      {:ok, :registered}
    end
  end
end
