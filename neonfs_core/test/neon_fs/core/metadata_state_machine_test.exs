defmodule NeonFS.Core.MetadataStateMachineTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.{Intent, MetadataStateMachine}

  defp base_state do
    %{
      data: %{},
      chunks: %{},
      files: %{},
      services: %{},
      volumes: %{},
      stripes: %{},
      segment_assignments: %{},
      intents: %{},
      active_intents_by_conflict_key: %{},
      encryption_keys: %{},
      volume_acls: %{},
      version: 0
    }
  end

  defp make_intent(overrides \\ []) do
    now = DateTime.utc_now()
    ttl = Keyword.get(overrides, :ttl_seconds, 300)

    defaults = [
      id: "intent-#{System.unique_integer([:positive])}",
      operation: :write_file,
      conflict_key: {:file, "test-file"},
      params: %{},
      started_at: now,
      ttl_seconds: ttl
    ]

    attrs = Keyword.merge(defaults, overrides)
    Intent.new(attrs)
  end

  defp make_expired_intent(overrides) do
    past = DateTime.add(DateTime.utc_now(), -600, :second)
    make_intent(Keyword.merge([started_at: past, ttl_seconds: 1], overrides))
  end

  describe "version/0" do
    test "returns 10" do
      assert MetadataStateMachine.version() == 10
    end
  end

  describe "which_module/1" do
    test "returns the same module for all versions" do
      for v <- 1..7 do
        assert MetadataStateMachine.which_module(v) == MetadataStateMachine
      end
    end
  end

  describe "init/1" do
    test "initialises empty v6 state" do
      state = MetadataStateMachine.init(%{})

      assert state.data == %{}
      assert state.chunks == %{}
      assert state.files == %{}
      assert state.services == %{}
      assert state.volumes == %{}
      assert state.stripes == %{}
      assert state.segment_assignments == %{}
      assert state.intents == %{}
      assert state.active_intents_by_conflict_key == %{}
      assert state.encryption_keys == %{}
      assert state.volume_acls == %{}
      assert state.version == 0
    end
  end

  describe "machine version migration 4 -> 5" do
    test "adds v5 fields to existing state" do
      old_state = %{
        data: %{some: :data},
        chunks: %{"hash1" => %{hash: "hash1"}},
        files: %{"file1" => %{id: "file1"}},
        services: %{node1: %{type: :core}},
        volumes: %{"vol1" => %{id: "vol1"}},
        stripes: %{"s1" => %{id: "s1"}},
        version: 100
      }

      {new_state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:machine_version, 4, 5}, old_state)

      # Existing data preserved
      assert new_state.data == %{some: :data}
      assert new_state.chunks == %{"hash1" => %{hash: "hash1"}}
      assert new_state.files == %{"file1" => %{id: "file1"}}
      assert new_state.services == %{node1: %{type: :core}}
      assert new_state.volumes == %{"vol1" => %{id: "vol1"}}
      assert new_state.stripes == %{"s1" => %{id: "s1"}}
      assert new_state.version == 100

      # New v5 fields added
      assert new_state.segment_assignments == %{}
      assert new_state.intents == %{}
      assert new_state.active_intents_by_conflict_key == %{}
    end
  end

  describe "machine version migration 8 -> 9 (IAM tables)" do
    test "adds the four IAM tables as empty maps" do
      old_state = %{
        data: %{},
        chunks: %{},
        files: %{},
        services: %{},
        volumes: %{},
        stripes: %{},
        segment_assignments: %{},
        intents: %{},
        active_intents_by_conflict_key: %{},
        encryption_keys: %{},
        volume_acls: %{},
        s3_credentials: %{},
        escalations: %{},
        version: 200
      }

      {new_state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:machine_version, 8, 9}, old_state)

      assert new_state.iam_users == %{}
      assert new_state.iam_groups == %{}
      assert new_state.iam_policies == %{}
      assert new_state.iam_identity_mappings == %{}
      # Existing state untouched.
      assert new_state.volume_acls == %{}
      assert new_state.version == 200
    end

    test "leaves existing IAM tables intact when they already exist" do
      existing_users = %{"u-1" => %{name: "pre-existing"}}

      old_state = %{
        data: %{},
        chunks: %{},
        files: %{},
        services: %{},
        volumes: %{},
        stripes: %{},
        segment_assignments: %{},
        intents: %{},
        active_intents_by_conflict_key: %{},
        encryption_keys: %{},
        volume_acls: %{},
        s3_credentials: %{},
        escalations: %{},
        iam_users: existing_users,
        version: 200
      }

      {new_state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:machine_version, 8, 9}, old_state)

      assert new_state.iam_users == existing_users
      assert new_state.iam_groups == %{}
    end
  end

  describe "KV commands (v10)" do
    defp v10_state do
      %{
        data: %{},
        chunks: %{},
        files: %{},
        services: %{},
        volumes: %{},
        stripes: %{},
        segment_assignments: %{},
        intents: %{},
        active_intents_by_conflict_key: %{},
        encryption_keys: %{},
        volume_acls: %{},
        s3_credentials: %{},
        escalations: %{},
        kv: %{},
        version: 0
      }
    end

    test "kv_put / kv_delete round-trip" do
      state = v10_state()

      {after_put, :ok, []} =
        MetadataStateMachine.apply(%{}, {:kv_put, "alice", %{name: "Alice"}}, state)

      assert after_put.kv == %{"alice" => %{name: "Alice"}}
      assert after_put.version == state.version + 1

      {after_delete, :ok, []} =
        MetadataStateMachine.apply(%{}, {:kv_delete, "alice"}, after_put)

      assert after_delete.kv == %{}
      assert after_delete.version == after_put.version + 1
    end

    test "kv_put stores arbitrary (non-map) values" do
      state = v10_state()

      {after_put, :ok, []} =
        MetadataStateMachine.apply(%{}, {:kv_put, "count", 42}, state)

      assert after_put.kv == %{"count" => 42}
    end

    test "get_kv/1 returns the kv map" do
      state = v10_state() |> Map.put(:kv, %{"k" => "v"})
      assert MetadataStateMachine.get_kv(state) == %{"k" => "v"}
    end

    test "kv_put defensively re-creates a missing :kv on a pre-v10 snapshot" do
      pre_v10 = v10_state() |> Map.delete(:kv)

      {new_state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:kv_put, "k", %{v: 1}}, pre_v10)

      assert new_state.kv == %{"k" => %{v: 1}}
    end
  end

  describe "machine version migration 9 -> 10 (generic kv)" do
    test "drops the four IAM tables and introduces the flat :kv map" do
      old_state = %{
        data: %{},
        chunks: %{},
        files: %{},
        services: %{},
        volumes: %{},
        stripes: %{},
        segment_assignments: %{},
        intents: %{},
        active_intents_by_conflict_key: %{},
        encryption_keys: %{},
        volume_acls: %{},
        s3_credentials: %{},
        escalations: %{},
        iam_users: %{},
        iam_groups: %{},
        iam_policies: %{},
        iam_identity_mappings: %{},
        version: 300
      }

      {new_state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:machine_version, 9, 10}, old_state)

      refute Map.has_key?(new_state, :iam_users)
      refute Map.has_key?(new_state, :iam_groups)
      refute Map.has_key?(new_state, :iam_policies)
      refute Map.has_key?(new_state, :iam_identity_mappings)
      assert new_state.kv == %{}
      assert new_state.version == 300
    end
  end

  describe "machine version migration 5 -> 6" do
    test "adds v6 fields to existing state" do
      old_state = %{
        data: %{some: :data},
        chunks: %{"hash1" => %{hash: "hash1"}},
        files: %{"file1" => %{id: "file1"}},
        services: %{node1: %{type: :core}},
        volumes: %{"vol1" => %{id: "vol1"}},
        stripes: %{"s1" => %{id: "s1"}},
        segment_assignments: %{"seg-1" => %{replica_set: [:n1], version: 1}},
        intents: %{},
        active_intents_by_conflict_key: %{},
        version: 200
      }

      {new_state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:machine_version, 5, 6}, old_state)

      # Existing data preserved
      assert new_state.data == %{some: :data}
      assert new_state.chunks == %{"hash1" => %{hash: "hash1"}}
      assert new_state.files == %{"file1" => %{id: "file1"}}
      assert new_state.services == %{node1: %{type: :core}}
      assert new_state.volumes == %{"vol1" => %{id: "vol1"}}
      assert new_state.stripes == %{"s1" => %{id: "s1"}}
      assert new_state.segment_assignments == %{"seg-1" => %{replica_set: [:n1], version: 1}}
      assert new_state.version == 200

      # New v6 fields added
      assert new_state.encryption_keys == %{}
      assert new_state.volume_acls == %{}
    end

    test "does not overwrite existing v6 fields if already present" do
      state_with_fields = %{
        data: %{},
        chunks: %{},
        files: %{},
        services: %{},
        volumes: %{},
        stripes: %{},
        segment_assignments: %{},
        intents: %{},
        active_intents_by_conflict_key: %{},
        encryption_keys: %{"vol1" => %{1 => %{wrapped_key: "key"}}},
        volume_acls: %{"vol1" => %{owner_uid: 1000}},
        version: 100
      }

      {new_state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:machine_version, 5, 6}, state_with_fields)

      assert new_state.encryption_keys == %{"vol1" => %{1 => %{wrapped_key: "key"}}}
      assert new_state.volume_acls == %{"vol1" => %{owner_uid: 1000}}
    end
  end

  describe "{:put, key, value} and {:delete, key}" do
    test "stores and removes key-value pairs" do
      state = base_state()

      {state, :ok, []} = MetadataStateMachine.apply(%{}, {:put, :foo, :bar}, state)
      assert state.data == %{foo: :bar}
      assert state.version == 1

      {state, :ok, []} = MetadataStateMachine.apply(%{}, {:delete, :foo}, state)
      assert state.data == %{}
      assert state.version == 2
    end
  end

  describe "volume commands" do
    test "put_volume stores volume" do
      volume = %{id: "vol-1", name: "test"}

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:put_volume, volume}, base_state())

      assert state.volumes["vol-1"] == volume
      assert state.version == 1
    end

    test "delete_volume removes volume" do
      volume = %{id: "vol-1", name: "test"}
      state = %{base_state() | volumes: %{"vol-1" => volume}}

      {state, :ok, []} = MetadataStateMachine.apply(%{}, {:delete_volume, "vol-1"}, state)

      assert state.volumes == %{}
      assert state.version == 1
    end
  end

  describe "service registry commands" do
    test "register and deregister service" do
      service = %{node: :node1@host, type: :core, status: :active}

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:register_service, service}, base_state())

      assert state.services[{:node1@host, :core}] == service

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:deregister_service, :node1@host}, state)

      assert state.services == %{}
    end

    test "update_service_status" do
      service = %{node: :node1@host, type: :core, status: :active}
      state = %{base_state() | services: %{{:node1@host, :core} => service}}

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:update_service_status, :node1@host, :draining},
          state
        )

      assert state.services[{:node1@host, :core}].status == :draining
    end

    test "update_service_status returns error for unknown node" do
      {_state, {:error, :not_found}, []} =
        MetadataStateMachine.apply(
          %{},
          {:update_service_status, :unknown@host, :active},
          base_state()
        )
    end

    test "update_service_metrics" do
      service = %{node: :node1@host, type: :core, metrics: %{}}
      state = %{base_state() | services: %{{:node1@host, :core} => service}}

      metrics = %{cpu: 0.5, memory: 1024}

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:update_service_metrics, :node1@host, metrics},
          state
        )

      assert state.services[{:node1@host, :core}].metrics == metrics
    end
  end

  describe "legacy chunk commands" do
    test "put_chunk and delete_chunk" do
      chunk = %{hash: "abc123", size: 1024}

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:put_chunk, chunk}, base_state())

      assert state.chunks["abc123"] == chunk
      assert state.version == 1

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:delete_chunk, "abc123"}, state)

      assert state.chunks == %{}
      assert state.version == 2
    end

    test "commit_chunk" do
      chunk = %{hash: "abc123", active_write_refs: MapSet.new()}

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:put_chunk, chunk}, base_state())

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:commit_chunk, "abc123"}, state)

      assert state.chunks["abc123"].commit_state == :committed
    end

    test "update_chunk_locations" do
      chunk = %{hash: "abc123", locations: []}

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:put_chunk, chunk}, base_state())

      locations = [%{node: :n1, drive_id: "d1", tier: :hot}]

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:update_chunk_locations, "abc123", locations},
          state
        )

      assert state.chunks["abc123"].locations == locations
    end
  end

  describe "legacy file commands" do
    test "put_file, update_file, delete_file" do
      file = %{id: "f1", path: "/test.txt", size: 100}

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:put_file, file}, base_state())

      assert state.files["f1"] == file

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:update_file, "f1", %{size: 200}}, state)

      assert state.files["f1"].size == 200

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:delete_file, "f1"}, state)

      assert state.files == %{}
    end
  end

  describe "legacy stripe commands" do
    test "put_stripe, update_stripe, delete_stripe" do
      stripe = %{id: "s1", volume_id: "v1"}

      {state, {:ok, "s1"}, []} =
        MetadataStateMachine.apply(%{}, {:put_stripe, stripe}, base_state())

      assert state.stripes["s1"] == stripe

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:update_stripe, "s1", %{partial: true}}, state)

      assert state.stripes["s1"].partial == true

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:delete_stripe, "s1"}, state)

      assert state.stripes == %{}
    end
  end

  describe "{:assign_segment, segment_id, replica_set}" do
    test "assigns a new segment" do
      replica_set = [:node1@host, :node2@host, :node3@host]

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:assign_segment, "seg-1", replica_set},
          base_state()
        )

      assert state.segment_assignments["seg-1"] == %{
               replica_set: replica_set,
               version: 1
             }

      assert state.version == 1
    end

    test "re-assigning increments assignment version" do
      initial_set = [:node1@host, :node2@host]
      new_set = [:node2@host, :node3@host]

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:assign_segment, "seg-1", initial_set},
          base_state()
        )

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:assign_segment, "seg-1", new_set},
          state
        )

      assert state.segment_assignments["seg-1"].replica_set == new_set
      assert state.segment_assignments["seg-1"].version == 2
    end
  end

  describe "{:bulk_update_assignments, assignments}" do
    test "assigns multiple segments at once" do
      assignments = %{
        "seg-1" => [:node1@host, :node2@host],
        "seg-2" => [:node2@host, :node3@host],
        "seg-3" => [:node1@host, :node3@host]
      }

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:bulk_update_assignments, assignments},
          base_state()
        )

      assert map_size(state.segment_assignments) == 3
      assert state.segment_assignments["seg-1"].version == 1
      assert state.segment_assignments["seg-2"].version == 1
      assert state.version == 1
    end

    test "increments version for existing segments" do
      existing = %{
        "seg-1" => %{replica_set: [:node1@host], version: 3}
      }

      state = %{base_state() | segment_assignments: existing}

      assignments = %{
        "seg-1" => [:node1@host, :node2@host],
        "seg-2" => [:node3@host]
      }

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:bulk_update_assignments, assignments},
          state
        )

      assert state.segment_assignments["seg-1"].version == 4
      assert state.segment_assignments["seg-2"].version == 1
    end
  end

  describe "{:try_acquire_intent, intent}" do
    test "acquires intent when no conflict" do
      intent = make_intent()

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, intent}, base_state())

      assert Map.has_key?(state.intents, intent.id)
      assert state.active_intents_by_conflict_key[intent.conflict_key] == intent.id
      assert state.version == 1
    end

    test "returns conflict when active intent exists with same conflict key" do
      first = make_intent(id: "first", conflict_key: {:file, "f1"})
      second = make_intent(id: "second", conflict_key: {:file, "f1"})

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, first}, base_state())

      {state, {:ok, :conflict, existing}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, second}, state)

      assert existing.id == "first"
      assert state.active_intents_by_conflict_key[{:file, "f1"}] == "first"
    end

    test "acquires intent when existing intent has expired" do
      expired = make_expired_intent(id: "old", conflict_key: {:file, "f1"})
      new_intent = make_intent(id: "new", conflict_key: {:file, "f1"})

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, expired}, base_state())

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, new_intent}, state)

      assert state.active_intents_by_conflict_key[{:file, "f1"}] == "new"
      assert state.intents["old"].state == :expired
      assert state.intents["new"].state == :pending
    end

    test "different conflict keys do not conflict" do
      first = make_intent(id: "first", conflict_key: {:file, "f1"})
      second = make_intent(id: "second", conflict_key: {:file, "f2"})

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, first}, base_state())

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, second}, state)

      assert map_size(state.active_intents_by_conflict_key) == 2
    end
  end

  describe "{:complete_intent, intent_id}" do
    test "marks intent as completed and releases conflict key" do
      intent = make_intent(id: "test-intent")

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, intent}, base_state())

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:complete_intent, "test-intent"}, state)

      assert state.intents["test-intent"].state == :completed
      assert state.intents["test-intent"].completed_at != nil
      refute Map.has_key?(state.active_intents_by_conflict_key, intent.conflict_key)
    end

    test "returns error for non-existent intent" do
      {_state, {:error, :not_found}, []} =
        MetadataStateMachine.apply(%{}, {:complete_intent, "nonexistent"}, base_state())
    end
  end

  describe "{:fail_intent, intent_id, reason}" do
    test "marks intent as failed and releases conflict key" do
      intent = make_intent(id: "test-intent")

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, intent}, base_state())

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:fail_intent, "test-intent", :timeout},
          state
        )

      assert state.intents["test-intent"].state == :failed
      assert state.intents["test-intent"].error == :timeout
      assert state.intents["test-intent"].completed_at != nil
      refute Map.has_key?(state.active_intents_by_conflict_key, intent.conflict_key)
    end

    test "returns error for non-existent intent" do
      {_state, {:error, :not_found}, []} =
        MetadataStateMachine.apply(
          %{},
          {:fail_intent, "nonexistent", :reason},
          base_state()
        )
    end
  end

  describe "{:extend_intent, intent_id, additional_seconds}" do
    test "extends the TTL of a pending intent" do
      intent = make_intent(id: "test-intent", ttl_seconds: 300)

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, intent}, base_state())

      original_expires = state.intents["test-intent"].expires_at

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:extend_intent, "test-intent", 600},
          state
        )

      extended_expires = state.intents["test-intent"].expires_at
      assert DateTime.compare(extended_expires, original_expires) == :gt

      diff = DateTime.diff(extended_expires, original_expires, :second)
      assert diff == 600
    end

    test "returns error for non-existent intent" do
      {_state, {:error, :not_found}, []} =
        MetadataStateMachine.apply(
          %{},
          {:extend_intent, "nonexistent", 300},
          base_state()
        )
    end

    test "returns error for non-pending intent" do
      intent = make_intent(id: "test-intent")

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, intent}, base_state())

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:complete_intent, "test-intent"}, state)

      {_state, {:error, :not_pending}, []} =
        MetadataStateMachine.apply(
          %{},
          {:extend_intent, "test-intent", 300},
          state
        )
    end
  end

  describe ":cleanup_expired_intents" do
    test "removes expired pending intents" do
      expired1 = make_expired_intent(id: "exp1", conflict_key: {:file, "f1"})
      expired2 = make_expired_intent(id: "exp2", conflict_key: {:file, "f2"})
      active = make_intent(id: "active", conflict_key: {:file, "f3"})

      state = base_state()

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, expired1}, state)

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, expired2}, state)

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, active}, state)

      assert map_size(state.active_intents_by_conflict_key) == 3

      {state, {:ok, 2}, []} =
        MetadataStateMachine.apply(%{}, :cleanup_expired_intents, state)

      assert state.intents["exp1"].state == :expired
      assert state.intents["exp2"].state == :expired
      assert state.intents["active"].state == :pending

      assert map_size(state.active_intents_by_conflict_key) == 1
      assert state.active_intents_by_conflict_key[{:file, "f3"}] == "active"
    end

    test "returns zero when no intents are expired" do
      intent = make_intent()

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, intent}, base_state())

      {_state, {:ok, 0}, []} =
        MetadataStateMachine.apply(%{}, :cleanup_expired_intents, state)
    end

    test "skips already-completed intents" do
      intent = make_expired_intent(id: "done", conflict_key: {:file, "f1"})

      {state, {:ok, :acquired}, []} =
        MetadataStateMachine.apply(%{}, {:try_acquire_intent, intent}, base_state())

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:complete_intent, "done"}, state)

      {state, {:ok, 0}, []} =
        MetadataStateMachine.apply(%{}, :cleanup_expired_intents, state)

      assert state.intents["done"].state == :completed
    end
  end

  describe "encryption key commands" do
    defp make_wrapped_entry(overrides \\ %{}) do
      Map.merge(
        %{
          wrapped_key: :crypto.strong_rand_bytes(32),
          created_at: DateTime.utc_now(),
          deprecated_at: nil
        },
        overrides
      )
    end

    test "put_encryption_key stores a key for a volume" do
      entry = make_wrapped_entry()

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:put_encryption_key, "vol-1", 1, entry},
          base_state()
        )

      assert state.encryption_keys["vol-1"][1] == entry
      assert state.version == 1
    end

    test "put_encryption_key adds multiple versions for same volume" do
      entry_v1 = make_wrapped_entry()
      entry_v2 = make_wrapped_entry()

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:put_encryption_key, "vol-1", 1, entry_v1},
          base_state()
        )

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:put_encryption_key, "vol-1", 2, entry_v2},
          state
        )

      assert state.encryption_keys["vol-1"][1] == entry_v1
      assert state.encryption_keys["vol-1"][2] == entry_v2
      assert state.version == 2
    end

    test "put_encryption_key replaces existing key version" do
      entry_v1 = make_wrapped_entry()
      replacement = make_wrapped_entry()

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:put_encryption_key, "vol-1", 1, entry_v1},
          base_state()
        )

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:put_encryption_key, "vol-1", 1, replacement},
          state
        )

      assert state.encryption_keys["vol-1"][1] == replacement
    end

    test "delete_encryption_key removes a specific key version" do
      entry_v1 = make_wrapped_entry()
      entry_v2 = make_wrapped_entry()

      state = %{
        base_state()
        | encryption_keys: %{"vol-1" => %{1 => entry_v1, 2 => entry_v2}}
      }

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:delete_encryption_key, "vol-1", 1},
          state
        )

      refute Map.has_key?(state.encryption_keys["vol-1"], 1)
      assert state.encryption_keys["vol-1"][2] == entry_v2
      assert state.version == 1
    end

    test "delete_encryption_key removes volume entry when last key deleted" do
      entry = make_wrapped_entry()
      state = %{base_state() | encryption_keys: %{"vol-1" => %{1 => entry}}}

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:delete_encryption_key, "vol-1", 1},
          state
        )

      refute Map.has_key?(state.encryption_keys, "vol-1")
    end

    test "delete_encryption_key returns error for unknown volume" do
      {_state, {:error, :not_found}, []} =
        MetadataStateMachine.apply(
          %{},
          {:delete_encryption_key, "nonexistent", 1},
          base_state()
        )
    end

    test "set_current_key_version updates volume encryption config" do
      volume = %{
        id: "vol-1",
        name: "test",
        encryption: %{mode: :server_side, current_key_version: 1}
      }

      state = %{base_state() | volumes: %{"vol-1" => volume}}

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:set_current_key_version, "vol-1", 2},
          state
        )

      assert state.volumes["vol-1"].encryption.current_key_version == 2
      assert state.version == 1
    end

    test "set_current_key_version creates encryption map if not present" do
      volume = %{id: "vol-1", name: "test"}
      state = %{base_state() | volumes: %{"vol-1" => volume}}

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:set_current_key_version, "vol-1", 1},
          state
        )

      assert state.volumes["vol-1"].encryption.current_key_version == 1
    end

    test "set_current_key_version returns error for unknown volume" do
      {_state, {:error, :not_found}, []} =
        MetadataStateMachine.apply(
          %{},
          {:set_current_key_version, "nonexistent", 1},
          base_state()
        )
    end
  end

  describe "volume ACL commands" do
    test "put_volume_acl stores ACL data" do
      acl = %{volume_id: "vol-1", owner_uid: 1000, owner_gid: 1000, entries: []}

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:put_volume_acl, "vol-1", acl},
          base_state()
        )

      assert state.volume_acls["vol-1"] == acl
      assert state.version == 1
    end

    test "put_volume_acl replaces existing ACL" do
      old_acl = %{volume_id: "vol-1", owner_uid: 1000, owner_gid: 1000, entries: []}
      new_acl = %{volume_id: "vol-1", owner_uid: 2000, owner_gid: 2000, entries: []}

      state = %{base_state() | volume_acls: %{"vol-1" => old_acl}}

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:put_volume_acl, "vol-1", new_acl},
          state
        )

      assert state.volume_acls["vol-1"] == new_acl
    end

    test "update_volume_acl merges updates into existing ACL" do
      acl = %{volume_id: "vol-1", owner_uid: 1000, owner_gid: 1000, entries: []}
      state = %{base_state() | volume_acls: %{"vol-1" => acl}}

      entry = %{principal: {:uid, 2000}, permissions: MapSet.new([:read])}

      {state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:update_volume_acl, "vol-1", %{entries: [entry]}},
          state
        )

      assert state.volume_acls["vol-1"].entries == [entry]
      assert state.volume_acls["vol-1"].owner_uid == 1000
      assert state.version == 1
    end

    test "update_volume_acl returns error for unknown volume" do
      {_state, {:error, :not_found}, []} =
        MetadataStateMachine.apply(
          %{},
          {:update_volume_acl, "nonexistent", %{entries: []}},
          base_state()
        )
    end
  end

  describe "volume deletion cascade" do
    test "delete_volume removes encryption keys and ACL" do
      volume = %{id: "vol-1", name: "test"}
      entry = %{wrapped_key: <<1, 2, 3>>, created_at: DateTime.utc_now(), deprecated_at: nil}
      acl = %{volume_id: "vol-1", owner_uid: 1000, owner_gid: 1000, entries: []}

      state = %{
        base_state()
        | volumes: %{"vol-1" => volume, "vol-2" => %{id: "vol-2", name: "other"}},
          encryption_keys: %{"vol-1" => %{1 => entry}, "vol-2" => %{1 => entry}},
          volume_acls: %{"vol-1" => acl, "vol-2" => %{volume_id: "vol-2"}}
      }

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:delete_volume, "vol-1"}, state)

      # vol-1 data removed
      refute Map.has_key?(state.volumes, "vol-1")
      refute Map.has_key?(state.encryption_keys, "vol-1")
      refute Map.has_key?(state.volume_acls, "vol-1")

      # vol-2 data preserved
      assert Map.has_key?(state.volumes, "vol-2")
      assert Map.has_key?(state.encryption_keys, "vol-2")
      assert Map.has_key?(state.volume_acls, "vol-2")
    end

    test "delete_volume works when volume has no keys or ACL" do
      volume = %{id: "vol-1", name: "test"}
      state = %{base_state() | volumes: %{"vol-1" => volume}}

      {state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:delete_volume, "vol-1"}, state)

      assert state.volumes == %{}
      assert state.encryption_keys == %{}
      assert state.volume_acls == %{}
    end
  end

  describe "unknown commands" do
    test "catch-all returns :unknown_command for truly unknown commands" do
      state = base_state()

      {^state, {:error, :unknown_command}, []} =
        MetadataStateMachine.apply(%{}, {:some_future_command, "arg"}, state)
    end
  end

  describe "query functions" do
    test "get_segment_assignments/1 returns assignments" do
      assignments = %{
        "seg-1" => %{replica_set: [:node1@host], version: 1}
      }

      state = %{base_state() | segment_assignments: assignments}
      assert MetadataStateMachine.get_segment_assignments(state) == assignments
    end

    test "get_intent/2 returns intent or nil" do
      intent = make_intent(id: "test")
      state = %{base_state() | intents: %{"test" => intent}}

      assert MetadataStateMachine.get_intent(state, "test") == intent
      assert MetadataStateMachine.get_intent(state, "missing") == nil
    end

    test "get_encryption_keys/2 returns keys for a volume or nil" do
      keys = %{1 => %{wrapped_key: <<1>>, created_at: DateTime.utc_now(), deprecated_at: nil}}
      state = %{base_state() | encryption_keys: %{"vol-1" => keys}}

      assert MetadataStateMachine.get_encryption_keys(state, "vol-1") == keys
      assert MetadataStateMachine.get_encryption_keys(state, "missing") == nil
    end

    test "get_volume_acl/2 returns ACL for a volume or nil" do
      acl = %{volume_id: "vol-1", owner_uid: 1000, owner_gid: 1000, entries: []}
      state = %{base_state() | volume_acls: %{"vol-1" => acl}}

      assert MetadataStateMachine.get_volume_acl(state, "vol-1") == acl
      assert MetadataStateMachine.get_volume_acl(state, "missing") == nil
    end

    test "list_active_intents/1 returns only active intents" do
      active = make_intent(id: "active", conflict_key: {:file, "f1"})
      completed = %{make_intent(id: "done", conflict_key: {:file, "f2"}) | state: :completed}

      state = %{
        base_state()
        | intents: %{"active" => active, "done" => completed},
          active_intents_by_conflict_key: %{{:file, "f1"} => "active"}
      }

      active_list = MetadataStateMachine.list_active_intents(state)
      assert length(active_list) == 1
      assert hd(active_list).id == "active"
    end
  end
end
