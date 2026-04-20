defmodule NeonFS.Core.MetadataStateMachine do
  @moduledoc """
  Ra state machine for cluster-wide metadata storage (v8).

  Stores cluster-critical metadata with strong consistency via Raft consensus:
  - Service registry (nodes and their service types)
  - Volume definitions
  - Segment assignments (consistent hash ring -> replica sets)
  - Active write intents (cross-segment atomicity and concurrent writer detection)
  - Encryption keys (per-volume, wrapped with cluster master key)
  - Volume ACLs (UID/GID-based permission entries)
  - General key-value data (volume-level metadata)

  Also retains chunk, file, and stripe metadata for backward compatibility.
  These will move to quorum-based BlobStore storage (Tier 2/3) in Phase 5
  tasks 0086-0089.
  """

  @behaviour :ra_machine

  alias NeonFS.Client.ServiceInfo
  alias NeonFS.Core.Intent

  @type wrapped_key_entry :: %{
          wrapped_key: binary(),
          created_at: DateTime.t(),
          deprecated_at: DateTime.t() | nil
        }

  @type command ::
          {:put, key :: term(), value :: term()}
          | {:delete, key :: term()}
          | {:put_chunk, chunk_meta :: map()}
          | {:update_chunk_locations, hash :: binary(), locations :: [map()]}
          | {:delete_chunk, hash :: binary()}
          | {:commit_chunk, hash :: binary()}
          | {:add_write_ref, hash :: binary(), write_id :: String.t()}
          | {:remove_write_ref, hash :: binary(), write_id :: String.t()}
          | {:put_volume, volume_data :: map()}
          | {:delete_volume, volume_id :: binary()}
          | {:put_file, file_meta :: map()}
          | {:update_file, file_id :: binary(), updates :: map()}
          | {:delete_file, file_id :: binary()}
          | {:register_service, service_info :: map()}
          | {:deregister_service, node()}
          | {:deregister_service, node(), atom()}
          | {:update_service_status, node(), atom()}
          | {:update_service_metrics, node(), map()}
          | {:put_stripe, stripe_data :: map()}
          | {:update_stripe, stripe_id :: binary(), updates :: map()}
          | {:delete_stripe, stripe_id :: binary()}
          | {:assign_segment, segment_id :: term(), replica_set :: [node()]}
          | {:bulk_update_assignments, assignments :: %{term() => [node()]}}
          | {:try_acquire_intent, intent :: Intent.t()}
          | {:complete_intent, intent_id :: binary()}
          | {:fail_intent, intent_id :: binary(), reason :: term()}
          | {:extend_intent, intent_id :: binary(), additional_seconds :: pos_integer()}
          | :cleanup_expired_intents
          | {:put_encryption_key, volume_id :: binary(), version :: pos_integer(),
             wrapped_entry :: wrapped_key_entry()}
          | {:delete_encryption_key, volume_id :: binary(), version :: pos_integer()}
          | {:set_current_key_version, volume_id :: binary(), version :: pos_integer()}
          | {:put_volume_acl, volume_id :: binary(), acl_data :: map()}
          | {:update_volume_acl, volume_id :: binary(), updates :: map()}
          | {:put_s3_credential, cred_data :: map()}
          | {:delete_s3_credential, access_key_id :: String.t()}
          | {:put_escalation, escalation_data :: map()}
          | {:delete_escalation, escalation_id :: String.t()}

  @type segment_assignment :: %{
          replica_set: [node()],
          version: non_neg_integer()
        }

  @type state :: %{
          data: %{optional(term()) => term()},
          chunks: %{optional(binary()) => map()},
          files: %{optional(binary()) => map()},
          services: %{optional({node(), atom()}) => map()},
          volumes: %{optional(binary()) => map()},
          stripes: %{optional(binary()) => map()},
          segment_assignments: %{optional(term()) => segment_assignment()},
          intents: %{optional(binary()) => Intent.t()},
          active_intents_by_conflict_key: %{optional(term()) => binary()},
          encryption_keys: %{
            optional(binary()) => %{optional(pos_integer()) => wrapped_key_entry()}
          },
          volume_acls: %{optional(binary()) => map()},
          s3_credentials: %{optional(String.t()) => map()},
          escalations: %{optional(String.t()) => map()},
          version: non_neg_integer()
        }

  # Public query functions

  @doc """
  Returns all segment assignments from the given state.
  """
  @spec get_segment_assignments(state()) :: %{term() => segment_assignment()}
  def get_segment_assignments(state), do: state.segment_assignments

  @doc """
  Returns the intent with the given ID from the state, or nil.
  """
  @spec get_intent(state(), binary()) :: Intent.t() | nil
  def get_intent(state, intent_id), do: Map.get(state.intents, intent_id)

  @doc """
  Returns all active (pending) intents from the state.
  """
  @spec list_active_intents(state()) :: [Intent.t()]
  def list_active_intents(state) do
    state.active_intents_by_conflict_key
    |> Map.values()
    |> Enum.map(&Map.get(state.intents, &1))
    |> Enum.reject(&is_nil/1)
  end

  @doc """
  Returns all pending intents that have exceeded their TTL.
  """
  @spec list_expired_intents(state()) :: [Intent.t()]
  def list_expired_intents(state) do
    now = DateTime.utc_now()

    state.intents
    |> Map.values()
    |> Enum.filter(fn intent ->
      intent.state == :pending and Intent.expired_at?(intent, now)
    end)
  end

  @doc """
  Returns the encryption keys for the given volume, or nil if none exist.
  """
  @spec get_encryption_keys(state(), binary()) :: %{pos_integer() => wrapped_key_entry()} | nil
  def get_encryption_keys(state, volume_id), do: Map.get(state.encryption_keys, volume_id)

  @doc """
  Returns the volume ACL for the given volume, or nil if none exist.
  """
  @spec get_volume_acl(state(), binary()) :: map() | nil
  def get_volume_acl(state, volume_id), do: Map.get(state.volume_acls, volume_id)

  # Ra machine callbacks

  @doc """
  Initialise the state machine with a clean v9 state.
  """
  @impl :ra_machine
  def init(_config) do
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
      version: 0
    }
  end

  @impl :ra_machine
  def apply(_meta, {:put, key, value}, state) do
    new_data = Map.put(state.data, key, value)
    new_state = %{state | data: new_data, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :put],
      %{version: new_state.version},
      %{key: key}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:delete, key}, state) do
    new_data = Map.delete(state.data, key)
    new_state = %{state | data: new_data, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :delete],
      %{version: new_state.version},
      %{key: key}
    )

    {new_state, :ok, []}
  end

  # Machine version upgrades

  def apply(_meta, {:machine_version, 1, 2}, state) do
    require Logger
    Logger.info("Ra machine version upgrade", from: 1, to: 2, change: "adding services registry")

    new_state = Map.put_new(state, :services, %{})
    {new_state, :ok, []}
  end

  def apply(_meta, {:machine_version, 2, 3}, state) do
    require Logger

    Logger.info("Ra machine version upgrade",
      from: 2,
      to: 3,
      change: "volume tiering/caching/io_weight"
    )

    volumes = Map.get(state, :volumes, %{})

    migrated_volumes =
      Map.new(volumes, fn {id, vol} ->
        migrated =
          vol
          |> migrate_volume_tiering()
          |> migrate_volume_caching()
          |> migrate_volume_io_weight()

        {id, migrated}
      end)

    new_state = %{state | volumes: migrated_volumes}
    {new_state, :ok, []}
  end

  def apply(_meta, {:machine_version, 3, 4}, state) do
    require Logger
    Logger.info("Ra machine version upgrade", from: 3, to: 4, change: "adding stripes registry")

    new_state = Map.put_new(state, :stripes, %{})
    {new_state, :ok, []}
  end

  def apply(_meta, {:machine_version, 4, 5}, state) do
    require Logger

    Logger.info("Ra machine version upgrade",
      from: 4,
      to: 5,
      change: "segment assignments + intents"
    )

    new_state =
      state
      |> Map.put_new(:segment_assignments, %{})
      |> Map.put_new(:intents, %{})
      |> Map.put_new(:active_intents_by_conflict_key, %{})

    {new_state, :ok, []}
  end

  def apply(_meta, {:machine_version, 5, 6}, state) do
    require Logger

    Logger.info("Ra machine version upgrade",
      from: 5,
      to: 6,
      change: "encryption keys + volume ACLs"
    )

    new_state =
      state
      |> Map.put_new(:encryption_keys, %{})
      |> Map.put_new(:volume_acls, %{})

    {new_state, :ok, []}
  end

  def apply(_meta, {:machine_version, 6, 7}, state) do
    require Logger

    Logger.info("Ra machine version upgrade",
      from: 6,
      to: 7,
      change: "remove rotation state from volumes"
    )

    volumes =
      Map.new(state.volumes, fn {id, vol} ->
        encryption = Map.get(vol, :encryption, %{})
        cleaned_encryption = Map.delete(encryption, :rotation)
        {id, Map.put(vol, :encryption, cleaned_encryption)}
      end)

    new_state = %{state | volumes: volumes}
    {new_state, :ok, []}
  end

  def apply(_meta, {:machine_version, 7, 8}, state) do
    require Logger

    Logger.info("Ra machine version upgrade",
      from: 7,
      to: 8,
      change: "multi-service registry keys"
    )

    new_state = %{state | services: migrate_services(state.services)}
    {new_state, :ok, []}
  end

  def apply(_meta, {:machine_version, from_version, to_version}, state) do
    require Logger
    Logger.info("Ra machine version upgrade", from: from_version, to: to_version)

    {state, :ok, []}
  end

  # Chunk commands (legacy — will move to quorum BlobStore in tasks 0086-0089)

  def apply(_meta, {:put_chunk, chunk_meta}, state) do
    hash = chunk_meta.hash
    new_chunks = Map.put(state.chunks, hash, chunk_meta)
    new_state = %{state | chunks: new_chunks, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :put_chunk],
      %{version: new_state.version},
      %{hash: hash}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:update_chunk_locations, hash, locations}, state) do
    case Map.get(state.chunks, hash) do
      nil ->
        {state, {:error, :not_found}, []}

      chunk_meta ->
        updated_meta = Map.put(chunk_meta, :locations, locations)
        new_chunks = Map.put(state.chunks, hash, updated_meta)
        new_state = %{state | chunks: new_chunks, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :update_chunk_locations],
          %{version: new_state.version},
          %{hash: hash, location_count: length(locations)}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:delete_chunk, hash}, state) do
    new_chunks = Map.delete(state.chunks, hash)
    new_state = %{state | chunks: new_chunks, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :delete_chunk],
      %{version: new_state.version},
      %{hash: hash}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:commit_chunk, hash}, state) do
    case Map.get(state.chunks, hash) do
      nil ->
        {state, {:error, :not_found}, []}

      chunk_meta ->
        active_write_refs = Map.get(chunk_meta, :active_write_refs, MapSet.new())

        if MapSet.size(active_write_refs) == 0 do
          updated_meta = Map.put(chunk_meta, :commit_state, :committed)
          new_chunks = Map.put(state.chunks, hash, updated_meta)
          new_state = %{state | chunks: new_chunks, version: state.version + 1}

          :telemetry.execute(
            [:neonfs, :ra, :command, :commit_chunk],
            %{version: new_state.version},
            %{hash: hash}
          )

          {new_state, :ok, []}
        else
          {state, {:error, :has_active_writes}, []}
        end
    end
  end

  def apply(_meta, {:add_write_ref, hash, write_id}, state) do
    case Map.get(state.chunks, hash) do
      nil ->
        {state, {:error, :not_found}, []}

      chunk_meta ->
        active_write_refs = Map.get(chunk_meta, :active_write_refs, MapSet.new())
        updated_refs = MapSet.put(active_write_refs, write_id)
        updated_meta = Map.put(chunk_meta, :active_write_refs, updated_refs)
        new_chunks = Map.put(state.chunks, hash, updated_meta)
        new_state = %{state | chunks: new_chunks, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :add_write_ref],
          %{version: new_state.version},
          %{hash: hash, write_id: write_id}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:remove_write_ref, hash, write_id}, state) do
    case Map.get(state.chunks, hash) do
      nil ->
        {state, {:error, :not_found}, []}

      chunk_meta ->
        active_write_refs = Map.get(chunk_meta, :active_write_refs, MapSet.new())
        updated_refs = MapSet.delete(active_write_refs, write_id)
        updated_meta = Map.put(chunk_meta, :active_write_refs, updated_refs)
        new_chunks = Map.put(state.chunks, hash, updated_meta)
        new_state = %{state | chunks: new_chunks, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :remove_write_ref],
          %{version: new_state.version},
          %{hash: hash, write_id: write_id}
        )

        {new_state, :ok, []}
    end
  end

  # Volume commands

  def apply(_meta, {:put_volume, volume_data}, state) do
    id = volume_data.id
    new_volumes = Map.put(state.volumes, id, volume_data)
    new_state = %{state | volumes: new_volumes, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :put_volume],
      %{version: new_state.version},
      %{id: id, name: volume_data.name}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:delete_volume, volume_id}, state) do
    new_state = %{
      state
      | volumes: Map.delete(state.volumes, volume_id),
        encryption_keys: Map.delete(state.encryption_keys, volume_id),
        volume_acls: Map.delete(state.volume_acls, volume_id),
        version: state.version + 1
    }

    :telemetry.execute(
      [:neonfs, :ra, :command, :delete_volume],
      %{version: new_state.version},
      %{id: volume_id}
    )

    {new_state, :ok, []}
  end

  # File metadata commands (legacy — will move to quorum BlobStore in tasks 0086-0089)

  def apply(_meta, {:put_file, file_meta}, state) do
    id = file_meta.id
    new_files = Map.put(state.files, id, file_meta)
    new_state = %{state | files: new_files, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :put_file],
      %{version: new_state.version},
      %{id: id, path: file_meta[:path]}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:update_file, file_id, updates}, state) do
    case Map.get(state.files, file_id) do
      nil ->
        {state, {:error, :not_found}, []}

      file_meta ->
        updated_meta = Map.merge(file_meta, updates)
        new_files = Map.put(state.files, file_id, updated_meta)
        new_state = %{state | files: new_files, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :update_file],
          %{version: new_state.version},
          %{id: file_id}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:delete_file, file_id}, state) do
    new_files = Map.delete(state.files, file_id)
    new_state = %{state | files: new_files, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :delete_file],
      %{version: new_state.version},
      %{id: file_id}
    )

    {new_state, :ok, []}
  end

  # Service registry commands

  def apply(_meta, {:register_service, service_info}, state) do
    key = service_key(service_info)
    new_services = Map.put(state.services, key, service_info)
    new_state = %{state | services: new_services, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :register_service],
      %{version: new_state.version},
      %{node: service_info.node, type: service_info.type}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:deregister_service, node}, state) do
    new_services =
      Enum.reject(state.services, fn service_entry ->
        match_service_node?(service_entry, node)
      end)
      |> Map.new()

    new_state = %{state | services: new_services, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :deregister_service],
      %{version: new_state.version},
      %{node: node}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:deregister_service, node, type}, state) do
    new_services = Map.delete(state.services, {node, type})
    new_state = %{state | services: new_services, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :deregister_service],
      %{version: new_state.version},
      %{node: node, type: type}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:update_service_status, node, status}, state) do
    case matching_service_keys(state.services, node) do
      [] ->
        {state, {:error, :not_found}, []}

      _keys ->
        new_services =
          update_matching_services(state.services, node, &Map.put(&1, :status, status))

        new_state = %{state | services: new_services, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :update_service_status],
          %{version: new_state.version},
          %{node: node, status: status}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:update_service_metrics, node, metrics}, state) do
    case matching_service_keys(state.services, node) do
      [] ->
        {state, {:error, :not_found}, []}

      _keys ->
        new_services =
          update_matching_services(state.services, node, &Map.put(&1, :metrics, metrics))

        new_state = %{state | services: new_services, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :update_service_metrics],
          %{version: new_state.version},
          %{node: node}
        )

        {new_state, :ok, []}
    end
  end

  # Stripe commands (legacy — will move to quorum BlobStore in tasks 0086-0089)

  def apply(_meta, {:put_stripe, stripe_data}, state) do
    id = stripe_data.id
    new_stripes = Map.put(state.stripes, id, stripe_data)
    new_state = %{state | stripes: new_stripes, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :put_stripe],
      %{version: new_state.version},
      %{id: id}
    )

    {new_state, {:ok, id}, []}
  end

  def apply(_meta, {:update_stripe, stripe_id, updates}, state) do
    case Map.get(state.stripes, stripe_id) do
      nil ->
        {state, {:error, :not_found}, []}

      stripe_data ->
        updated = Map.merge(stripe_data, updates)
        new_stripes = Map.put(state.stripes, stripe_id, updated)
        new_state = %{state | stripes: new_stripes, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :update_stripe],
          %{version: new_state.version},
          %{id: stripe_id}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:delete_stripe, stripe_id}, state) do
    new_stripes = Map.delete(state.stripes, stripe_id)
    new_state = %{state | stripes: new_stripes, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :delete_stripe],
      %{version: new_state.version},
      %{id: stripe_id}
    )

    {new_state, :ok, []}
  end

  # Encryption key commands (new in v6)

  def apply(_meta, {:put_encryption_key, volume_id, key_version, wrapped_entry}, state) do
    volume_keys = Map.get(state.encryption_keys, volume_id, %{})
    updated_keys = Map.put(volume_keys, key_version, wrapped_entry)
    new_encryption_keys = Map.put(state.encryption_keys, volume_id, updated_keys)
    new_state = %{state | encryption_keys: new_encryption_keys, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :put_encryption_key],
      %{version: new_state.version},
      %{volume_id: volume_id, key_version: key_version}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:delete_encryption_key, volume_id, key_version}, state) do
    case Map.get(state.encryption_keys, volume_id) do
      nil ->
        {state, {:error, :not_found}, []}

      volume_keys ->
        updated_keys = Map.delete(volume_keys, key_version)

        new_encryption_keys =
          if map_size(updated_keys) == 0 do
            Map.delete(state.encryption_keys, volume_id)
          else
            Map.put(state.encryption_keys, volume_id, updated_keys)
          end

        new_state = %{state | encryption_keys: new_encryption_keys, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :delete_encryption_key],
          %{version: new_state.version},
          %{volume_id: volume_id, key_version: key_version}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:set_current_key_version, volume_id, key_version}, state) do
    case Map.get(state.volumes, volume_id) do
      nil ->
        {state, {:error, :not_found}, []}

      volume_data ->
        encryption = Map.get(volume_data, :encryption, %{})
        updated_encryption = Map.put(encryption, :current_key_version, key_version)
        updated_volume = Map.put(volume_data, :encryption, updated_encryption)
        new_volumes = Map.put(state.volumes, volume_id, updated_volume)
        new_state = %{state | volumes: new_volumes, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :set_current_key_version],
          %{version: new_state.version},
          %{volume_id: volume_id, key_version: key_version}
        )

        {new_state, :ok, []}
    end
  end

  # Volume ACL commands (new in v6)

  def apply(_meta, {:put_volume_acl, volume_id, acl_data}, state) do
    new_acls = Map.put(state.volume_acls, volume_id, acl_data)
    new_state = %{state | volume_acls: new_acls, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :put_volume_acl],
      %{version: new_state.version},
      %{volume_id: volume_id}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:update_volume_acl, volume_id, updates}, state) do
    case Map.get(state.volume_acls, volume_id) do
      nil ->
        {state, {:error, :not_found}, []}

      existing_acl ->
        updated_acl = Map.merge(existing_acl, updates)
        new_acls = Map.put(state.volume_acls, volume_id, updated_acl)
        new_state = %{state | volume_acls: new_acls, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :update_volume_acl],
          %{version: new_state.version},
          %{volume_id: volume_id}
        )

        {new_state, :ok, []}
    end
  end

  # S3 credential commands (new in v9)

  def apply(_meta, {:put_s3_credential, cred_data}, state) do
    access_key_id = cred_data.access_key_id
    state = ensure_s3_credentials(state)
    new_creds = Map.put(state.s3_credentials, access_key_id, cred_data)
    new_state = %{state | s3_credentials: new_creds, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :put_s3_credential],
      %{version: new_state.version},
      %{access_key_id: access_key_id}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:delete_s3_credential, access_key_id}, state) do
    state = ensure_s3_credentials(state)
    new_creds = Map.delete(state.s3_credentials, access_key_id)
    new_state = %{state | s3_credentials: new_creds, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :delete_s3_credential],
      %{version: new_state.version},
      %{access_key_id: access_key_id}
    )

    {new_state, :ok, []}
  end

  # Escalation commands (decision escalation system, issue #245)

  def apply(_meta, {:put_escalation, escalation_data}, state) do
    state = ensure_escalations(state)
    id = escalation_data.id
    new_escalations = Map.put(state.escalations, id, escalation_data)
    new_state = %{state | escalations: new_escalations, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :put_escalation],
      %{version: new_state.version},
      %{id: id, category: escalation_data[:category], status: escalation_data[:status]}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:delete_escalation, id}, state) do
    state = ensure_escalations(state)
    new_escalations = Map.delete(state.escalations, id)
    new_state = %{state | escalations: new_escalations, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :delete_escalation],
      %{version: new_state.version},
      %{id: id}
    )

    {new_state, :ok, []}
  end

  # Segment assignment commands (new in v5)

  def apply(_meta, {:assign_segment, segment_id, replica_set}, state) do
    existing = Map.get(state.segment_assignments, segment_id)
    new_version = if existing, do: existing.version + 1, else: 1

    assignment = %{replica_set: replica_set, version: new_version}
    new_assignments = Map.put(state.segment_assignments, segment_id, assignment)
    new_state = %{state | segment_assignments: new_assignments, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :assign_segment],
      %{version: new_state.version},
      %{segment_id: segment_id, replica_count: length(replica_set)}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:bulk_update_assignments, assignments}, state) do
    new_assignments =
      Enum.reduce(assignments, state.segment_assignments, fn {segment_id, replica_set}, acc ->
        existing = Map.get(acc, segment_id)
        new_version = if existing, do: existing.version + 1, else: 1
        Map.put(acc, segment_id, %{replica_set: replica_set, version: new_version})
      end)

    new_state = %{state | segment_assignments: new_assignments, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :bulk_update_assignments],
      %{version: new_state.version},
      %{segment_count: map_size(assignments)}
    )

    {new_state, :ok, []}
  end

  # Intent management commands (new in v5)

  def apply(_meta, {:try_acquire_intent, %Intent{} = intent}, state) do
    now = DateTime.utc_now()

    case Map.get(state.active_intents_by_conflict_key, intent.conflict_key) do
      nil ->
        new_state =
          state
          |> put_in_intents(intent.id, intent)
          |> put_in_conflict_key(intent.conflict_key, intent.id)
          |> increment_version()

        :telemetry.execute(
          [:neonfs, :ra, :command, :try_acquire_intent],
          %{version: new_state.version},
          %{intent_id: intent.id, operation: intent.operation, result: :acquired}
        )

        {new_state, {:ok, :acquired}, []}

      existing_id ->
        existing = Map.get(state.intents, existing_id)

        if Intent.expired_at?(existing, now) do
          new_state =
            state
            |> put_in_intents(existing_id, %{existing | state: :expired})
            |> put_in_intents(intent.id, intent)
            |> put_in_conflict_key(intent.conflict_key, intent.id)
            |> increment_version()

          :telemetry.execute(
            [:neonfs, :ra, :command, :try_acquire_intent],
            %{version: new_state.version},
            %{intent_id: intent.id, operation: intent.operation, result: :acquired_expired}
          )

          {new_state, {:ok, :acquired}, []}
        else
          :telemetry.execute(
            [:neonfs, :ra, :command, :try_acquire_intent],
            %{version: state.version},
            %{intent_id: intent.id, operation: intent.operation, result: :conflict}
          )

          {state, {:ok, :conflict, existing}, []}
        end
    end
  end

  def apply(_meta, {:complete_intent, intent_id}, state) do
    case Map.get(state.intents, intent_id) do
      nil ->
        {state, {:error, :not_found}, []}

      intent ->
        completed = %{intent | state: :completed, completed_at: DateTime.utc_now()}

        new_state =
          state
          |> put_in_intents(intent_id, completed)
          |> remove_conflict_key(intent.conflict_key)
          |> increment_version()

        :telemetry.execute(
          [:neonfs, :ra, :command, :complete_intent],
          %{version: new_state.version},
          %{intent_id: intent_id}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:fail_intent, intent_id, reason}, state) do
    case Map.get(state.intents, intent_id) do
      nil ->
        {state, {:error, :not_found}, []}

      intent ->
        failed = %{intent | state: :failed, completed_at: DateTime.utc_now(), error: reason}

        new_state =
          state
          |> put_in_intents(intent_id, failed)
          |> remove_conflict_key(intent.conflict_key)
          |> increment_version()

        :telemetry.execute(
          [:neonfs, :ra, :command, :fail_intent],
          %{version: new_state.version},
          %{intent_id: intent_id, reason: reason}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:extend_intent, intent_id, additional_seconds}, state) do
    case Map.get(state.intents, intent_id) do
      nil ->
        {state, {:error, :not_found}, []}

      %Intent{state: :pending} = intent ->
        extended = %{
          intent
          | expires_at: DateTime.add(intent.expires_at, additional_seconds, :second)
        }

        new_state =
          state
          |> put_in_intents(intent_id, extended)
          |> increment_version()

        :telemetry.execute(
          [:neonfs, :ra, :command, :extend_intent],
          %{version: new_state.version},
          %{intent_id: intent_id, additional_seconds: additional_seconds}
        )

        {new_state, :ok, []}

      _non_pending ->
        {state, {:error, :not_pending}, []}
    end
  end

  def apply(_meta, :cleanup_expired_intents, state) do
    now = DateTime.utc_now()

    expired_ids =
      state.intents
      |> Enum.filter(fn {_id, intent} ->
        intent.state == :pending and Intent.expired_at?(intent, now)
      end)
      |> Enum.map(fn {id, _intent} -> id end)

    new_state =
      Enum.reduce(expired_ids, state, fn intent_id, acc ->
        intent = Map.get(acc.intents, intent_id)
        expired_intent = %{intent | state: :expired}

        acc
        |> put_in_intents(intent_id, expired_intent)
        |> remove_conflict_key(intent.conflict_key)
      end)

    new_state = increment_version(new_state)

    :telemetry.execute(
      [:neonfs, :ra, :command, :cleanup_expired_intents],
      %{version: new_state.version, cleaned_count: length(expired_ids)},
      %{}
    )

    {new_state, {:ok, length(expired_ids)}, []}
  end

  # Catch-all for truly unknown commands (prevents crashes)
  def apply(_meta, command, state) do
    require Logger

    command_type =
      case command do
        {type, _} -> type
        {type, _, _} -> type
        {type, _, _, _} -> type
        other -> other
      end

    Logger.debug("Ignoring unhandled Ra command", command_type: inspect(command_type))

    {state, {:error, :unknown_command}, []}
  end

  @doc """
  Handle state transitions. Called when the Ra server enters a new state.
  """
  @impl :ra_machine
  def state_enter(_state_name, _state) do
    []
  end

  @doc """
  Called when a snapshot is installed (e.g., during cluster catch-up).
  """
  @impl :ra_machine
  def snapshot_installed(_meta, _state, _old_meta, _old_state) do
    []
  end

  @doc """
  Return the state machine version for upgrade/migration support.
  """
  @impl :ra_machine
  def version, do: 8

  @doc """
  Return the module to handle a specific state machine version.
  """
  @impl :ra_machine
  def which_module(_version), do: __MODULE__

  # Private helpers

  defp put_in_intents(state, intent_id, intent) do
    %{state | intents: Map.put(state.intents, intent_id, intent)}
  end

  defp put_in_conflict_key(state, conflict_key, intent_id) do
    %{
      state
      | active_intents_by_conflict_key:
          Map.put(state.active_intents_by_conflict_key, conflict_key, intent_id)
    }
  end

  defp remove_conflict_key(state, conflict_key) do
    %{
      state
      | active_intents_by_conflict_key:
          Map.delete(state.active_intents_by_conflict_key, conflict_key)
    }
  end

  defp increment_version(state) do
    %{state | version: state.version + 1}
  end

  defp matching_service_keys(services, node) do
    services
    |> Enum.filter(&match_service_node?(&1, node))
    |> Enum.map(fn service_entry -> service_entry_key(service_entry) end)
  end

  defp migrate_services(services) do
    Map.new(services, fn {_key, info} ->
      migrated = ServiceInfo.from_map(info)
      {service_key(migrated), ServiceInfo.to_map(migrated)}
    end)
  end

  defp service_key(service_info) do
    {service_info.node, service_info.type}
  end

  defp update_matching_services(services, node, fun) do
    Map.new(services, fn service_entry ->
      key = service_entry_key(service_entry)
      info = service_entry_info(service_entry)

      if elem(key, 0) == node do
        {key, fun.(info)}
      else
        {key, info}
      end
    end)
  end

  defp match_service_node?(service_entry, node) do
    service_entry
    |> service_entry_key()
    |> elem(0)
    |> Kernel.==(node)
  end

  defp service_entry_info({_key, info}), do: info

  defp service_entry_key({{node, type}, _info}), do: {node, type}

  defp service_entry_key({_node, info}), do: service_key(ServiceInfo.from_map(info))

  # Migration helpers for version 2 -> 3

  defp migrate_volume_tiering(vol) do
    case Map.get(vol, :tiering) do
      %{initial_tier: _} ->
        vol

      _ ->
        initial_tier = Map.get(vol, :initial_tier, :hot)

        vol
        |> Map.put(:tiering, %{
          initial_tier: initial_tier,
          promotion_threshold: 10,
          demotion_delay: 86_400
        })
        |> Map.delete(:initial_tier)
    end
  end

  defp migrate_volume_caching(vol) do
    Map.put_new(vol, :caching, %{
      transformed_chunks: true,
      reconstructed_stripes: true,
      remote_chunks: true
    })
  end

  defp migrate_volume_io_weight(vol) do
    Map.put_new(vol, :io_weight, 100)
  end

  # Migration helper for v8 -> v9: ensure s3_credentials key exists
  defp ensure_s3_credentials(%{s3_credentials: _} = state), do: state
  defp ensure_s3_credentials(state), do: Map.put(state, :s3_credentials, %{})

  # Defensive init for pre-escalation-era snapshots
  defp ensure_escalations(%{escalations: _} = state), do: state
  defp ensure_escalations(state), do: Map.put(state, :escalations, %{})
end
