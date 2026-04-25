defmodule NeonFS.Core.MetadataStateMachine do
  @moduledoc """
  Ra state machine for cluster-wide metadata storage (v11).

  Stores cluster-critical metadata with strong consistency via Raft consensus:
  - Service registry (nodes and their service types)
  - Volume definitions
  - Segment assignments (consistent hash ring -> replica sets)
  - Active write intents (cross-segment atomicity and concurrent writer detection)
  - Encryption keys (per-volume, wrapped with cluster master key)
  - Volume ACLs (UID/GID-based permission entries)
  - General key-value data (volume-level metadata)
  - Generic cluster-wide KV store (`NeonFS.Core.KVStore`, v10). Used by
    orchestration-layer packages (e.g. `neonfs_iam`) that need durable
    Ra-replicated storage without depending on `neonfs_core` directly.

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
          | {:delete_volume_acl, volume_id :: binary()}
          | {:grant_volume_acl_entry, volume_id :: binary(), principal :: term(),
             permissions :: [atom()]}
          | {:revoke_volume_acl_entry, volume_id :: binary(), principal :: term()}
          | {:put_s3_credential, cred_data :: map()}
          | {:delete_s3_credential, access_key_id :: String.t()}
          | {:put_escalation, escalation_data :: map()}
          | {:delete_escalation, escalation_id :: String.t()}
          | {:kv_put, key :: binary(), value :: term()}
          | {:kv_delete, key :: binary()}
          | {:claim_namespace_path, path :: String.t(), scope :: namespace_scope(),
             holder :: term()}
          | {:claim_namespace_subtree, path :: String.t(), scope :: namespace_scope(),
             holder :: term()}
          | {:release_namespace_claim, claim_id :: String.t()}
          | {:release_namespace_claims_for_holder, holder :: term()}

  @type segment_assignment :: %{
          replica_set: [node()],
          version: non_neg_integer()
        }

  @typedoc """
  Lock scope for namespace claims (RFC 4918 collection-lock semantics).
  An `:exclusive` claim conflicts with any other claim covering the
  same path / subtree; `:shared` claims coexist with each other but
  still conflict with `:exclusive` claims.
  """
  @type namespace_scope :: :exclusive | :shared

  @typedoc """
  A namespace claim — either a `:path` claim covering a single path or
  a `:subtree` claim covering a path and all its descendants.
  Holders are opaque terms (typically a pid in production; arbitrary
  test fixtures otherwise) — the state machine treats them as black
  boxes.
  """
  @type namespace_claim :: %{
          path: String.t(),
          scope: namespace_scope(),
          type: :path | :subtree,
          holder: term()
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
          kv: %{optional(binary()) => term()},
          namespace_claims: %{optional(String.t()) => namespace_claim()},
          namespace_claim_seq: non_neg_integer(),
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

  @doc """
  Returns the entire generic KV table as a `key => value` map.
  Returns an empty map when the state was produced by a pre-v10
  machine. Used by `NeonFS.Core.KVStore` to serve reads through
  `:ra.local_query`.
  """
  @spec get_kv(state()) :: %{optional(binary()) => term()}
  def get_kv(state), do: Map.get(state, :kv, %{})

  @doc """
  Returns the escalation with the given ID, or nil.
  """
  @spec get_escalation(state(), String.t()) :: map() | nil
  def get_escalation(state, id) do
    state
    |> Map.get(:escalations, %{})
    |> Map.get(id)
  end

  @doc """
  Returns every escalation as an `id => escalation` map.
  """
  @spec get_escalations(state()) :: %{optional(String.t()) => map()}
  def get_escalations(state), do: Map.get(state, :escalations, %{})

  @doc """
  Returns every registered service as a `{node, type} => service_map` map.
  """
  @spec get_services(state()) :: %{optional({node(), atom()}) => map()}
  def get_services(state), do: Map.get(state, :services, %{})

  @doc """
  Returns the service registered at the given `{node, type}`, or nil.
  """
  @spec get_service(state(), node(), atom()) :: map() | nil
  def get_service(state, node, type) do
    state
    |> Map.get(:services, %{})
    |> Map.get({node, type})
  end

  @doc """
  Returns the S3 credential for the given access key ID, or nil.
  """
  @spec get_s3_credential(state(), String.t()) :: map() | nil
  def get_s3_credential(state, access_key_id) do
    state
    |> Map.get(:s3_credentials, %{})
    |> Map.get(access_key_id)
  end

  @doc """
  Returns every S3 credential as an `access_key_id => cred` map.
  """
  @spec get_s3_credentials(state()) :: %{optional(String.t()) => map()}
  def get_s3_credentials(state), do: Map.get(state, :s3_credentials, %{})

  @doc """
  Returns the namespace claim with the given id, or `nil` when none
  exists. Used by `NeonFS.Core.NamespaceCoordinator` for release
  validation and for operator probes.
  """
  @spec get_namespace_claim(state(), String.t()) :: namespace_claim() | nil
  def get_namespace_claim(state, claim_id) do
    state
    |> Map.get(:namespace_claims, %{})
    |> Map.get(claim_id)
  end

  @doc """
  Returns every namespace claim whose `:path` starts with `prefix`.
  Pass `""` to list every claim. Sorted by claim id (so iteration
  order is stable across calls).
  """
  @spec list_namespace_claims(state(), String.t()) :: [{String.t(), namespace_claim()}]
  def list_namespace_claims(state, prefix \\ "") when is_binary(prefix) do
    state
    |> Map.get(:namespace_claims, %{})
    |> Enum.filter(fn {_id, %{path: p}} -> String.starts_with?(p, prefix) end)
    |> Enum.sort_by(fn {id, _claim} -> id end)
  end

  @doc """
  Returns every namespace claim held by `holder`. The
  `NamespaceCoordinator` GenServer uses this on `:DOWN` to verify
  which claims it owns before sending the bulk release command.
  """
  @spec list_namespace_claims_for_holder(state(), term()) :: [{String.t(), namespace_claim()}]
  def list_namespace_claims_for_holder(state, holder) do
    state
    |> Map.get(:namespace_claims, %{})
    |> Enum.filter(fn {_id, %{holder: h}} -> h == holder end)
    |> Enum.sort_by(fn {id, _claim} -> id end)
  end

  # Ra machine callbacks

  @doc """
  Initialise the state machine with a clean v10 state.
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
      kv: %{},
      namespace_claims: %{},
      namespace_claim_seq: 0,
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

  def apply(_meta, {:machine_version, 8, 9}, state) do
    require Logger

    Logger.info("Ra machine version upgrade",
      from: 8,
      to: 9,
      change: "IAM tables (superseded by v10 generic kv)"
    )

    new_state =
      state
      |> Map.put_new(:iam_users, %{})
      |> Map.put_new(:iam_groups, %{})
      |> Map.put_new(:iam_policies, %{})
      |> Map.put_new(:iam_identity_mappings, %{})

    {new_state, :ok, []}
  end

  def apply(_meta, {:machine_version, 9, 10}, state) do
    require Logger

    Logger.info("Ra machine version upgrade",
      from: 9,
      to: 10,
      change: "drop IAM tables, add generic :kv"
    )

    new_state =
      state
      |> Map.delete(:iam_users)
      |> Map.delete(:iam_groups)
      |> Map.delete(:iam_policies)
      |> Map.delete(:iam_identity_mappings)
      |> Map.put_new(:kv, %{})

    {new_state, :ok, []}
  end

  def apply(_meta, {:machine_version, 10, 11}, state) do
    require Logger

    Logger.info("Ra machine version upgrade",
      from: 10,
      to: 11,
      change: "add delete/grant/revoke volume_acl commands"
    )

    {state, :ok, []}
  end

  def apply(_meta, {:machine_version, 11, 12}, state) do
    require Logger

    Logger.info("Ra machine version upgrade",
      from: 11,
      to: 12,
      change: "add namespace coordinator (claims by path / subtree)"
    )

    new_state =
      state
      |> Map.put_new(:namespace_claims, %{})
      |> Map.put_new(:namespace_claim_seq, 0)

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

  def apply(_meta, {:delete_volume_acl, volume_id}, state) do
    new_acls = Map.delete(state.volume_acls, volume_id)
    new_state = %{state | volume_acls: new_acls, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :delete_volume_acl],
      %{version: new_state.version},
      %{volume_id: volume_id}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:grant_volume_acl_entry, volume_id, principal, permissions}, state) do
    case Map.get(state.volume_acls, volume_id) do
      nil ->
        {state, {:error, :not_found}, []}

      acl_data ->
        existing_entries = Map.get(acl_data, :entries, [])
        new_entry = %{principal: principal, permissions: permissions}
        updated_entries = upsert_acl_entry(existing_entries, principal, new_entry)
        updated_acl_data = Map.put(acl_data, :entries, updated_entries)
        new_acls = Map.put(state.volume_acls, volume_id, updated_acl_data)
        new_state = %{state | volume_acls: new_acls, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :grant_volume_acl_entry],
          %{version: new_state.version},
          %{volume_id: volume_id}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:revoke_volume_acl_entry, volume_id, principal}, state) do
    case Map.get(state.volume_acls, volume_id) do
      nil ->
        {state, {:error, :not_found}, []}

      acl_data ->
        existing_entries = Map.get(acl_data, :entries, [])
        updated_entries = Enum.reject(existing_entries, &(&1.principal == principal))
        updated_acl_data = Map.put(acl_data, :entries, updated_entries)
        new_acls = Map.put(state.volume_acls, volume_id, updated_acl_data)
        new_state = %{state | volume_acls: new_acls, version: state.version + 1}

        :telemetry.execute(
          [:neonfs, :ra, :command, :revoke_volume_acl_entry],
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

  # Generic KV commands (new in v10) — replace the IAM-specific
  # primitives added in v9 with a single flat binary-keyed map.
  # Consumers that need namespacing pick key prefixes of their own
  # (e.g. `"iam_user:<uuid>"`) and scan with `KVStore.list_prefix/1`.

  def apply(_meta, {:kv_put, key, value}, state) when is_binary(key) do
    state = ensure_kv(state)
    new_state = %{state | kv: Map.put(state.kv, key, value), version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :kv_put],
      %{version: new_state.version},
      %{key: key}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:kv_delete, key}, state) when is_binary(key) do
    state = ensure_kv(state)
    new_state = %{state | kv: Map.delete(state.kv, key), version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :kv_delete],
      %{version: new_state.version},
      %{key: key}
    )

    {new_state, :ok, []}
  end

  # Namespace coordinator commands (new in v12, sub-issue #300 of #226)
  #
  # `:claim_namespace_path` and `:claim_namespace_subtree` allocate a
  # new claim under a sequenced id (`"ns-claim-<n>"`). Conflict
  # detection scans the existing claim map — O(N) but N is bounded by
  # active interface workloads (collection locks, atomic creates,
  # rename windows), and reads happen on followers via local query.

  def apply(_meta, {:claim_namespace_path, path, scope, holder}, state)
      when is_binary(path) and scope in [:exclusive, :shared] do
    apply_namespace_claim(:path, path, scope, holder, state)
  end

  def apply(_meta, {:claim_namespace_subtree, path, scope, holder}, state)
      when is_binary(path) and scope in [:exclusive, :shared] do
    apply_namespace_claim(:subtree, path, scope, holder, state)
  end

  def apply(_meta, {:release_namespace_claim, claim_id}, state) when is_binary(claim_id) do
    state = ensure_namespace(state)
    {released, claims} = Map.pop(state.namespace_claims, claim_id)
    new_state = %{state | namespace_claims: claims, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :release_namespace_claim],
      %{version: new_state.version, released: if(released, do: 1, else: 0)},
      %{claim_id: claim_id}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:release_namespace_claims_for_holder, holder}, state) do
    state = ensure_namespace(state)

    {kept, released_count} =
      Enum.reduce(state.namespace_claims, {%{}, 0}, fn {id, claim}, {acc, count} ->
        if claim.holder == holder do
          {acc, count + 1}
        else
          {Map.put(acc, id, claim), count}
        end
      end)

    new_state = %{state | namespace_claims: kept, version: state.version + 1}

    :telemetry.execute(
      [:neonfs, :ra, :command, :release_namespace_claims_for_holder],
      %{version: new_state.version, released: released_count},
      %{}
    )

    {new_state, {:ok, released_count}, []}
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
  def version, do: 11

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

  # Defensive init for pre-v10 snapshots (before the generic :kv map
  # existed — v9 used per-category iam_users/iam_groups/iam_policies/
  # iam_identity_mappings maps which we don't carry forward since
  # nothing ever wrote production data through them).
  defp ensure_kv(%{kv: _} = state), do: state
  defp ensure_kv(state), do: Map.put(state, :kv, %{})

  # Defensive init for pre-v12 snapshots (before namespace claims).
  defp ensure_namespace(%{namespace_claims: _, namespace_claim_seq: _} = state), do: state

  defp ensure_namespace(state) do
    state
    |> Map.put_new(:namespace_claims, %{})
    |> Map.put_new(:namespace_claim_seq, 0)
  end

  defp apply_namespace_claim(type, path, scope, holder, state) do
    state = ensure_namespace(state)

    case detect_namespace_conflict(type, path, scope, state.namespace_claims) do
      :ok ->
        seq = state.namespace_claim_seq + 1
        claim_id = "ns-claim-" <> Integer.to_string(seq)
        claim = %{path: path, scope: scope, type: type, holder: holder}

        new_state = %{
          state
          | namespace_claims: Map.put(state.namespace_claims, claim_id, claim),
            namespace_claim_seq: seq,
            version: state.version + 1
        }

        :telemetry.execute(
          [:neonfs, :ra, :command, :claim_namespace],
          %{version: new_state.version},
          %{type: type, scope: scope}
        )

        {new_state, {:ok, claim_id}, []}

      {:error, conflict_id} ->
        {state, {:error, :conflict, conflict_id}, []}
    end
  end

  # Conflict detection — RFC 4918 §10.4 collection-lock semantics:
  #
  #   exclusive vs *           = conflict
  #   shared    vs shared      = ok (compatible)
  #   shared    vs exclusive   = conflict
  #
  # Multi-granularity:
  #   subtree(/a)              conflicts with any *-claim on /a/x.
  #   path(/a/x)               conflicts with subtree(/a) (or any ancestor
  #                            subtree).
  #   subtree(/a)              conflicts with subtree(/a/b) and vice
  #                            versa (overlapping subtrees).
  defp detect_namespace_conflict(new_type, new_path, new_scope, claims) do
    Enum.find_value(claims, :ok, fn {claim_id, existing} ->
      if claims_conflict?(new_type, new_path, new_scope, existing) do
        {:error, claim_id}
      end
    end)
  end

  defp claims_conflict?(new_type, new_path, new_scope, %{
         type: existing_type,
         path: existing_path,
         scope: existing_scope
       }) do
    overlaps?(new_type, new_path, existing_type, existing_path) and
      scopes_conflict?(new_scope, existing_scope)
  end

  defp overlaps?(:path, a, :path, b), do: a == b
  defp overlaps?(:path, p, :subtree, root), do: in_subtree?(p, root)
  defp overlaps?(:subtree, root, :path, p), do: in_subtree?(p, root)

  defp overlaps?(:subtree, root_a, :subtree, root_b),
    do: in_subtree?(root_a, root_b) or in_subtree?(root_b, root_a)

  defp scopes_conflict?(:exclusive, _), do: true
  defp scopes_conflict?(_, :exclusive), do: true
  defp scopes_conflict?(:shared, :shared), do: false

  # `path` is in the subtree rooted at `root` when it equals `root` or
  # sits under `root` separated by `/`. Special-case `"/"` so a
  # whole-volume claim covers everything.
  defp in_subtree?(_path, "/"), do: true

  defp in_subtree?(path, root) do
    path == root or String.starts_with?(path, root <> "/")
  end

  defp upsert_acl_entry(entries, principal, new_entry) do
    case Enum.find_index(entries, &(&1.principal == principal)) do
      nil -> entries ++ [new_entry]
      idx -> List.replace_at(entries, idx, new_entry)
    end
  end
end
