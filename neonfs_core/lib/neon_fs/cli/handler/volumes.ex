defmodule NeonFS.CLI.Handler.Volumes do
  @moduledoc """
  Volume-operations CLI command handlers: list, create, update, delete,
  get, and per-volume key rotation.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `list_volumes/1`, `create_volume/2`, `update_volume/2`,
  `delete_volume/1,2`, `get_volume/1`, `rotate_volume_key/1` and
  `rotation_status/1` RPC entry points here, so the CLI wire contract is
  unchanged.

  `map_to_opts/1` is public because the FUSE mount handler (still on the
  facade) reuses it to coerce its options map; the volume-config parsing
  closure it anchors lives here with the rest of the volume logic.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.{
    ACLManager,
    AuditLog,
    Authorise,
    DriveRegistry,
    KeyManager,
    KeyRotation,
    MetadataStateMachine,
    RaSupervisor,
    Volume,
    VolumeACL,
    VolumeEncryption,
    VolumeRegistry
  }

  alias NeonFS.Error.{Invalid, InvalidConfig, NotFound, PermissionDenied, VolumeNotFound}

  require Logger

  @immutable_update_fields ~w(durability encryption name id)
  @tiering_fields ~w(initial_tier promotion_threshold demotion_delay)
  @caching_fields ~w(transformed_chunks reconstructed_stripes remote_chunks)
  @verification_fields ~w(on_read sampling_rate scrub_interval)
  @metadata_consistency_fields ~w(metadata_replicas read_quorum write_quorum)

  @doc """
  Lists volumes in the cluster.

  ## Parameters
  - `filters` - Optional filter map:
    - `"all"` - When `true`, includes system volumes (default: excluded)

  ## Returns
  - `{:ok, [map]}` - List of volume maps
  """
  @spec list_volumes(map()) :: {:ok, [map()]}
  def list_volumes(filters \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      opts =
        case Map.get(filters, "all") do
          true -> [include_system: true]
          _ -> []
        end

      volumes =
        VolumeRegistry.list(opts)
        |> Enum.map(&volume_to_map/1)

      {:ok, volumes}
    end
  end

  @doc """
  Creates a new volume with the given name and configuration.

  ## Parameters
  - `name` - Volume name (string)
  - `config` - Configuration map with optional keys:
    - `:owner` - Owner string
    - `:durability` - Durability config map
    - `:write_ack` - Write acknowledgment level (`:local`, `:quorum`, `:all`)
    - `:tiering` - Tiering config map (`:initial_tier`, `:promotion_threshold`, `:demotion_delay`)
    - `:caching` - Caching config map (`:transformed_chunks`, `:reconstructed_stripes`, `:remote_chunks`)
    - `:io_weight` - I/O scheduling weight (positive integer)
    - `:compression` - Compression config map
    - `:verification` - Verification config map

  ## Returns
  - `{:ok, map}` - Created volume as map
  - `{:error, reason}` - Error tuple
  """
  @spec create_volume(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def create_volume(name, config) when is_binary(name) and is_map(config) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      opts = map_to_opts(config)
      owner_uid = Keyword.get(opts, :owner_uid, 0)
      owner_gid = Keyword.get(opts, :owner_gid, 0)

      with {:ok, parsed_opts} <- parse_durability_opt(opts),
           {:ok, enc_opts} <- parse_encryption_opt(parsed_opts),
           final_opts = merge_verification_defaults(enc_opts),
           :ok <- check_durability_fits_cluster(name, final_opts),
           {:ok, volume} <- VolumeRegistry.create(name, final_opts),
           :ok <- setup_encryption_if_needed(volume) do
        create_initial_acl(volume.id, owner_uid, owner_gid)

        AuditLog.log_event(
          event_type: :volume_created,
          actor_uid: owner_uid,
          resource: volume.id,
          details: %{name: name}
        )

        {:ok, volume_to_map(volume)}
      else
        {:error, %NeonFS.Error.AlreadyExists{}} ->
          {:error, Invalid.exception(message: "Volume '#{name}' already exists")}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Updates an existing volume's configuration.

  ## Parameters
  - `name` - Volume name (string)
  - `config` - Configuration map with string keys. Supported fields:
    - `"atime_mode"` - POSIX atime mode (`"noatime"` or `"relatime"`)
    - `"compression"` - Compression config map (e.g. `%{"algorithm" => "none"}`)
    - `"io_weight"` - I/O scheduling weight (positive integer)
    - `"write_ack"` - Write acknowledgement level (`"local"`, `"quorum"`, `"all"`)
    - `"initial_tier"` / `"promotion_threshold"` / `"demotion_delay"` - Tiering sub-fields
    - `"transformed_chunks"` / `"reconstructed_stripes"` / `"remote_chunks"` - Caching sub-fields
    - `"on_read"` / `"sampling_rate"` / `"scrub_interval"` - Verification sub-fields
    - `"metadata_replicas"` / `"read_quorum"` / `"write_quorum"` - Metadata consistency sub-fields

  Immutable fields (`durability`, `encryption`, `name`, `id`) are rejected.
  Nested sub-fields are merged with the volume's current configuration.

  ## Returns
  - `{:ok, map}` - Updated volume as map
  - `{:error, reason}` - Error tuple
  """
  @spec update_volume(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def update_volume(name, config) when is_binary(name) and is_map(config) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(name),
         :ok <- reject_immutable_updates(config),
         opts = build_update_opts(config, volume),
         {:ok, updated} <- VolumeRegistry.update(volume.id, opts) do
      :telemetry.execute(
        [:neonfs, :cli, :volume_updated],
        %{},
        %{volume_id: volume.id, name: name, fields: Map.keys(config)}
      )

      AuditLog.log_event(
        event_type: :volume_updated,
        actor_uid: 0,
        resource: volume.id,
        details: %{name: name, fields: Map.keys(config)}
      )

      {:ok, volume_to_map(updated)}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Deletes a volume by name.

  ## Parameters
  - `name` - Volume name (string)

  ## Returns
  - `{:ok, %{}}` - Success with empty map
  - `{:error, reason}` - Error tuple
  """
  @spec delete_volume(String.t()) :: {:ok, map()} | {:error, term()}
  def delete_volume(name), do: delete_volume(name, [])

  @spec delete_volume(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def delete_volume(name, opts) when is_binary(name) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      uid = Keyword.get(opts, :uid, 0)
      gids = Keyword.get(opts, :gids, [])

      with {:ok, volume} <- VolumeRegistry.get_by_name(name),
           :ok <- Authorise.check(uid, gids, :admin, {:volume, volume.id}),
           :ok <- VolumeRegistry.delete(volume.id) do
        cleanup_volume_acl(volume.id)

        AuditLog.log_event(
          event_type: :volume_deleted,
          actor_uid: uid,
          resource: volume.id,
          details: %{name: name}
        )

        {:ok, %{}}
      else
        {:error, :not_found} ->
          {:error, VolumeNotFound.exception(volume_name: name)}

        {:error, %{class: :forbidden}} ->
          {:error, PermissionDenied.exception(operation: :admin)}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Gets volume details by name.

  ## Parameters
  - `name` - Volume name (string)

  ## Returns
  - `{:ok, map}` - Volume details as map
  - `{:error, reason}` - Error tuple
  """
  @spec get_volume(String.t()) :: {:ok, map()} | {:error, term()}
  def get_volume(name) when is_binary(name) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case VolumeRegistry.get_by_name(name) do
        {:ok, volume} ->
          {:ok, volume_to_map(volume)}

        {:error, :not_found} ->
          {:error, VolumeNotFound.exception(volume_name: name)}
      end
    end
  end

  @doc """
  Starts key rotation for a volume.

  ## Parameters
  - `volume_name` - Volume name (string)

  ## Returns
  - `{:ok, map}` - Rotation info with from_version, to_version, total_chunks
  - `{:error, reason}` - Error tuple
  """
  @spec rotate_volume_key(String.t()) :: {:ok, map()} | {:error, term()}
  def rotate_volume_key(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, result} <- KeyRotation.start_rotation(volume.id) do
      {:ok, result}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns the current key rotation status for a volume.

  ## Parameters
  - `volume_name` - Volume name (string)

  ## Returns
  - `{:ok, map}` - Rotation state with progress
  - `{:error, :no_rotation}` - No rotation in progress
  - `{:error, reason}` - Error tuple
  """
  @spec rotation_status(String.t()) :: {:ok, map()} | {:error, term()}
  def rotation_status(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, result} <- KeyRotation.rotation_status(volume.id) do
      {:ok, result}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, :no_rotation} ->
        {:error, NotFound.exception(message: "No key rotation in progress for '#{volume_name}'")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Converts a string-keyed map to a keyword list, coercing keys to
  existing atoms. Falls back to the raw map entries when a key has no
  existing atom. Public because the FUSE mount handler reuses it.
  """
  @spec map_to_opts(map()) :: keyword()
  def map_to_opts(map) when is_map(map) do
    map
    |> Enum.map(fn {k, v} -> {String.to_existing_atom(k), v} end)
    |> Enum.into([])
  rescue
    # If key doesn't exist as atom, use string key directly
    ArgumentError ->
      Enum.into(map, [])
  end

  # Private

  defp volume_to_map(%Volume{} = volume) do
    %{
      id: volume.id,
      name: volume.name,
      owner: volume.owner,
      atime_mode: volume.atime_mode,
      durability: volume.durability,
      durability_display: format_durability(volume.durability),
      write_ack: volume.write_ack,
      tiering: volume.tiering,
      caching: volume.caching,
      io_weight: volume.io_weight,
      compression: volume.compression,
      verification: volume.verification,
      metadata_consistency: volume.metadata_consistency,
      encryption: encryption_to_map(volume.encryption),
      logical_size: volume.logical_size,
      physical_size: volume.physical_size,
      chunk_count: volume.chunk_count,
      file_count: volume.file_count,
      created_at: DateTime.to_iso8601(volume.created_at),
      updated_at: DateTime.to_iso8601(volume.updated_at)
    }
    |> put_quota(:max_size, volume.max_size)
    |> put_quota(:max_files, volume.max_files)
  end

  # A `nil` quota (unlimited) is omitted rather than encoded as the atom
  # `nil`, so the CLI decodes the key's absence as "no limit" without
  # needing to special-case the ETF atom.
  defp put_quota(map, _key, nil), do: map
  defp put_quota(map, key, value), do: Map.put(map, key, value)

  defp parse_encryption_opt(opts) do
    case Keyword.get(opts, :encryption) do
      nil ->
        {:ok, opts}

      %VolumeEncryption{} ->
        {:ok, opts}

      %{mode: mode} when is_atom(mode) ->
        enc = build_encryption_config(mode)
        {:ok, Keyword.put(opts, :encryption, enc)}

      %{"mode" => mode} when is_binary(mode) ->
        enc = build_encryption_config(String.to_existing_atom(mode))
        {:ok, Keyword.put(opts, :encryption, enc)}

      _other ->
        {:ok, opts}
    end
  rescue
    ArgumentError ->
      {:error, InvalidConfig.exception(field: :encryption, reason: "invalid encryption mode")}
  end

  defp build_encryption_config(:none), do: VolumeEncryption.new(mode: :none)

  defp build_encryption_config(:server_side) do
    VolumeEncryption.new(mode: :server_side, current_key_version: 1)
  end

  defp setup_encryption_if_needed(volume) do
    if Volume.encrypted?(volume) do
      case KeyManager.setup_volume_encryption(volume.id) do
        {:ok, _version} -> :ok
        {:error, reason} -> {:error, reason}
      end
    else
      :ok
    end
  end

  defp merge_verification_defaults(opts) do
    case Keyword.get(opts, :verification) do
      nil ->
        opts

      config when is_map(config) ->
        Keyword.put(opts, :verification, Map.merge(Volume.default_verification(), config))
    end
  end

  defp parse_durability_opt(opts) do
    case Keyword.get(opts, :durability) do
      nil ->
        {:ok, opts}

      durability when is_binary(durability) ->
        case parse_durability(durability) do
          {:ok, config} -> {:ok, Keyword.put(opts, :durability, config)}
          {:error, _} = err -> err
        end

      _map ->
        {:ok, opts}
    end
  end

  # Refuse `neonfs volume create` when the requested durability needs
  # more drives than the cluster currently has — replicas and erasure
  # shards are placed on distinct drives, which may live on the same
  # node, so the bound is the cluster-wide drive count, not the core
  # node count (#1032). A single multi-drive node can satisfy
  # `replicate:2`; a factor above the total drive count cannot be
  # placed and writes would leave the chunk under-replicated.
  # `--allow-under-replicated` plumbs through as
  # `allow_under_replicated: true` for operators who are about to
  # scale out and want the volume in place ahead of the new drives.
  defp check_durability_fits_cluster(name, opts) do
    cond do
      Keyword.get(opts, :allow_under_replicated, false) ->
        :ok

      needed = required_replicas_from_opts(opts) ->
        check_replica_count(name, needed)

      true ->
        :ok
    end
  end

  defp check_replica_count(name, needed) do
    drive_count = cluster_drive_count()

    if needed <= drive_count do
      :ok
    else
      {:error,
       Invalid.exception(
         message:
           "Volume '#{name}' needs #{needed} replicas but the cluster has only " <>
             "#{drive_count} drive(s). Add more drives, lower the replication " <>
             "factor, or pass `--allow-under-replicated` to create the volume " <>
             "anyway (chunks stay under-replicated until the cluster grows).",
         details: %{volume_name: name, required_replicas: needed, drives: drive_count}
       )}
    end
  end

  # The authoritative cluster-wide drive set lives in the Ra
  # `MetadataStateMachine` (strongly consistent). `DriveRegistry`'s ETS
  # table is an eventually-consistent local cache that under-counts
  # remote drives until its periodic peer sync catches up, which races
  # volume creation during cluster formation (#1032). Prefer Ra; fall
  # back to the local cache only when Ra is unreachable (no core node
  # to coordinate with — e.g. handler unit tests without a cluster).
  defp cluster_drive_count do
    case RaSupervisor.local_query(&MetadataStateMachine.get_drives/1) do
      {:ok, drives} when is_map(drives) -> map_size(drives)
      _ -> length(DriveRegistry.list_drives())
    end
  end

  defp required_replicas_from_opts(opts) do
    case Keyword.get(opts, :durability) do
      %{type: :replicate, factor: factor} when is_integer(factor) -> factor
      %{type: :erasure, data_chunks: d, parity_chunks: p} -> d + p
      _ -> nil
    end
  end

  defp parse_durability("replicate:" <> rest) do
    case Integer.parse(rest) do
      {n, ""} when n >= 1 ->
        {:ok, %{type: :replicate, factor: n, min_copies: max(1, n - 1)}}

      _ ->
        {:error, durability_format_error()}
    end
  end

  defp parse_durability("erasure:" <> rest) do
    parse_erasure_parts(String.split(rest, ":"))
  end

  defp parse_durability(_), do: {:error, durability_format_error()}

  defp parse_erasure_parts([d_str, p_str]) do
    with {d, ""} <- Integer.parse(d_str),
         {p, ""} <- Integer.parse(p_str),
         true <- d >= 1 and p >= 1 do
      {:ok, %{type: :erasure, data_chunks: d, parity_chunks: p}}
    else
      _ -> {:error, durability_format_error()}
    end
  end

  defp parse_erasure_parts(_), do: {:error, durability_format_error()}

  defp durability_format_error do
    InvalidConfig.exception(
      field: :durability,
      reason: "invalid format, use 'replicate:N' or 'erasure:D:P'"
    )
  end

  defp format_durability(%{type: :replicate, factor: factor}) do
    "replicate:#{factor}"
  end

  defp format_durability(%{type: :erasure, data_chunks: d, parity_chunks: p}) do
    overhead = (d + p) / d
    overhead_str = :erlang.float_to_binary(overhead, decimals: 2)
    "erasure:#{d}+#{p} (#{overhead_str}x overhead)"
  end

  defp format_durability(_), do: "unknown"

  defp cleanup_volume_acl(volume_id) do
    ACLManager.delete_volume_acl(volume_id)
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp create_initial_acl(volume_id, owner_uid, owner_gid) do
    acl = VolumeACL.new(volume_id: volume_id, owner_uid: owner_uid, owner_gid: owner_gid)
    ACLManager.set_volume_acl(volume_id, acl)
  rescue
    _ -> Logger.warning("Failed to create initial ACL for volume", volume_id: volume_id)
  catch
    :exit, _ -> Logger.warning("Failed to create initial ACL for volume", volume_id: volume_id)
  end

  defp encryption_to_map(%VolumeEncryption{} = enc) do
    base = %{
      mode: Atom.to_string(enc.mode),
      current_key_version: enc.current_key_version
    }

    case enc.rotation do
      nil ->
        Map.put(base, :rotation, nil)

      rotation ->
        Map.put(base, :rotation, %{
          from_version: rotation.from_version,
          to_version: rotation.to_version,
          started_at: DateTime.to_iso8601(rotation.started_at),
          progress: rotation.progress
        })
    end
  end

  defp reject_immutable_updates(config) do
    found =
      config
      |> Map.keys()
      |> Enum.filter(&(&1 in @immutable_update_fields))

    if found == [] do
      :ok
    else
      {:error,
       InvalidConfig.exception(
         field: :immutable,
         reason: "cannot update immutable fields: #{Enum.join(found, ", ")}"
       )}
    end
  end

  defp build_update_opts(config, volume) do
    []
    |> maybe_put_simple(config, "atime_mode", :atime_mode, &coerce_atom/1)
    |> maybe_put_simple(config, "io_weight", :io_weight, &coerce_integer/1)
    |> maybe_put_simple(config, "write_ack", :write_ack, &coerce_atom/1)
    |> maybe_put_simple(config, "owner", :owner, &Function.identity/1)
    |> maybe_put_nested(config, @tiering_fields, :tiering, volume.tiering, &coerce_tiering/2)
    |> maybe_put_nested(config, @caching_fields, :caching, volume.caching, &coerce_caching/2)
    |> maybe_put_nested(
      config,
      @verification_fields,
      :verification,
      volume.verification,
      &coerce_verification/2
    )
    |> maybe_put_nested(
      config,
      @metadata_consistency_fields,
      :metadata_consistency,
      volume.metadata_consistency,
      &coerce_metadata_consistency/2
    )
    |> maybe_put_compression(config, volume)
  end

  defp maybe_put_simple(opts, config, string_key, opt_key, coerce_fn) do
    case Map.fetch(config, string_key) do
      {:ok, value} -> Keyword.put(opts, opt_key, coerce_fn.(value))
      :error -> opts
    end
  end

  defp maybe_put_nested(opts, config, field_names, opt_key, current, coerce_fn) do
    sub_config = Map.take(config, field_names)

    if map_size(sub_config) == 0 do
      opts
    else
      merged =
        Enum.reduce(sub_config, current || %{}, fn {k, v}, acc ->
          coerce_fn.(acc, {k, v})
        end)

      Keyword.put(opts, opt_key, merged)
    end
  end

  defp maybe_put_compression(opts, config, volume) do
    case Map.fetch(config, "compression") do
      {:ok, comp} when is_map(comp) ->
        coerced = coerce_compression_map(comp)
        merged = Map.merge(volume.compression, coerced)
        Keyword.put(opts, :compression, merged)

      {:ok, comp} ->
        Keyword.put(opts, :compression, %{algorithm: coerce_atom(comp)})

      :error ->
        opts
    end
  end

  defp coerce_compression_map(map) do
    map
    |> atomise_map()
    |> Map.new(fn
      {:algorithm, v} -> {:algorithm, coerce_atom(v)}
      {:level, v} -> {:level, coerce_integer(v)}
      {:min_size, v} -> {:min_size, coerce_integer(v)}
      other -> other
    end)
  end

  defp coerce_tiering(acc, {"initial_tier", v}), do: Map.put(acc, :initial_tier, coerce_atom(v))

  defp coerce_tiering(acc, {"promotion_threshold", v}),
    do: Map.put(acc, :promotion_threshold, coerce_integer(v))

  defp coerce_tiering(acc, {"demotion_delay", v}),
    do: Map.put(acc, :demotion_delay, coerce_integer(v))

  defp coerce_caching(acc, {"transformed_chunks", v}),
    do: Map.put(acc, :transformed_chunks, coerce_boolean(v))

  defp coerce_caching(acc, {"reconstructed_stripes", v}),
    do: Map.put(acc, :reconstructed_stripes, coerce_boolean(v))

  defp coerce_caching(acc, {"remote_chunks", v}),
    do: Map.put(acc, :remote_chunks, coerce_boolean(v))

  defp coerce_verification(acc, {"on_read", v}), do: Map.put(acc, :on_read, coerce_atom(v))

  defp coerce_verification(acc, {"sampling_rate", v}),
    do: Map.put(acc, :sampling_rate, coerce_float(v))

  defp coerce_verification(acc, {"scrub_interval", v}),
    do: Map.put(acc, :scrub_interval, coerce_integer(v))

  defp coerce_metadata_consistency(acc, {"metadata_replicas", v}),
    do: Map.put(acc, :replicas, coerce_integer(v))

  defp coerce_metadata_consistency(acc, {"read_quorum", v}),
    do: Map.put(acc, :read_quorum, coerce_integer(v))

  defp coerce_metadata_consistency(acc, {"write_quorum", v}),
    do: Map.put(acc, :write_quorum, coerce_integer(v))

  defp coerce_atom(v) when is_atom(v), do: v
  defp coerce_atom(v) when is_binary(v), do: String.to_existing_atom(v)

  defp coerce_integer(v) when is_integer(v), do: v
  defp coerce_integer(v) when is_binary(v), do: String.to_integer(v)

  defp coerce_float(v) when is_float(v), do: v
  defp coerce_float(v) when is_integer(v), do: v / 1
  defp coerce_float(v) when is_binary(v), do: String.to_float(v)

  defp coerce_boolean(v) when is_boolean(v), do: v
  defp coerce_boolean("true"), do: true
  defp coerce_boolean("false"), do: false

  defp atomise_map(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {coerce_atom(k), v} end)
  end
end
