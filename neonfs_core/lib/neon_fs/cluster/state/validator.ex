defmodule NeonFS.Cluster.State.Validator do
  @moduledoc """
  Validates cluster state values after parsing.

  Returns specific field-level error messages instead of generic
  `:invalid_json` errors, helping operators diagnose configuration
  problems in `cluster.json`.
  """

  alias NeonFS.Cluster.State
  alias NeonFS.Core.DriveConfig

  @valid_tiers ~w(hot warm cold)
  @atom_safe_regex ~r/^[a-zA-Z0-9_@.\-]+$/

  @type error :: %{field: atom(), message: String.t()}

  @doc """
  Validates a `%State{}` struct, returning `:ok` or `{:error, errors}`.

  All fields are checked and multiple errors are collected rather than
  short-circuiting on the first failure.
  """
  @spec validate(State.t()) :: :ok | {:error, [error()]}
  def validate(%State{} = state) do
    errors =
      []
      |> validate_cluster_id(state.cluster_id)
      |> validate_cluster_name(state.cluster_name)
      |> validate_created_at(state.created_at)
      |> validate_drives(state.drives)
      |> validate_gc(state.gc)
      |> validate_metrics(state.metrics)
      |> validate_peer_settings(state)
      |> validate_ra_cluster_members(state.ra_cluster_members)
      |> validate_this_node(state.this_node)
      |> validate_worker(state.worker)

    case errors do
      [] -> :ok
      errors -> {:error, Enum.reverse(errors)}
    end
  end

  # Cluster ID — must be a non-empty string

  defp validate_cluster_id(errors, cluster_id) do
    validate_field(errors, cluster_id, :cluster_id, &non_empty_string?/1)
  end

  # Cluster name — must be a non-empty string

  defp validate_cluster_name(errors, cluster_name) do
    validate_field(errors, cluster_name, :cluster_name, &non_empty_string?/1)
  end

  # Created at — must be a DateTime struct

  defp validate_created_at(errors, %DateTime{}), do: errors

  defp validate_created_at(errors, _) do
    add_error(errors, :created_at, "must be a valid ISO 8601 datetime")
  end

  # Drives — each must have valid path, tier, and capacity

  defp validate_drives(errors, drives) when is_list(drives) do
    drives
    |> Enum.with_index()
    |> Enum.reduce(errors, &validate_single_drive/2)
  end

  defp validate_drives(errors, _), do: add_error(errors, :drives, "must be a list")

  # GC config — optional fields with type constraints

  defp validate_gc(errors, gc) when is_map(gc) and map_size(gc) == 0, do: errors

  defp validate_gc(errors, gc) when is_map(gc) do
    errors
    |> validate_config_field(gc, "interval_ms", :"gc.interval_ms", &positive_integer?/1)
    |> validate_config_field(
      gc,
      "pressure_threshold",
      :"gc.pressure_threshold",
      &valid_threshold?/1
    )
  end

  defp validate_gc(errors, _), do: errors

  # Peer settings — must be positive integers

  # Metrics config — optional fields with type constraints

  defp validate_metrics(errors, metrics) when is_map(metrics) and map_size(metrics) == 0,
    do: errors

  defp validate_metrics(errors, metrics) when is_map(metrics) do
    errors
    |> validate_config_field(metrics, "bind", :"metrics.bind", &non_empty_string?/1)
    |> validate_config_field(metrics, "enabled", :"metrics.enabled", &boolean?/1)
    |> validate_config_field(
      metrics,
      "poll_interval_ms",
      :"metrics.poll_interval_ms",
      &positive_integer?/1
    )
    |> validate_config_field(metrics, "port", :"metrics.port", &positive_integer?/1)
  end

  defp validate_metrics(errors, _), do: errors

  # Peer settings — must be positive integers

  defp validate_peer_settings(errors, state) do
    errors
    |> validate_field(
      state.min_peers_for_operation,
      :min_peers_for_operation,
      &positive_integer?/1
    )
    |> validate_field(state.peer_connect_timeout, :peer_connect_timeout, &positive_integer?/1)
    |> validate_field(state.peer_sync_interval, :peer_sync_interval, &positive_integer?/1)
    |> validate_field(state.startup_peer_timeout, :startup_peer_timeout, &positive_integer?/1)
  end

  # Ra cluster members — must be valid node name atoms

  defp validate_ra_cluster_members(errors, members) when is_list(members) do
    members
    |> Enum.with_index()
    |> Enum.reduce(errors, &validate_ra_member/2)
  end

  defp validate_ra_cluster_members(errors, _) do
    add_error(errors, :ra_cluster_members, "must be a list")
  end

  # This node — must have required fields with valid values

  defp validate_this_node(errors, this_node) when is_map(this_node) do
    errors
    |> validate_this_node_fields(this_node)
    |> validate_this_node_name(this_node)
  end

  defp validate_this_node(errors, _), do: add_error(errors, :this_node, "must be a map")

  # Worker config — optional fields with type constraints

  defp validate_worker(errors, worker) when is_map(worker) and map_size(worker) == 0, do: errors

  defp validate_worker(errors, worker) when is_map(worker) do
    errors
    |> validate_config_field(
      worker,
      "max_concurrent",
      :"worker.max_concurrent",
      &positive_integer?/1
    )
    |> validate_config_field(
      worker,
      "max_per_minute",
      :"worker.max_per_minute",
      &positive_integer?/1
    )
    |> validate_config_field(
      worker,
      "drive_concurrency",
      :"worker.drive_concurrency",
      &positive_integer?/1
    )
  end

  defp validate_worker(errors, _), do: errors

  # Validator functions

  defp atom_safe?(value) when is_atom(value), do: atom_safe?(Atom.to_string(value))

  defp atom_safe?(value) when is_binary(value) do
    if Regex.match?(@atom_safe_regex, value),
      do: :ok,
      else: {:error, "contains invalid characters"}
  end

  defp atom_safe?(_), do: {:error, "must be an atom or string"}

  defp non_empty_string?(nil), do: {:error, "is required"}
  defp non_empty_string?(value) when not is_binary(value), do: {:error, "must be a string"}
  defp non_empty_string?(""), do: {:error, "must not be empty"}
  defp non_empty_string?(_), do: :ok

  defp boolean?(value) when is_boolean(value), do: :ok
  defp boolean?(_), do: {:error, "must be a boolean"}

  defp valid_capacity?(nil), do: {:error, "is required"}
  defp valid_capacity?(""), do: {:error, "must not be empty"}

  defp valid_capacity?(value) when is_binary(value) do
    case DriveConfig.parse_capacity(value) do
      {:ok, _bytes} -> :ok
      {:error, _} -> {:error, "invalid capacity format (got: #{inspect(value)})"}
    end
  end

  defp valid_capacity?(value) when is_integer(value) and value >= 0, do: :ok

  defp valid_capacity?(value) do
    {:error, "invalid capacity (got: #{inspect(value)})"}
  end

  defp positive_integer?(value) when is_integer(value) and value > 0, do: :ok
  defp positive_integer?(_), do: {:error, "must be a positive integer"}

  defp valid_node_name?(value) when is_atom(value) do
    name = Atom.to_string(value)

    if String.contains?(name, "@"),
      do: :ok,
      else: {:error, "must be a valid node name (name@host format)"}
  end

  defp valid_node_name?(_), do: {:error, "must be an atom"}

  defp valid_threshold?(value) when is_number(value) and value >= 0.0 and value <= 1.0, do: :ok
  defp valid_threshold?(value) when is_number(value), do: {:error, "must be between 0.0 and 1.0"}
  defp valid_threshold?(_), do: {:error, "must be a float between 0.0 and 1.0"}

  defp valid_tier?(nil), do: {:error, "is required"}

  defp valid_tier?(value) do
    tier_str = to_string(value)

    if tier_str in @valid_tiers,
      do: :ok,
      else: {:error, "must be one of: hot, warm, cold (got: #{tier_str})"}
  end

  # Sub-validation helpers

  defp validate_config_field(errors, map, key, field, validator_fn) do
    value = Map.get(map, key) || Map.get(map, String.to_atom(key))

    case value do
      nil -> errors
      v -> validate_field(errors, v, field, validator_fn)
    end
  end

  defp validate_drive_capacity(errors, drive, index) do
    capacity = get_field(drive, :capacity, "capacity")
    validate_field(errors, capacity, :"drives[#{index}].capacity", &valid_capacity?/1)
  end

  defp validate_drive_path(errors, drive, index) do
    path = get_field(drive, :path, "path")
    validate_field(errors, path, :"drives[#{index}].path", &non_empty_string?/1)
  end

  defp validate_drive_tier(errors, drive, index) do
    tier = get_field(drive, :tier, "tier")
    validate_field(errors, tier, :"drives[#{index}].tier", &valid_tier?/1)
  end

  defp validate_ra_member({member, index}, errors) do
    validate_field(errors, member, :"ra_cluster_members[#{index}]", &valid_node_name?/1)
  end

  defp validate_single_drive({drive, index}, errors) do
    errors
    |> validate_drive_path(drive, index)
    |> validate_drive_tier(drive, index)
    |> validate_drive_capacity(drive, index)
  end

  defp validate_this_node_fields(errors, this_node) do
    missing =
      [:id, :name, :joined_at]
      |> Enum.reject(&Map.has_key?(this_node, &1))

    case missing do
      [] ->
        errors

      keys ->
        names = Enum.map_join(keys, ", ", &Atom.to_string/1)
        add_error(errors, :this_node, "missing required fields: #{names}")
    end
  end

  defp validate_this_node_name(errors, this_node) do
    case Map.get(this_node, :name) do
      nil -> errors
      name -> validate_field(errors, name, :"this_node.name", &atom_safe?/1)
    end
  end

  # Generic helpers

  defp add_error(errors, field, message) do
    [%{field: field, message: message} | errors]
  end

  defp get_field(map, atom_key, string_key) do
    Map.get(map, atom_key) || Map.get(map, string_key)
  end

  defp validate_field(errors, value, field, validator_fn) do
    case validator_fn.(value) do
      :ok -> errors
      {:error, msg} -> add_error(errors, field, msg)
    end
  end
end
