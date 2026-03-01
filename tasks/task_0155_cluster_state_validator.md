# Task 0155: Cluster.State Validator Module

## Status
Complete

## Phase
Gap Analysis — M-12 (1/2)

## Description
Create a `NeonFS.Cluster.State.Validator` module that validates the contents
of `cluster.json` after parsing, returning specific field-level errors
instead of the current blanket `{:error, :invalid_json}`.

Currently `parse_state/1` wraps everything in `rescue _ ->` which catches
all exceptions but gives no detail about what is wrong. A malformed UUID, a
missing required field, or an invalid tier name all produce the same opaque
error. Operators have no way to diagnose configuration problems.

The validator should check all required fields, types, value ranges, and
format constraints, returning a list of specific error descriptions.

## Acceptance Criteria
- [ ] `NeonFS.Cluster.State.Validator` module created
- [ ] `validate/1` accepts a `%State{}` struct and returns `:ok` or `{:error, errors}`
- [ ] `errors` is a list of `%{field: atom, message: String.t()}` maps
- [ ] Validates `cluster_id` is a valid UUID v4 format
- [ ] Validates `cluster_name` is a non-empty string
- [ ] Validates `created_at` is a valid ISO 8601 datetime
- [ ] Validates `this_node` has required fields: `id`, `name`, `joined_at`
- [ ] Validates `this_node.name` is a valid atom-safe string (no special characters)
- [ ] Validates each drive has a valid `path`, known `tier` (`:hot`/`:warm`/`:cold`), and positive `capacity`
- [ ] Validates `gc` config: `interval_ms` is positive integer, `pressure_threshold` is float 0.0–1.0
- [ ] Validates `worker` config: `max_concurrent`, `max_per_minute`, `drive_concurrency` are positive integers
- [ ] Validates `ra_cluster_members` are valid node name atoms
- [ ] Multiple errors are collected (not short-circuiting on first error)
- [ ] `State.load/0` calls `Validator.validate/1` after `parse_state/1`
- [ ] `State.load/0` returns `{:error, {:validation_failed, errors}}` with specific field-level errors
- [ ] Unit test: valid state passes validation
- [ ] Unit test: invalid UUID is caught with clear error message
- [ ] Unit test: missing required field is caught
- [ ] Unit test: invalid drive config is caught
- [ ] Unit test: multiple errors are returned together
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/cluster/state/validator_test.exs`:
  - Build valid State structs, verify `:ok`
  - Mutate individual fields to invalid values, verify specific error messages
  - Build State with multiple invalid fields, verify all errors returned
  - Test edge cases: empty strings, negative numbers, malformed UUIDs

## Dependencies
- None

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cluster/state/validator.ex` (create — validator module)
- `neonfs_core/lib/neon_fs/cluster/state.ex` (modify — call validator in load/0)
- `neonfs_core/test/neon_fs/cluster/state/validator_test.exs` (create — tests)

## Reference
- `spec/operations.md` — configuration management
- `spec/node-management.md`
- `spec/gap-analysis.md` — M-12
- Existing: `neonfs_core/lib/neon_fs/cluster/state.ex` (`parse_state/1`, `load/0`)

## Notes
The validator runs after parsing, not during. This means the struct has
already been constructed — validation checks the values, not the JSON
structure. If parsing itself fails (malformed JSON, missing struct keys
due to `@enforce_keys`), that's a separate error path that should also
produce a clear message.

Consider using a simple validation DSL or helper functions to keep the
validator readable:

```elixir
defp validate_field(errors, value, field, validator_fn) do
  case validator_fn.(value) do
    :ok -> errors
    {:error, msg} -> [%{field: field, message: msg} | errors]
  end
end
```

UUID v4 validation regex: `~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i`
