# Task 0142: JSON Log Formatter Setup

## Status
Complete

## Phase
Gap Analysis — M-7 (1/3)

## Description
Add a JSON log formatter to the project so that production deployments emit
structured JSON logs. This is the foundation for structured logging — later
tasks will add metadata fields and convert existing log calls.

Elixir 1.19+ supports custom log formatters via the `:default_handler`
config. Use `LoggerJSON` (a well-maintained hex package) or the built-in
formatter capabilities to configure JSON output in production while keeping
human-readable console output in development and test.

## Acceptance Criteria
- [ ] `logger_json` added as a dependency to `neonfs_core/mix.exs`
- [ ] `logger_json` added as a dependency to `neonfs_fuse/mix.exs`
- [ ] Production config (`config/runtime.exs` or `config/prod.exs`) configures JSON formatter
- [ ] Dev/test config retains the default console formatter
- [ ] JSON output includes: `time`, `level`, `message`, `metadata` fields
- [ ] Standard metadata fields defined: `:component`, `:node_name`, `:volume_id`
- [ ] `NeonFS.Core.Logger` helper module created with convenience functions that set component metadata
- [ ] Helper provides `with_metadata/2` that sets Logger metadata for a block
- [ ] Container release config (`rel/env.sh.eex` or runtime config) uses JSON formatter
- [ ] Unit test: verify LoggerJSON is configured in prod environment
- [ ] Smoke test: Logger.info with metadata produces valid JSON in prod config
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit test verifying the formatter configuration is set correctly for each environment
- Smoke test that emits a log line in prod config and parses the output as valid JSON
- Verify existing tests pass (no log format changes in test env)

## Dependencies
- None

## Files to Create/Modify
- `neonfs_core/mix.exs` (modify — add `logger_json` dep)
- `neonfs_fuse/mix.exs` (modify — add `logger_json` dep)
- `neonfs_core/config/prod.exs` or `neonfs_core/config/runtime.exs` (modify — formatter config)
- `neonfs_core/lib/neon_fs/core/log.ex` (create — logger helper module)
- `neonfs_core/test/neon_fs/core/log_test.exs` (create — tests)

## Reference
- `spec/operations.md` lines 200–218
- `spec/observability.md`
- `spec/gap-analysis.md` — M-7
- https://hex2txt.fly.dev/logger_json/llms.txt (package documentation)

## Notes
`LoggerJSON` supports multiple formatter backends. Use
`LoggerJSON.Formatters.Basic` for straightforward JSON output. The formatter
automatically includes Elixir's Logger metadata in the JSON output, so any
`Logger.metadata/1` calls will appear in the structured output.

Keep dev and test environments using the console formatter — developers
shouldn't have to parse JSON when reading logs locally. The JSON formatter
is specifically for production/container deployments where logs are
aggregated by tools like Loki, Elasticsearch, or CloudWatch.
