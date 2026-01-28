# Task 0029: Configure Elixir Release

## Status
Complete

## Phase
1 - Foundation

## Description
Configure Mix releases for building deployable NeonFS packages. This includes release configuration, runtime configuration, and the release overlay scripts. **Note: neonfs_core and neonfs_fuse are deployed as separate services/containers, so two separate releases are configured.**

## Acceptance Criteria
- [x] Release configuration in mix.exs (both applications)
- [x] Separate releases for neonfs_core and neonfs_fuse (deployed as separate services)
- [x] Runtime configuration via config/runtime.exs (both applications)
- [x] Cookie via environment variable (RELEASE_COOKIE)
- [x] Environment variable configuration support
- [x] Release builds successfully with `MIX_ENV=prod mix release` (both applications)
- [x] Release starts with `_build/prod/rel/<app>/bin/<app> start`
- [x] Release can be stopped gracefully (SIGTERM)
- [x] Release includes Rust NIFs (verified .so files present)

## Release Configuration
```elixir
# mix.exs (umbrella or combined)
def project do
  [
    releases: [
      neonfs: [
        applications: [
          neonfs_core: :permanent,
          neonfs_fuse: :permanent
        ],
        include_executables_for: [:unix],
        steps: [:assemble, :tar]
      ]
    ]
  ]
end
```

## Runtime Configuration
```elixir
# config/runtime.exs
import Config

config :neonfs_core,
  data_dir: System.get_env("NEONFS_DATA_DIR", "/var/lib/neonfs"),
  node_name: System.get_env("RELEASE_NODE", "neonfs@localhost")

config :neonfs_fuse,
  default_mount_options: []
```

## Testing Strategy
- Build release: `MIX_ENV=prod mix release`
- Start release in foreground
- Verify all applications running
- Stop release gracefully
- Verify Rust NIFs loaded correctly

## Dependencies
- task_0026_elixir_supervision_tree
- task_0027_fuse_supervision_tree
- All NIF tasks complete (blob, fuse)

## Files to Create/Modify
- `mix.exs` (add release configuration)
- `config/runtime.exs` (new or update)
- `rel/env.sh.eex` (release environment)
- `rel/vm.args.eex` (BEAM VM arguments)

## Reference
- spec/deployment.md - Directory Layout
- Elixir release documentation

## Notes
This may require an umbrella project structure or a "meta" mix project that includes both applications. Evaluate the best approach for the monorepo.
