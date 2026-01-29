# NeonFS CI Configuration

This directory contains Forgejo Actions workflow configurations for continuous integration.

## Workflows

### ci.yml

The main CI workflow runs on:
- Push to `main` or `develop` branches
- Pull requests to `main`

#### Jobs

1. **lint**: Code formatting and style checks
   - Elixir: `mix format --check-formatted`, `mix credo --strict`
   - Rust: `cargo fmt --check`, `cargo clippy`

2. **unit-tests**: Fast unit tests
   - Elixir: `mix test --exclude integration`
   - Rust: `cargo test`
   - Excludes FUSE tests (require kernel support)

3. **dialyzer**: Static type analysis for Elixir
   - Uses PLT caching for faster runs
   - Runs on both `neonfs_core` and `neonfs_fuse`

4. **integration-tests**: Full acceptance test suite
   - Runs `scripts/acceptance-test.sh`
   - Requires privileged container for FUSE
   - Tests full system: build → start → CLI → FUSE → persistence

5. **build-release**: Production release build
   - Builds release tarballs for both services
   - Builds CLI binary
   - Uploads artifacts

## Running Locally

### Prerequisites

- Docker or Podman
- Forgejo Actions runner (for CI simulation)

### Using the CI Docker Image

Build the CI image:

```bash
docker build -f Dockerfile.ci -t neonfs-ci:latest .
```

Run tests in the container:

```bash
docker run --rm -v $(pwd):/workspace neonfs-ci:latest bash -c '
  cd /workspace
  . ~/.asdf/asdf.sh
  cd neonfs_core && mix deps.get && mix test --exclude integration
'
```

### Running Acceptance Tests

The acceptance test script can run outside of CI:

```bash
./scripts/acceptance-test.sh
```

Requirements:
- Elixir 1.19.5 (OTP 28)
- Rust 1.93.0
- FUSE3 (optional, tests will skip if not available)

## Caching Strategy

The workflow uses GitHub Actions-compatible caching:

1. **Dependencies**: Mix deps, Cargo registry/git, Rust target dirs
   - Key: `${{ runner.os }}-test-${{ hashFiles('**/mix.lock', '**/Cargo.lock') }}`

2. **Dialyzer PLTs**: Persistent Lookup Tables
   - Key: `plt-${{ hashFiles('**/mix.lock') }}`

Caches are restored with prefix matching, allowing partial cache hits when dependencies change.

## FUSE Tests

FUSE tests require:
- Privileged container (or `--device /dev/fuse`)
- FUSE kernel module loaded on host
- `fusermount` or `umount` commands available

The integration-tests job uses `options: --privileged` to enable FUSE support.

## Forgejo Runner Setup

To run these workflows on your Forgejo instance:

1. Install Forgejo Actions runner
2. Register runner with your instance
3. Configure runner with Docker/Podman
4. Ensure runner has access to container images

See: https://forgejo.org/docs/latest/admin/actions/

## Troubleshooting

### Tests fail with "FUSE not available"

This is expected in environments without FUSE support. The acceptance test script gracefully skips FUSE tests when fusermount is not available.

### Dialyzer takes too long

PLT caching should make subsequent runs faster. First run builds PLTs (5-10 minutes), later runs use cached PLTs (1-2 minutes).

### Integration tests time out

Services may need more time to start. The acceptance script includes sleep delays after starting services. Increase these if needed.
