# Task 0183: CI Integration for Periodic Fuzzing

## Status
Blocked — depends on 0179 (fuzz scaffolding); needs CI design

## Phase
Gap Analysis — H-3 (5/5)

## Description
Integrate the fuzz targets into the Forgejo CI pipeline. Two modes:

1. **Per-PR**: Run each fuzz target for 5 minutes (non-blocking — failure
   posts a comment but doesn't fail the build)
2. **Nightly**: Run each fuzz target for 30 minutes as a scheduled job

CI needs the nightly Rust toolchain and `cargo-fuzz` installed.

## Acceptance Criteria
- [ ] `.forgejo/workflows/fuzz.yml` created (or fuzz job added to existing CI)
- [ ] Nightly Rust toolchain installed in CI job
- [ ] `cargo-fuzz` installed via `cargo install cargo-fuzz`
- [ ] Per-PR job: runs all 3 fuzz targets for 5 minutes each
- [ ] Per-PR job: non-blocking — reports results as PR comment, doesn't fail the build
- [ ] Nightly job: runs all 3 fuzz targets for 30 minutes each
- [ ] Nightly job: failure creates a Forgejo issue with the crash input attached
- [ ] Crash artifacts uploaded as CI artifacts for reproduction
- [ ] Seed corpora checked into the repository and used by CI
- [ ] CI caches the fuzz corpus between runs (grows over time for better coverage)
- [ ] Job timeout set appropriately (45 minutes for PR, 2 hours for nightly)
- [ ] Documentation: comment in workflow file explaining fuzz job structure

## Testing Strategy
- Verify the workflow file is valid YAML
- Manually trigger the fuzz job on a test branch
- Verify artifacts are uploaded correctly
- Verify PR comment is posted (for PR mode)

## Dependencies
- Task 0180 (Chunk parsing fuzz target)
- Task 0181 (Compression and encryption fuzz target)
- Task 0182 (CLI term parsing fuzz target)

## Files to Create/Modify
- `.forgejo/workflows/fuzz.yml` (create — fuzz CI workflow)
- `.forgejo/workflows/ci.yml` (modify — optionally reference fuzz workflow)

## Reference
- `spec/gap-analysis.md` — H-3
- `spec/testing.md` lines 480–487 (CI integration)
- Existing: `.forgejo/workflows/ci.yml` (current CI configuration)
- `cargo-fuzz` CI documentation

## Notes
The per-PR fuzz run is intentionally non-blocking because fuzzing is
probabilistic — a 5-minute run may not find real bugs but can catch
regressions in parsing robustness. Making it blocking would introduce
flakiness.

The nightly run has a longer time budget and is more likely to find subtle
issues. When it does find a crash:
1. The crash input is saved as a CI artifact
2. A Forgejo issue is created with the minimised crash input
3. The developer reproduces locally with `cargo fuzz run <target> <artifact>`

Corpus caching between runs is important: the fuzzer builds up a corpus of
interesting inputs over time. Clearing the corpus on each run wastes
previous exploration.

The workflow structure:

```yaml
on:
  pull_request:
    # 5-min per target, non-blocking
  schedule:
    - cron: '0 3 * * *'  # 3 AM daily
    # 30-min per target
```

The nightly schedule runs at 3 AM (server time) to avoid contention with
developer activity.
