# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](Https://conventionalcommits.org) for commit guidelines.

<!-- changelog -->

## [v0.1.6](https://harton.dev/project-neon/neonfs/compare/v0.1.5...v0.1.6) (2026-04-04)




### Bug Fixes:

* Ra election timeout and cleanup race on slow hardware by [@jimsynz](https://github.com/jimsynz)

* use long names for Erlang distribution and default to `hostname -f` by [@jimsynz](https://github.com/jimsynz)

## [v0.1.5](https://harton.dev/project-neon/neonfs/compare/v0.1.4...v0.1.5) (2026-04-02)




## [v0.1.4](https://harton.dev/project-neon/neonfs/compare/v0.1.3...v0.1.4) (2026-04-01)




### Bug Fixes:

* ci: use dynamic nfpm download URL for arm64 release builds by [@jimsynz](https://github.com/jimsynz)

## [v0.1.3](https://harton.dev/project-neon/neonfs/compare/v0.1.2...v0.1.3) (2026-04-01)




### Bug Fixes:

* ci: downgrade artifact actions to v3 for Forgejo compatibility by [@jimsynz](https://github.com/jimsynz)

## [v0.1.2](https://harton.dev/project-neon/neonfs/compare/v0.1.1...v0.1.2) (2026-04-01)




### Bug Fixes:

* core: fix two pre-existing test flakiness issues by [@jimsynz](https://github.com/jimsynz)

* core: eliminate ETS TOCTOU race conditions in concurrent write paths by [@jimsynz](https://github.com/jimsynz)

* containers: serialise cargo cache access to prevent arm64 build race by [@jimsynz](https://github.com/jimsynz)

## [v0.1.1](https://harton.dev/project-neon/neonfs/compare/v0.1.0...v0.1.1) (2026-03-30)




### Bug Fixes:

* ci: build container targets sequentially to avoid BuildKit session timeout by [@jimsynz](https://github.com/jimsynz)

### Improvements:

* replace ASDF with pre-built images for faster container builds (#90) by [@jimsynz](https://github.com/jimsynz)

## [0.1.0](https://harton.dev/project-neon/neonfs/compare/0.1.0...0.1.0) (2026-03-25)




### Features:

* add `neonfs_omnibus` all-in-one deployable package by [@jimsynz](https://github.com/jimsynz)

* Phase 5 â leaderless quorum metadata tiering by [@jimsynz](https://github.com/jimsynz)

* Phase 4 - Erasure Coding by [@jimsynz](https://github.com/jimsynz)

* Phase 3 - Tiered Storage (#18) by [@jimsynz](https://github.com/jimsynz)

* migrate CI from Drone to Forgejo Actions (#3) by Renovate Bot

* service discovery and Phase 2 integration completion by [@jimsynz](https://github.com/jimsynz)

* [0037] - Phase 2 Integration Test - Multi-Node Cluster by [@jimsynz](https://github.com/jimsynz)

* [0036] - Remote Chunk Reading by [@jimsynz](https://github.com/jimsynz)

* [0035] - Basic Replication by [@jimsynz](https://github.com/jimsynz)

* [0034] - Distributed Chunk Metadata by [@jimsynz](https://github.com/jimsynz)

* [0033] - Node Join Flow by [@jimsynz](https://github.com/jimsynz)

* [0032] - Cluster Bootstrap by [@jimsynz](https://github.com/jimsynz)

* [0031] - Ra Consensus Integration Setup by [@jimsynz](https://github.com/jimsynz)

* [0044] - Acceptance Testing and Forgejo CI by [@jimsynz](https://github.com/jimsynz)

* [0043] - Split systemd Units for Core and FUSE by [@jimsynz](https://github.com/jimsynz)

* [0041] - Fix FUSE Rust Compilation Errors by [@jimsynz](https://github.com/jimsynz)

* [0042] - Implement proper Erlang distribution for CLI-daemon communication by [@jimsynz](https://github.com/jimsynz)

* [0042] - Implement CLI RPC Server and Fix CLI Integration by [@jimsynz](https://github.com/jimsynz)

* [0040] - Implement Metadata Persistence with DETS by [@jimsynz](https://github.com/jimsynz)

* [0030] - Implement Phase 1 Integration Test by [@jimsynz](https://github.com/jimsynz)

* [0029] - Configure Elixir Release by [@jimsynz](https://github.com/jimsynz)

* [0028] - Implement systemd Integration by [@jimsynz](https://github.com/jimsynz)

* [0027] - Implement FUSE Supervision Tree by [@jimsynz](https://github.com/jimsynz)

* [0026] - Implement Core Supervision Tree by [@jimsynz](https://github.com/jimsynz)

* [0025] - Implement CLI Commands by [@jimsynz](https://github.com/jimsynz)

* [0024] - Implement CLI Handler Module in Elixir by [@jimsynz](https://github.com/jimsynz)

* [0023] - Implement CLI Daemon Connection by [@jimsynz](https://github.com/jimsynz)

* [0022] - Create neonfs-cli Rust Crate Scaffolding by [@jimsynz](https://github.com/jimsynz)

* [0021] - Implement FUSE Mount Manager by [@jimsynz](https://github.com/jimsynz)

* [0020] - Implement FUSE Operation Handler by [@jimsynz](https://github.com/jimsynz)

* [0019] - Implement Basic Read Path by [@jimsynz](https://github.com/jimsynz)

* [0018] - Implement Basic Write Path by [@jimsynz](https://github.com/jimsynz)

* [0017] - Implement Volume Configuration Structure by [@jimsynz](https://github.com/jimsynz)

* [0016] - Implement File Metadata Structure by [@jimsynz](https://github.com/jimsynz)

* [0015] - Implement Chunk Metadata Structure by [@jimsynz](https://github.com/jimsynz)

* [0013] - Implement FUSE Write Operations by [@jimsynz](https://github.com/jimsynz)

* [0012] - Implement FUSE Mount and Basic Operations by [@jimsynz](https://github.com/jimsynz)

* [0011] - Implement FUSE-Elixir Channel Communication by [@jimsynz](https://github.com/jimsynz)

* [0014] - Implement Elixir Blob Store Wrapper Module by [@jimsynz](https://github.com/jimsynz)

* [0010] - Create neonfs_fuse Rust Crate with Rustler by [@jimsynz](https://github.com/jimsynz)

* [0009] - Implement Chunk Tier Migration by [@jimsynz](https://github.com/jimsynz)

* [0008] - Implement FastCDC Content-Defined Chunking by [@jimsynz](https://github.com/jimsynz)

* [0007] - Implement Chunk Compression by [@jimsynz](https://github.com/jimsynz)

* [0006] - Implement Chunk Verification on Read by [@jimsynz](https://github.com/jimsynz)

* [0005] - Implement Blob Store Read/Write Operations by [@jimsynz](https://github.com/jimsynz)

* [0004] - Implement Blob Store Directory Layout by James Harton

* [0003] - Implement SHA-256 Hashing Module by James Harton

* [0002] - Configure .check.exs with Rust Tool Integration by James Harton

* [0001] - Create neonfs_blob Rust Crate with Rustler by James Harton

### Bug Fixes:

* core: variable shadowing in ServiceRegistry.do_deregister/3 by [@jimsynz](https://github.com/jimsynz)

* nfs: stale MetadataCache, broken root listing, and write truncation (#75) by Renovate Bot

* deps: update rust crate erl_rpc to 0.4 (#74) by Renovate Bot

* nfs: pass volume ID instead of name to core operations by [@jimsynz](https://github.com/jimsynz)

* sync volume ETS cache across cluster via event subscription by [@jimsynz](https://github.com/jimsynz)

* prevent crash when restoring DETS into missing ETS tables by [@jimsynz](https://github.com/jimsynz)

* make `neonfs_client` a proper OTP application to fix omnibus crash by [@jimsynz](https://github.com/jimsynz)

* race condition in drive worker removal test by [@jimsynz](https://github.com/jimsynz)

* update Ra API calls for v3.0.2 compatibility by [@jimsynz](https://github.com/jimsynz)

* cluster: stabilise full-mesh service registration by Renovate Bot

* distribution: handle  tuple variants and align cookie setup by [@jimsynz](https://github.com/jimsynz)

* show full node name (including hostname) in CLI table output by [@jimsynz](https://github.com/jimsynz)

* resolve Credo nesting and logger metadata warnings in `PoolManager` by [@jimsynz](https://github.com/jimsynz)

* detect endpoint changes in `PoolManager` discovery refresh by [@jimsynz](https://github.com/jimsynz)

* increase HEALTHCHECK `--start-period` to 120s by [@jimsynz](https://github.com/jimsynz)

* handle `BigInteger` in CLI term conversions by [@jimsynz](https://github.com/jimsynz)

* prevent Formation orphan detection false positive on fresh deploy by [@jimsynz](https://github.com/jimsynz)

* pin `data_size` in `BlobStoreTest` telemetry `assert_receive` patterns by Renovate Bot

* deps: update rust crate rand to 0.10 (#36) by Renovate Bot

* deps: update rust crate fuser to 0.17 (#35) by Renovate Bot

* deps: update rust crate eetf to v0.11.0 (#15) by [@jimsynz](https://github.com/jimsynz)

* deps: pin `eetf` to =0.8 to prevent incompatible upgrades by Renovate Bot

* deps: update rust crate fuser to 0.16 (#11) by [@jimsynz](https://github.com/jimsynz)

* resolve test failures and credo warnings by [@jimsynz](https://github.com/jimsynz)

* Clean up temporary DETS files from failed snapshots by [@jimsynz](https://github.com/jimsynz)

* Remove premature tier directory creation and add systemd validation by [@jimsynz](https://github.com/jimsynz)

### Improvements:

* universal health check system for all node types by [@jimsynz](https://github.com/jimsynz)

* nfs: deterministic hash-based inode allocation and per-volume `fsid` by [@jimsynz](https://github.com/jimsynz)

* containers: add OCI labels and remove unused CI image by [@jimsynz](https://github.com/jimsynz)

* cli: rename mount to fuse and add nfs subcommands by Renovate Bot

* push container images to GHCR mirror by [@jimsynz](https://github.com/jimsynz)

* implement `neonfs node list` with real cluster data by [@jimsynz](https://github.com/jimsynz)

* wire `neonfs node status` to HealthCheck, add container HEALTHCHECK by [@jimsynz](https://github.com/jimsynz)

* add `--all` flag to `volume list` to include system volumes by [@jimsynz](https://github.com/jimsynz)

* cluster-wide queries for gc, scrub, mount, audit, and worker status by [@jimsynz](https://github.com/jimsynz)

* cluster-wide `drive list` with `--node` filter and auto-detect capacity by [@jimsynz](https://github.com/jimsynz)

* add cluster rebalance job for balancing storage across drives by [@jimsynz](https://github.com/jimsynz)

* add drive evacuation with `StorageMetrics` and `:draining` state by [@jimsynz](https://github.com/jimsynz)

* add runtime drive management via `cluster.json` by [@jimsynz](https://github.com/jimsynz)

* add human-readable capacity suffixes and partition validation to `NEONFS_DRIVES` by [@jimsynz](https://github.com/jimsynz)

* peer-based integration tests and CI pipeline overhaul (#2) by [@jimsynz](https://github.com/jimsynz)
