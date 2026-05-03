# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](Https://conventionalcommits.org) for commit guidelines.

<!-- changelog -->

## [v0.2.0](https://harton.dev/project-neon/neonfs/compare/v0.1.11...v0.2.0) (2026-05-03)
### Breaking Changes:

* fuse: delete fuser NIF and dead Rust code (#662) (#664) by James Harton

* nfs: delete nfs3_server NIF and dead code (#657) (#660) by James Harton

* core: delete write_file/4 whole-binary API (#380) by James Harton



### Features:

* cli,core: cluster repair start / status command (#709) (#720) by James Harton

* core: ReplicaRepair membership-change auto-trigger (#708) (#719) by James Harton

* core: ReplicaRepair Job.Runner + ReplicaRepairScheduler (#707) (#718) by James Harton

* core: NeonFS.Core.ReplicaRepair walk + reconcile primitive (#706) (#717) by James Harton

* packaging: containerd content store image, deb, omnibus, docs (#553) (#716) by James Harton

* containerd: Status / ListStatuses / Abort RPCs + stale-partial sweep (#552) (#715) by James Harton

* containerd: Info / List / Update / Delete metadata RPCs (#551) (#714) by James Harton

* containerd: Write RPC streams uploads, verifies digest, atomic commit (#550) (#713) by James Harton

* containerd: Read RPC streams sharded sha256 blobs from NeonFS (#549) (#712) by James Harton

* containerd: neonfs_containerd package scaffold + gRPC server (#548) (#711) by James Harton

* cli: cluster ca rotate --status/--stage/--abort/--finalize (#692) (#701) by James Harton

* core: cluster ca rotate --status mode (#692) (#700) by James Harton

* core: cluster ca rotate --finalize mode (#692) (#699) by James Harton

* core: cluster ca rotate --stage mode (#692) (#698) by James Harton

* core: handle_ca_rotate --abort flow + audit log (#692) (#696) by James Harton

* client: TLSDistConfig.install_node_cert/3 atomic primitive (#692) (#695) by James Harton

* client: dual-CA bundle support in TLSDistConfig (#691) (#694) by James Harton

* core: dual-CA primitives in CertificateAuthority (#690) (#693) by James Harton

* core,cli: cluster force-reset Ra minority-recovery mutation (#473) (#689) by James Harton

* fuse: blocking SETLKW byte-range fcntl via wait queue (#681) (#686) by James Harton

* fuse: INTERRUPT opcode + cancellation of queued SETLKW (#675) (#685) by James Harton

* fuse,core: blocking flock SETLKW via wait queue (#677) (#684) by James Harton

* core: byte-range lock wait queue + cancel_wait (#679) (#683) by James Harton

* fuse: byte-range fcntl locks (GETLK / SETLK non-blocking) (#674) (#682) by James Harton

* core: byte-range advisory lock primitives in NamespaceCoordinator (#673) (#680) by James Harton

* fuse: FLOCK whole-file advisory locks via NamespaceCoordinator (#672) (#678) by James Harton

* fuse: xattr opcodes (GETXATTR / SETXATTR / LISTXATTR / REMOVEXATTR) (#671) (#676) by James Harton

* fuse: rewire MountManager through Fusermount + Session (#661) (#663) by James Harton

* nfs: flip default handler stack to :beam + Mount.Backend impl (#656) (#659) by James Harton

* nfs: ExportManager :beam handler-stack dispatch (#655) (#658) by James Harton

* fuse: pin/unpin on open/release + file_id-keyed read/write (#651) (#653) by James Harton

* core: read_file_by_id / write_file_at_by_id APIs (#650) (#652) by James Harton

* core: pin-release-triggered GC of detached files (#644) (#649) by James Harton

* core: FileMeta :detached state + delete_file pin-check (#643) (#646) by James Harton

* namespace-coordinator: claim_pinned primitive for handle-pinned files (#637) (#641) by James Harton

* nfs: NFSv3 SYMLINK + MKNOD/LINK stubs (procs 10, 11, 15) (#633) by James Harton

* nfs: NFSv3 RENAME (proc 14) (#632) by James Harton

* nfs: NFSv3 WRITE / COMMIT (procs 7, 21) (#631) by James Harton

* nfs: NFSv3 MKDIR / REMOVE / RMDIR (procs 9, 12, 13) (#630) by James Harton

* nfs: NFSv3 CREATE (proc 8) â UNCHECKED / GUARDED / EXCLUSIVE (#629) by James Harton

* nfs: NFSv3 SETATTR (proc 2) on the BEAM stack (#628) by James Harton

* docker: map `-o key=value` opts to typed Volume settings (#617) by James Harton

* cifs: upgrade `O_EXCL | O_CREAT` to claim_create (#616) by James Harton

* fuse: route `O_EXCL | O_CREAT` through claim_create (#615) by James Harton

* webdav: route `If-None-Match: *` PUTs through claim_create (#614) by James Harton

* core: WriteOperation `create_only: true` via claim_create (#613) by James Harton

* coordinator: claim_create primitive in state machine + GenServer API (#612) by James Harton

* csi,packaging: container image, omnibus bake target, release pipeline (#598) by James Harton

* csi,packaging: Helm chart for Controller Deployment and Node DaemonSet (#597) by James Harton

* csi: volume health reporting for pod scheduling (#596) by James Harton

* csi: Node service (stage/unstage + publish/unpublish with FUSE bind mount) (#581) by James Harton

* fuse: cache flushers (FLUSH/FSYNC/FSYNCDIR/FALLOCATE) + real-mount integration test (#580) by James Harton

* fuse: native-BEAM session WRITE + CREATE data-path opcodes (#579) by James Harton

* fuse: native-BEAM session SETATTR / MKDIR / UNLINK / RMDIR / RENAME / RENAME2 (#578) by James Harton

* cli,core: neonfs dr snapshot {create,list,show} CLI surface (#572) by James Harton

* core: NeonFS.Core.DRSnapshotScheduler with grandfather-father-son retention (#571) by James Harton

* csi: Controller service (volume lifecycle + publish/unpublish) (#567) by James Harton

* nfs: NeonFS.NFS.NFSv3Backend + Filehandle codec for native-BEAM stack (#566) by James Harton

* core: mkdir/delete_file race resolution via NamespaceCoordinator (#563) by James Harton

* core: claim_rename primitive + NeonFS.Core.rename_file integration (#562) by James Harton

* csi: scaffold neonfs_csi package + gRPC server + Identity service (#556) by James Harton

* cifs: scaffold neonfs_cifs Samba VFS module backend (#546) by James Harton

* nfs_server: NFSv3 READDIR and READDIRPLUS with cookie pagination (#545) by James Harton

* nfs_server: NFSv3 READ procedure with streaming-aware response (#544) by James Harton

* fuse: mount session with INIT handshake and read-path opcodes (#543) by James Harton

* core: namespace coordinator Ra state machine and primitives (#541) by James Harton

* nfs_server: NFSv3 types, Backend behaviour, and 8 metadata procedures (#536) by James Harton

* fuse_server: fusermount3 wrapper for FUSE mount and unmount (#528) by James Harton

* cli: emergency-bootstrap audit-log JSONL emission (#527) by James Harton

* cli: --new-key fresh-CA generation in emergency-bootstrap (#526) by James Harton

* integration,client: mixed-role TLS data-plane harness via join_cluster_rpc (#525) by James Harton

* cli: node cert regeneration after emergency-bootstrap install (#522) by James Harton

* cli: live-service refusal for emergency-bootstrap (#520) by James Harton

* cli: atomic install of validated CA material to $NEONFS_TLS_DIR (#519) by James Harton

* cli: emergency-bootstrap tarball validation layer (#517) by James Harton

* cli: cluster ca emergency-bootstrap surface + flag-level safety gates (#505) by James Harton

* integration: peer-cluster mixed-role peer spawning (#500) by James Harton

* client,core: ChunkWriter multi-replica fan-out via put_chunk handler (#497) by James Harton

* cli,core: neonfs cluster force-reset surface + safety gates (#496) by James Harton

* cli: neonfs cluster remove-node for permanent node decommission (#467) by James Harton

* docker: container image target and release pipeline (#392) by James Harton

* omnibus: integrate neonfs_docker into the omnibus bundle (#393) by James Harton

* docker: `/health` endpoint and HealthCheck registration (#391) by James Harton

* docker: Debian package and systemd unit for neonfs_docker (#394) by James Harton

* docker: VolumeDriver Mount/Unmount with ref-counted MountTracker (#388) by James Harton

* docker: neonfs_docker package and HTTP lifecycle endpoints (#376) by James Harton

* iam: scaffold the neonfs_iam package (#367) by James Harton

* core: persistent pending-write log for streaming write recovery (#335) by James Harton

* nfs_server: add ONC RPC v2 server framework with portmapper (#336) by James Harton

* core: lay Ra/ETS/Ash foundations for IAM (#333) by James Harton

* fuse_server: add pure-Elixir FUSE 7.31 protocol codec (#330) by James Harton

* fuse_server: add minimal /dev/fuse transport NIF with enif_select (#328) by James Harton

* cli: add escalation list/show/resolve Rust CLI commands (#273) by James Harton

* core: add decision escalation system (#265) by James Harton

* implement content-type detection for WebDAV and S3 (#173) by James Harton

* add webdav_server standalone hex package (#165) by James Harton

* nfs: implement NLM v4 protocol adapter for advisory locking (#164) by James Harton

* core: add lock recovery grace period after master node failover (#163) by James Harton

* core: add optional write-blocks-at-lock backpressure (#162) by James Harton

* core: add share mode (deny-write) enforcement to write pipeline (#159) by James Harton

* core: integrate DLM with write pipeline for mandatory lock enforcement (#156) by James Harton

* packaging: add neonfs_s3 debian package (#151) by James Harton

* s3: add credential rotate/show and bucket commands to CLI (#150) by James Harton

* s3: implement S3 credential management in neonfs_core (#148) by James Harton

* s3: add standalone container target (#144) by James Harton

* s3: add health endpoint and degraded mode handling (#143) by James Harton

* add neonfs_s3 package â S3-compatible API for NeonFS volumes (#137) by James Harton

* s3_server: implement conditional request handling (RFC 7232) (#133) by James Harton

* s3_server: add virtual-hosted-style request routing (#129) by James Harton

* add s3_server standalone S3-compatible server package (#125) by James Harton

### Bug Fixes:

* containerd: pass `start_server: true` so the gRPC listener actually starts (#736) by James Harton

* client: register `:containerd` as a known service type (#732) by James Harton

* containerd: send WriteContentResponse per WRITE frame (#730) by James Harton

* core: purge departed-node references from force-reset state (#688) (#722) by James Harton

* nfs: wait for drive registration in NFSv3BeamWriteTest (#704) (#705) by James Harton

* integration: extend DRSnapshotSchedulerTest leader-change deadline (#606) (#703) by James Harton

* integration: bump PartitionTest:42 timeout to 60s (#606) (#697) by James Harton

* anti-entropy: parallelise sync_now segment processing (#668) (#670) by James Harton

* test: bump partition-test convergence budget to 120s (#606) (#669) by James Harton

* test: wait for cross-node Ra visibility in NamespaceCoordinator tests (#666) (#667) by James Harton

* test-support: bump peer-cluster ensure_all_started timeout to 60s (#647) (#648) by James Harton

* client: preserve explicit `modified_at` / `changed_at` in FileMeta.update/2 (#636) by James Harton

* core: consistent read in Escalation.emit_pending_metrics fixes flaky test (#569) by James Harton

* integration: re-trigger anti-entropy on every poll in partition-healing tests (#568) by James Harton

* integration: raise EscalationTest:49 drain deadline 10s -> 30s (#515) by James Harton

* core,client: propagate codec through put_chunk â commit_chunks (#484) by James Harton

* integration: register drives on followers in remove_node_test setup (#476) by James Harton

* core: wrap Bandit with BanditLauncher to retry {:inet_async, :timeout} flake (#435) by James Harton

* integration: assert_connected polls for full mesh with bounded timeout (#439) by James Harton

* integration: drain stale escalation-state telemetry until pending count drops (#436) by James Harton

* integration: wait_for_ra_ready match shape `:ra_system.fetch` actually returns (#430) by James Harton

* packaging: include neonfs_webdav in build-debs.sh (#400) by James Harton

* core: write_file_streamed/4 mandatory lock and share-mode enforcement (#375) by James Harton

* core: route QuorumCoordinator-backed point reads through quorum by James Harton

* core: pin dedup test chunk strategy to eliminate `:auto` flakiness (#339) by James Harton

* core: discriminate blob paths by codec to stop cross-volume overwrites (#272) by James Harton

* integration: add bandit and thousand_island to dialyzer PLT (#262) by James Harton

* integration: retry Bandit listener bind on transient :inet_async timeouts by James Harton

* devcontainer,ci: cap BEAM port table with `+Q 65536` by James Harton

* deps: update rust crate fastcdc to v4 (#179) by Renovate Bot

* webdav: preserve dead properties across COPY and cross-volume MOVE (#197) by James Harton

* rename list_files to list_files_recursive to reflect actual semantics (#191) by James Harton

* resolve directory-only entries in get_file_meta and list_dir (#188) by James Harton

* preserve content type on WebDAV COPY/MOVE and S3 CopyObject (#180) by James Harton

* persist acl_entries and default_acl in FileIndex serialisation (#176) by James Harton

* crypto: guard key rotation against empty local replicas (#131) by James Harton

* gc: delete blob files from disk during garbage collection (#128) by James Harton

* file_index: fall back to local ETS delete when quorum write fails (#127) by James Harton

* blob: clean up temp files on write failure (#127) by James Harton

* gc: delete blob files from disk during garbage collection (#128) by James Harton

* nfs: add write backpressure and parallel chunk processing (#109) (#124) by James Harton

* nfs: include . and .. entries in readdir responses (#123) by James Harton

* nfs: return real file timestamps in directory listings (#122) by James Harton

* normalise ChunkFetcher 3-tuple return in offset write path (#121) by James Harton

### Improvements:

* namespace-coordinator: release telemetry includes claim metadata (#642) (#645) by James Harton

* test_support: foundation package + extract shared scaffolding from neonfs_integration (#605) by James Harton

* webdav: replace lock-null synthetic DLM IDs with namespace coordinator claims (#590) by James Harton

* webdav,core: migrate Depth:infinity locks to NamespaceCoordinator (#560) by James Harton

* docker: configurable mount cap and :mount_capacity health signal (#540) by James Harton

* docker: cluster-aware :cluster health check (#539) by James Harton

* client,integration: bound chunk-writer accumulator and document TLS heap term (#537) by James Harton

* core,integration: per-child start-time telemetry on NeonFS.Core.Supervisor (#513) by James Harton

* integration: drop 3-node data-plane round-trip tests to 2 nodes (#512) by James Harton

* integration: per-app start-time profiler for peer-cluster setup (#511) by James Harton

* integration: share 3-node cluster across partition-healing tests (#509) by James Harton

* s3: complete_multipart_upload concatenates chunk lists instead of reading+writing bytes (#494) by James Harton

* s3: copy_object streams source through ChunkWriter (#493) by James Harton

* s3: migrate single-file write paths off call_core(:write_file_at) via ChunkWriter (#492) by James Harton

* webdav: migrate cross-node fallback off call_core(:write_file_at) (#491) by James Harton

* integration: seed Connection.bootstrap_nodes in peer-cluster harness (#486) by James Harton

* client: NeonFS.Client.ChunkWriter.write_file_stream/4 (#480) by James Harton

* core: propagate encryption opts from resolve_put_chunk_opts/1 (#474) by James Harton

* client: add neonfs_chunker NIF crate with chunker parity test (#475) by James Harton

* core: add commit_chunks/4 RPC for pre-chunked streaming writes (#453) by James Harton

* core,client: put_chunk frame carries volume_id for core-side compression (#469) by James Harton

* integration: remove Process.sleep(1100) from intent expiry test by pre-dating started_at (#427) by James Harton

* integration: share cluster across read-only ClusterFormationTest multi-node smoke tests (#426) by James Harton

* integration: per-phase telemetry on PeerCluster.start_cluster! (#428) by James Harton

* integration: migrate test callsites from `WriteOperation.write_file/4` to `write_file_at/5` (#390) by James Harton

* integration: rename TestHelpers.write_file â write_file_from_binary (#372) by James Harton

* core: migrate neonfs_core unit tests off write_file/4 (#373) by James Harton

* core,fuse,nfs: migrate production write_file/4 callsites to streaming (#371) by James Harton

* webdav: replace in-tree webdav_server with the davy hex package (#357) by James Harton

* s3: replace in-tree s3_server with the firkin hex package (#358) by James Harton

* webdav: migrate remaining write_file callers to streaming (#334) by James Harton

* s3_server,s3: stream PutObject and UploadPart bodies (#326) by James Harton

* client: stream erasure-coded fallback one stripe at a time (#308) by James Harton

* client,s3,webdav: stream chunks to non-co-located interface nodes (#298) by James Harton

* webdav_server: cap buffered PUT bodies at configurable limit (#271) by James Harton

* webdav,webdav_server: stream PUT bodies through write_file_streamed (#261) by James Harton

* core: add `write_file_streamed/4` for replicated volumes (#259) by James Harton

* core: add incremental chunker NIF for streaming writes (#257) by James Harton

* webdav,s3,nfs,fuse: reject cross-volume copy/move (#240) by James Harton

* webdav,client: migrate WebDAV GET to ChunkReader (#230) (#237) by James Harton

* s3,client: migrate S3 GET to ChunkReader (#229) (#235) by James Harton

* nfs,client: migrate NFS reads to ChunkReader (#228) (#234) by James Harton

* fuse,client: migrate FUSE reads to ChunkReader (#227) (#233) by James Harton

* core,client: add read_file_refs + data-plane ChunkReader (#200) (#232) by James Harton

* webdav: reject collection DELETE when descendants are locked (#223) (#224) by James Harton

* renovate: batch dependency updates weekly by James Harton

* webdav: implement collection locking with Depth header (#213) (#221) by James Harton

* webdav: periodic cleanup for expired lock store entries (#220) by James Harton

* webdav: coordinate lock-null resources via DLM (#216) by James Harton

* webdav: implement distributed lock store for cross-protocol locking (#212) by James Harton

* omnibus: Add neonfs_webdav to the omnibus container and package (#211) by James Harton

* webdav: Add container and Debian package deployment for neonfs_webdav (#209) by James Harton

* Use read_file_stream in S3 and WebDAV for chunked HTTP responses (#208) by James Harton

* add seekable chunked stream API for large file reads (#205) by James Harton

* Remove separate read_file/2 facade, unify into read_file/3 (#202) by James Harton

* implement HTTP range request support for S3 and WebDAV (#198) by James Harton

* webdav: implement dead property storage via PROPPATCH (#192) by James Harton

* add WebDAV integration tests (#186) by James Harton

* consolidate NeonFS.Core RPC facade for interface nodes (#185) by James Harton

* packaging: recommend nfs-common for NFS packages by James Harton

* cli: rename `nfs mount`/`unmount` to `nfs export`/`unexport` by James Harton

### Performance Improvements:

* integration: surface slowest tests via ExUnit `:slowest` option (#382) by James Harton

## [v0.1.11](https://harton.dev/project-neon/neonfs/compare/v0.1.10...v0.1.11) (2026-04-09)




### Bug Fixes:

* deps: update rust crate md5 to 0.8 (#106) by Renovate Bot

* use TLS distribution for CLI-to-daemon connections by [@jimsynz](https://github.com/jimsynz)

## [v0.1.10](https://harton.dev/project-neon/neonfs/compare/v0.1.9...v0.1.10) (2026-04-09)




### Bug Fixes:

* send sd_notify READY=1 for Type=notify systemd services (#105) by [@jimsynz](https://github.com/jimsynz)

## [v0.1.9](https://harton.dev/project-neon/neonfs/compare/v0.1.8...v0.1.9) (2026-04-09)




### Bug Fixes:

* replace EPMD with custom NeonFS.Epmd module (#104) by [@jimsynz](https://github.com/jimsynz)

## [v0.1.8](https://harton.dev/project-neon/neonfs/compare/v0.1.7...v0.1.8) (2026-04-08)




### Bug Fixes:

* packaging: include `neonfs-tls-common.sh` in neonfs-common deb package by [@jimsynz](https://github.com/jimsynz)

## [v0.1.7](https://harton.dev/project-neon/neonfs/compare/v0.1.6...v0.1.7) (2026-04-07)




### Improvements:

* TLS distribution and secure cluster join (#98) by [@jimsynz](https://github.com/jimsynz)

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
