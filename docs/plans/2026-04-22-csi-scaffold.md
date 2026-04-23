# CSI Driver Package Scaffold Implementation Plan

- Date: 2026-04-22
- Scope: new package `neonfs_csi/` — first slice of [#244](https://harton.dev/project-neon/neonfs/issues/244)
- Issue: [#313](https://harton.dev/project-neon/neonfs/issues/313)

## Scope

Package skeleton + gRPC server + CSI Identity service. This is the "does it start and answer a Probe" milestone; Controller and Node services are separate sub-issues (#314, #315).

## Package shape

Follows the template established by `neonfs_docker` (the most recently landed interface package) and documented in the [Codebase Patterns](https://harton.dev/project-neon/neonfs/wiki/Codebase-Patterns) wiki under "Package shape for new interface packages". Copy, don't reinvent.

```
neonfs_csi/
├── mix.exs                   # deps: neonfs_client, grpc, protobuf
├── config/config.exs
├── lib/neon_fs/csi/
│   ├── application.ex        # start_link → HealthCheck.register → notify_ready
│   ├── supervisor.ex         # VolumeStore → Registrar(type: :csi) → gRPC listener
│   ├── health_check.ex       # probe readiness
│   ├── identity.ex           # Identity service impl (this issue)
│   └── config.ex             # mode detection (controller / node)
├── proto/
│   └── csi.proto             # vendored from github.com/container-storage-interface/spec v1.9
├── priv/generated/           # protoc-elixir output; checked in to avoid compile-time protoc dep
├── rel/
│   ├── env.sh.eex            # copy from neonfs_nfs; cookie + optional TLS dist
│   └── vm.args.eex
└── test/
```

## Mode detection

CSI deployments run the same binary as two pod types:

- **Controller** — typically one replica per cluster; handles volume lifecycle RPCs. Socket at `/var/lib/csi/sockets/pluginproxy/csi.sock`.
- **Node** — DaemonSet, one per cluster node; handles mount/unmount RPCs. Socket at `/var/lib/kubelet/plugins/neonfs.csi.harton.dev/csi.sock`.

Mode picks which services are registered with the gRPC server. Both run Identity.

Selection:

1. Explicit `NEONFS_CSI_MODE` env var (`controller` or `node`) — wins if set.
2. Otherwise, if `/var/lib/kubelet` exists on the host, default to `node`; else `controller`.
3. Sidecar-friendly: accept `--mode` CLI flag that passes through to the release script.

Configured in `NeonFS.CSI.Config.mode/0`, memoised at startup.

## Proto generation

- Vendor `csi.proto` from the v1.9 release of `github.com/container-storage-interface/spec`. Pin the commit hash in a short `proto/README.md`.
- Generate Elixir with `protoc-gen-elixir` at build time via a `mix.exs` task, OR check the generated files into `priv/generated/` to avoid the build-time protoc dep. Prefer the latter — matches how other Elixir projects in this space (e.g. `event_relay`) do it, and keeps the build hermetic.
- Regeneration script: `scripts/regen-csi-proto.sh`, same shape as `scripts/regen-cli-reference.sh` (added in PR #396). Devs run it manually; CI doesn't.

## gRPC server

- Use the `grpc` hex package (built on top of `mint_http2`).
- Listen on a Unix socket — same transport shape as `neonfs_docker`'s plug (Bandit on a Unix socket). CSI requires Unix-socket transport specifically; don't expose TCP.
- Socket path from config, default per-mode as above.
- Interceptors: logging + telemetry (`[:neonfs, :csi, :rpc, :start | :stop | :exception]`).

## Identity service

Three RPCs:

- `GetPluginInfo` — returns `{name, vendor_version, manifest}`. Name `neonfs.csi.harton.dev`. Version from `Mix.Project.config()[:version]`. Manifest includes `url` to the repo and the current commit SHA if available.
- `GetPluginCapabilities` — Controller mode returns `CONTROLLER_SERVICE` + `VOLUME_ACCESSIBILITY_CONSTRAINTS` (future-proofs topology-aware scheduling). Node mode returns `UNKNOWN` for controller capabilities (the Node process doesn't advertise any).
- `Probe` — returns `{ready: true}` once supervisor is fully started and `NeonFS.Client.Discovery` has reached any core node. Returns `{ready: false}` otherwise (kubelet retries).

## Supervision

`NeonFS.CSI.Supervisor`, strategy `:one_for_one`:

1. `VolumeStore` — ETS-backed map of `volume_id → volume metadata`. (Scaffold; Controller populates it later.)
2. `NeonFS.Client.Registrar` with `type: :csi`, `metadata: %{mode: mode, socket_path: path}`.
3. gRPC listener. In Controller mode, register Identity + (stub) Controller; in Node mode, register Identity + (stub) Node. Controller/Node service impls are empty stubs in this PR — the next sub-issues fill them in.

`NeonFS.CSI.Application.start/2`:

```elixir
def start(_type, _args) do
  {:ok, sup} = NeonFS.CSI.Supervisor.start_link([])
  NeonFS.CSI.HealthCheck.register_checks()

  unless Application.spec(:neonfs_omnibus) do
    NeonFS.Systemd.notify_ready()
  end

  {:ok, sup}
end
```

## Config surface

`config/runtime.exs` reads:

- `NEONFS_CSI_MODE` (`controller` | `node`) — default from `Config.mode/0`.
- `NEONFS_CSI_SOCKET_PATH` — defaults per-mode.
- `NEONFS_CSI_CORE_NODE` — bootstrap core node, same as every interface's `NEONFS_CORE_NODE`.
- Cookie / TLS dist via the standard env.sh pattern.

## Tests

1. **Per-RPC unit tests** using a `grpc_test` client against an in-process server (the `grpc` package supports this).
2. **Mode detection** — `Config.mode/0` honours env var, directory, and CLI flag priority.
3. **`grpc_health_probe` smoke test** (`@moduletag :grpc_health_probe`): the binary is trivially installed via `go install`; CI tests tag it so the probe binary's presence gates the test. Asserts `Probe` returns ready.

## CI

Add `neonfs_csi` to `.forgejo/workflows/ci.yml` as a new matrix entry mirroring `neonfs_docker`. The `runs-on: docker` runner has Elixir + Rust; no extra infra needed for the scaffold. Later sub-issues (#319, K8s integration test) will add kind/k3d.

## Out of scope — hard lines

- Controller RPCs (`CreateVolume`, `DeleteVolume`, `ControllerPublishVolume`, etc.) — #314.
- Node RPCs (`NodeStageVolume`, `NodePublishVolume`, etc.) — #315.
- Topology / volume health — #316.
- Helm chart — #317.
- Packaging + container image — #318.
- kind/k3d integration test — #319.

## Open points

1. **Protobuf generator pin.** `protoc-gen-elixir` is released occasionally; pin the version used to regenerate in `proto/README.md` so regeneration is reproducible.
2. **`grpc` hex package vs `grpc-elixir` fork.** The fork has slightly better performance but more dependencies. Start with `grpc`; revisit if benchmarks show the RPC overhead matters for Controller workloads (unlikely — CSI RPCs are small and infrequent).
3. **TLS-to-kubelet.** CSI spec permits kubelet talking to the Node plugin over TLS. Early versions of kubelet only do plaintext Unix sockets; the TLS path adds complexity with little payoff. Defer until a real deployment demands it.

## References

- Issue: [#313](https://harton.dev/project-neon/neonfs/issues/313)
- Parent: [#244](https://harton.dev/project-neon/neonfs/issues/244)
- Template: `neonfs_docker/` (newest interface package), `neonfs_omnibus/` (optional dep pattern)
- CSI spec: https://github.com/container-storage-interface/spec (pin v1.9 `csi.proto`)
- Wiki pattern: [Codebase Patterns — Package shape for new interface packages](https://harton.dev/project-neon/neonfs/wiki/Codebase-Patterns)
