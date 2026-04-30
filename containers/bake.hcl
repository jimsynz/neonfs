group "default" {
  targets = ["core", "fuse", "nfs", "s3", "webdav", "docker", "csi", "containerd", "omnibus", "cli"]
}

variable "TAG" {
  default = "latest"
}

variable "PLATFORMS" {
  default = "linux/amd64,linux/arm64"
}

# Tool versions — set via env vars parsed from .tool-versions by bake.sh
variable "ELIXIR_VERSION" {
  default = ""
}

variable "ERLANG_VERSION" {
  default = ""
}

variable "RUST_VERSION" {
  default = ""
}

target "base" {
  dockerfile = "containers/Containerfile.base"
  platforms  = split(",", PLATFORMS)
  tags       = ["forgejo.dmz/project-neon/neonfs/base:${TAG}"]
  args = {
    ELIXIR_VERSION = ELIXIR_VERSION
    ERLANG_VERSION = ERLANG_VERSION
    RUST_VERSION   = RUST_VERSION
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/base:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/base:main"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/base:${TAG},mode=max,ignore-error=true"]
}

target "core" {
  dockerfile = "containers/Containerfile.core"
  platforms  = split(",", PLATFORMS)
  tags       = [
    "forgejo.dmz/project-neon/neonfs/core:${TAG}",
    "ghcr.io/jimsynz/neonfs/core:${TAG}"
  ]
  contexts = {
    "client": "./neonfs_client"
    "src": "./neonfs_core"
    "base": "target:base"
    "cli": "target:cli"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/core:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/core:main"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/core:${TAG},mode=max,ignore-error=true"]
}

target "fuse" {
  dockerfile = "containers/Containerfile.fuse"
  platforms  = split(",", PLATFORMS)
  tags       = [
    "forgejo.dmz/project-neon/neonfs/fuse:${TAG}",
    "ghcr.io/jimsynz/neonfs/fuse:${TAG}"
  ]
  contexts = {
    "client": "./neonfs_client"
    "src": "./neonfs_fuse"
    "base": "target:base"
    "cli": "target:cli"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/fuse:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/fuse:main"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/fuse:${TAG},mode=max,ignore-error=true"]
}

target "nfs" {
  dockerfile = "containers/Containerfile.nfs"
  platforms  = split(",", PLATFORMS)
  tags       = [
    "forgejo.dmz/project-neon/neonfs/nfs:${TAG}",
    "ghcr.io/jimsynz/neonfs/nfs:${TAG}"
  ]
  contexts = {
    "client": "./neonfs_client"
    "src": "./neonfs_nfs"
    "base": "target:base"
    "cli": "target:cli"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/nfs:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/nfs:main"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/nfs:${TAG},mode=max,ignore-error=true"]
}

target "s3" {
  dockerfile = "containers/Containerfile.s3"
  platforms  = split(",", PLATFORMS)
  tags       = [
    "forgejo.dmz/project-neon/neonfs/s3:${TAG}",
    "ghcr.io/jimsynz/neonfs/s3:${TAG}"
  ]
  contexts = {
    "client": "./neonfs_client"
    "src": "./neonfs_s3"
    "base": "target:base"
    "cli": "target:cli"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/s3:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/s3:main"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/s3:${TAG},mode=max,ignore-error=true"]
}

target "webdav" {
  dockerfile = "containers/Containerfile.webdav"
  platforms  = split(",", PLATFORMS)
  tags       = [
    "forgejo.dmz/project-neon/neonfs/webdav:${TAG}",
    "ghcr.io/jimsynz/neonfs/webdav:${TAG}"
  ]
  contexts = {
    "client": "./neonfs_client"
    "src": "./neonfs_webdav"
    "base": "target:base"
    "cli": "target:cli"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/webdav:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/webdav:main"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/webdav:${TAG},mode=max,ignore-error=true"]
}

target "docker" {
  dockerfile = "containers/Containerfile.docker"
  platforms  = split(",", PLATFORMS)
  tags       = [
    "forgejo.dmz/project-neon/neonfs/docker:${TAG}",
    "ghcr.io/jimsynz/neonfs/docker:${TAG}"
  ]
  contexts = {
    "client": "./neonfs_client"
    "src": "./neonfs_docker"
    "base": "target:base"
    "cli": "target:cli"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/docker:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/docker:main"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/docker:${TAG},mode=max,ignore-error=true"]
}

target "csi" {
  dockerfile = "containers/Containerfile.csi"
  platforms  = split(",", PLATFORMS)
  tags       = [
    "forgejo.dmz/project-neon/neonfs/csi:${TAG}",
    "ghcr.io/jimsynz/neonfs/csi:${TAG}"
  ]
  contexts = {
    "client": "./neonfs_client"
    "src": "./neonfs_csi"
    "base": "target:base"
    "cli": "target:cli"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/csi:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/csi:main"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/csi:${TAG},mode=max,ignore-error=true"]
}

target "containerd" {
  dockerfile = "containers/Containerfile.containerd"
  platforms  = split(",", PLATFORMS)
  tags       = [
    "forgejo.dmz/project-neon/neonfs/containerd:${TAG}",
    "ghcr.io/jimsynz/neonfs/containerd:${TAG}"
  ]
  contexts = {
    "client": "./neonfs_client"
    "src": "./neonfs_containerd"
    "base": "target:base"
    "cli": "target:cli"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/containerd:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/containerd:main"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/containerd:${TAG},mode=max,ignore-error=true"]
}

target "omnibus" {
  dockerfile = "containers/Containerfile.omnibus"
  platforms  = split(",", PLATFORMS)
  tags       = [
    "forgejo.dmz/project-neon/neonfs/omnibus:${TAG}",
    "ghcr.io/jimsynz/neonfs/omnibus:${TAG}"
  ]
  contexts = {
    "client": "./neonfs_client"
    "core": "./neonfs_core"
    "fuse": "./neonfs_fuse"
    "nfs": "./neonfs_nfs"
    "s3": "./neonfs_s3"
    "webdav": "./neonfs_webdav"
    "docker": "./neonfs_docker"
    "containerd": "./neonfs_containerd"
    "src": "./neonfs_omnibus"
    "base": "target:base"
    "cli": "target:cli"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/omnibus:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/omnibus:main"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/omnibus:${TAG},mode=max,ignore-error=true"]
}

target "cli" {
  dockerfile = "containers/Containerfile.cli"
  platforms  = split(",", PLATFORMS)
  tags       = [
    "forgejo.dmz/project-neon/neonfs/cli:${TAG}",
    "ghcr.io/jimsynz/neonfs/cli:${TAG}"
  ]
  contexts = {
    "src": "./neonfs-cli"
  }
  args = {
    RUST_VERSION = RUST_VERSION
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/cli:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/cli:main"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/cli:${TAG},mode=max,ignore-error=true"]
}
