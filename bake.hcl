group "default" {
  targets = ["dev", "core", "fuse", "cli"]
}

variable "TAG" {
  default = "latest"
}

variable "PLATFORMS" {
  default = "linux/amd64,linux/arm64"
}

target "base" {
  dockerfile = "containers/Containerfile.base"
  platforms  = split(",", PLATFORMS)
  tags       = ["forgejo.dmz/project-neon/neonfs/base:${TAG}"]
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/base:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/base:latest"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/base:${TAG},mode=max"]
}

target "dev" {
  dockerfile = "containers/Containerfile.dev"
  platforms  = split(",", PLATFORMS)
  tags       = ["forgejo.dmz/project-neon/neonfs/dev:${TAG}"]
  contexts = {
    "base": "target:base"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/dev:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/dev:latest"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/dev:${TAG},mode=max"]
}

target "core" {
  dockerfile = "containers/Containerfile.core"
  platforms  = split(",", PLATFORMS)
  tags       = ["forgejo.dmz/project-neon/neonfs/core:${TAG}"]
  contexts = {
    "client": "./neonfs_client"
    "src": "./neonfs_core"
    "base": "target:base"
    "cli": "target:cli"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/core:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/core:latest"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/core:${TAG},mode=max"]
}

target "fuse" {
  dockerfile = "containers/Containerfile.fuse"
  platforms  = split(",", PLATFORMS)
  tags       = ["forgejo.dmz/project-neon/neonfs/fuse:${TAG}"]
  contexts = {
    "client": "./neonfs_client"
    "src": "./neonfs_fuse"
    "base": "target:base"
    "cli": "target:cli"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/fuse:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/fuse:latest"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/fuse:${TAG},mode=max"]
}

target "cli" {
  dockerfile = "containers/Containerfile.cli"
  platforms  = split(",", PLATFORMS)
  tags       = ["forgejo.dmz/project-neon/neonfs/cli:${TAG}"]
  contexts = {
    "src": "./neonfs-cli"
    "base": "target:base"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/cli:${TAG}","type=registry,ref=forgejo.dmz/cache/neonfs/cli:latest"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/cli:${TAG},mode=max"]
}
