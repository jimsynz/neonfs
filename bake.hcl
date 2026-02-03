group "default" {
  targets = ["dev", "core", "fuse", "cli"]
}

variable "TAG" {
  default = "latest"
}

variable "PLATFORMS" {
  type = list(string)
  default = ["linux/amd64", "linux/arm64"]
}

target "base" {
  dockerfile = "containers/Containerfile.base"
  platforms  = [for platform in PLATFORMS: "${platform}"]
  tags       = ["forgejo.dmz/project-neon/neonfs/base:${TAG}"]
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/base:${TAG}"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/base:${TAG},mode=max"]
}

target "dev" {
  dockerfile = "containers/Containerfile.dev"
  platforms  = [for platform in PLATFORMS: "${platform}"]
  tags       = ["forgejo.dmz/project-neon/neonfs/dev:${TAG}"]
  contexts = {
    "base": "target:base"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/dev:${TAG}"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/dev:${TAG},mode=max"]
}

target "builder" {
  dockerfile = "containers/Containerfile.builder"
  platforms  = [for platform in PLATFORMS: "${platform}"]
  tags       = ["forgejo.dmz/project-neon/neonfs/builder:${TAG}"]
  contexts = {
    "base": "target:base"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/builder:${TAG}"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/builder:${TAG},mode=max"]
}

target "core" {
  dockerfile = "containers/Containerfile.core"
  platforms  = [for platform in PLATFORMS: "${platform}"]
  tags       = ["forgejo.dmz/project-neon/neonfs/core:${TAG}"]
  contexts = {
    "src": "./neonfs_core"
    "builder": "target:builder"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/core:${TAG}"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/core:${TAG},mode=max"]
}

target "fuse" {
  dockerfile = "containers/Containerfile.fuse"
  platforms  = [for platform in PLATFORMS: "${platform}"]
  tags       = ["forgejo.dmz/project-neon/neonfs/fuse:${TAG}"]
  contexts = {
    "core": "./neonfs_core"
    "src": "./neonfs_fuse"
    "builder": "target:builder"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/fuse:${TAG}"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/fuse:${TAG},mode=max"]
}

target "cli" {
  dockerfile = "containers/Containerfile.cli"
  platforms  = [for platform in PLATFORMS: "${platform}"]
  tags       = ["forgejo.dmz/project-neon/neonfs/cli:${TAG}"]
  contexts = {
    "src": "./neonfs-cli"
    "builder": "target:builder"
  }
  cache-from = ["type=registry,ref=forgejo.dmz/cache/neonfs/cli:${TAG}"]
  cache-to   = ["type=registry,ref=forgejo.dmz/cache/neonfs/cli:${TAG},mode=max"]
}
