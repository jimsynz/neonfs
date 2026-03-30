#!/bin/sh
# Wrapper around `docker buildx bake` that reads tool versions from
# .tool-versions and exports them as env vars for bake.hcl.
#
# Usage: ./bake.sh [bake args...]
# Example: PLATFORMS=linux/amd64 ./bake.sh --load core

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TOOL_VERSIONS="${REPO_ROOT}/.tool-versions"

if [ ! -f "$TOOL_VERSIONS" ]; then
  echo "error: .tool-versions not found at ${TOOL_VERSIONS}" >&2
  exit 1
fi

export ELIXIR_VERSION=$(awk '/^elixir/ {print $2}' "$TOOL_VERSIONS")
export ERLANG_VERSION=$(awk '/^erlang/ {print $2}' "$TOOL_VERSIONS")
export RUST_VERSION=$(awk '/^rust/ {print $2}' "$TOOL_VERSIONS")

if [ -z "$ELIXIR_VERSION" ] || [ -z "$ERLANG_VERSION" ] || [ -z "$RUST_VERSION" ]; then
  echo "error: failed to parse all versions from ${TOOL_VERSIONS}" >&2
  echo "  elixir=${ELIXIR_VERSION:-<missing>} erlang=${ERLANG_VERSION:-<missing>} rust=${RUST_VERSION:-<missing>}" >&2
  exit 1
fi

cd "$REPO_ROOT" && exec docker buildx bake -f "${SCRIPT_DIR}/bake.hcl" "$@"
