#!/usr/bin/env bash
# Regenerate docs/cli-reference.md from the actual `neonfs` --help output.
#
# Usage: scripts/regen-cli-reference.sh
#
# Builds the release binary of neonfs-cli if necessary, then walks every
# top-level command and subcommand, capturing --help for each and formatting
# them as nested markdown sections.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CLI_DIR="${REPO_ROOT}/neonfs-cli"
OUT="${REPO_ROOT}/docs/cli-reference.md"

(cd "${CLI_DIR}" && cargo build --release --quiet)
CLI="${CLI_DIR}/target/release/neonfs-cli"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

top_cmds() {
  "$CLI" --help 2>&1 \
    | awk '/^Commands:/,/^Options:/' \
    | grep -E "^  [a-z]" \
    | awk '{print $1}' \
    | grep -v '^help$'
}

sub_cmds() {
  "$CLI" "$1" --help 2>&1 \
    | awk '/^Commands:/,/^Options:/' \
    | grep -E "^  [a-z]" \
    | awk '{print $1}' \
    | grep -v '^help$' || true
}

{
  echo "# NeonFS CLI reference"
  echo ""
  echo "Auto-generated from \`neonfs --help\` (clap). Regenerate with \`scripts/regen-cli-reference.sh\`."
  echo ""
  echo "The \`neonfs\` binary is a Rust command-line client that talks to the core cluster over Erlang distribution. Every command accepts \`--output json\` or \`--json\` for machine-readable output (default is a table)."
  echo ""
  echo "## Top-level"
  echo ""
  echo '```'
  "$CLI" --help 2>&1
  echo '```'
  echo ""

  while IFS= read -r cmd; do
    echo "## \`neonfs $cmd\`"
    echo ""
    echo '```'
    "$CLI" "$cmd" --help 2>&1
    echo '```'
    echo ""
    while IFS= read -r sub; do
      [ -z "$sub" ] && continue
      echo "### \`neonfs $cmd $sub\`"
      echo ""
      echo '```'
      "$CLI" "$cmd" "$sub" --help 2>&1
      echo '```'
      echo ""
    done < <(sub_cmds "$cmd")
  done < <(top_cmds)
} > "$tmp"

mv "$tmp" "$OUT"
echo "Regenerated $OUT ($(wc -l < "$OUT") lines)"
