#!/usr/bin/env bash
# Regenerate docs/cli-reference.md from the actual `neonfs` --help output.
#
# Usage:
#   scripts/regen-cli-reference.sh            # rewrite the committed file
#   scripts/regen-cli-reference.sh --check    # fail if it is out of date
#
# Builds the release binary of neonfs-cli if necessary, then walks every
# top-level command and subcommand, capturing --help for each and formatting
# them as nested markdown sections.
#
# `--check` exists because a generated file with nothing verifying it is right
# only until the first person forgets, and then silently wrong: `neonfs
# backup` and `neonfs dr` were missing entirely for long enough that nobody
# noticed, and the file gave no hint because every section in it was correct.
# Generation and checking share this script so there is one definition of the
# format — a separate checker would drift from the generator it polices.

set -euo pipefail

check_only=false
case "${1:-}" in
  "") ;;
  --check) check_only=true ;;
  *)
    echo "usage: $(basename "$0") [--check]" >&2
    exit 2
    ;;
esac

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

if [ "$check_only" = true ]; then
  if diff -u "$OUT" "$tmp" > /dev/null 2>&1; then
    echo "✓ $(realpath --relative-to="$REPO_ROOT" "$OUT") is up to date"
    exit 0
  fi

  # The diff is printed because the useful question is *what* drifted — a
  # bare "out of date" leaves a contributor regenerating blind to see whether
  # the change was theirs.
  echo "❌ $(realpath --relative-to="$REPO_ROOT" "$OUT") is out of date." >&2
  echo >&2
  diff -u --label "committed" "$OUT" --label "generated" "$tmp" >&2 || true
  echo >&2
  echo "Regenerate it with: scripts/regen-cli-reference.sh" >&2
  exit 1
fi

mv "$tmp" "$OUT"
echo "Regenerated $OUT ($(wc -l < "$OUT") lines)"
