#!/usr/bin/env bash
# Guard against whole-file buffering regressions.
#
# A single NeonFS volume can hold files much larger than available RAM.
# Buffering a whole file — as a binary, iolist, Vec<u8>, or any other
# "one value holding all the bytes" — will OOM the node. Every known
# instance is a correctness defect, not a performance nit.
#
# This script greps for patterns that typically indicate unbounded
# buffering. False positives can be silenced with an inline comment:
#
#     # audit:bounded <reason>      (Elixir)
#     // audit:bounded <reason>     (Rust)
#
# placed on the violating line or the line immediately above it.
#
# See AGENTS.md and CLAUDE.md for the full "No Whole-File Buffering"
# rule and https://harton.dev/project-neon/neonfs/issues/254 for the
# audit that introduced this guard.

set -u

cd "$(git rev-parse --show-toplevel)"

VIOLATIONS=0

check_pattern() {
  local description="$1"
  local pattern="$2"
  shift 2
  local globs=("$@")

  local hits
  # shellcheck disable=SC2086
  hits=$(git grep -n -E "$pattern" -- "${globs[@]}" 2>/dev/null || true)

  [ -z "$hits" ] && return 0

  while IFS= read -r line; do
    [ -z "$line" ] && continue
    local file lineno content
    file=$(printf '%s' "$line" | cut -d: -f1)
    lineno=$(printf '%s' "$line" | cut -d: -f2)
    content=$(printf '%s' "$line" | cut -d: -f3-)

    # Allow `audit:bounded` annotation on the hit line or the one above.
    local this_line prev_line
    this_line=$(sed -n "${lineno}p" "$file" 2>/dev/null || true)
    prev_line=$(sed -n "$((lineno - 1))p" "$file" 2>/dev/null || true)
    if printf '%s\n%s\n' "$prev_line" "$this_line" | grep -qE 'audit:bounded'; then
      continue
    fi

    printf '❌ %s:%s — %s\n    %s\n' "$file" "$lineno" "$description" "$content"
    VIOLATIONS=$((VIOLATIONS + 1))
  done <<< "$hits"
}

# ── Elixir ────────────────────────────────────────────────────────────────
# Plug.Conn.read_body with no :length option — reads the whole body.
check_pattern \
  'Plug.Conn.read_body/read_body without :length — body size unbounded' \
  '(Plug\.Conn\.)?read_body\(\s*conn\s*\)' \
  '*/lib/*.ex' '*/lib/**/*.ex'

# Explicit length: :infinity on read_body.
check_pattern \
  'read_body with length: :infinity — entire body buffered' \
  'read_body\(.*length:\s*:infinity' \
  '*/lib/*.ex' '*/lib/**/*.ex'

# Enum.into/reduce that collapses a stream into a single binary.
check_pattern \
  'Stream collapsed into a single binary — use streaming consumer' \
  'Enum\.(into|reduce)\([^,)]+,\s*<<>>' \
  '*/lib/*.ex' '*/lib/**/*.ex'

# stream |> Enum.to_list() — frequently a precursor to IO.iodata_to_binary.
check_pattern \
  'Stream.to_list on a file-body stream — materialises whole stream' \
  '\|>\s*Enum\.to_list\(\)\s*\|>\s*IO\.iodata_to_binary' \
  '*/lib/*.ex' '*/lib/**/*.ex'

# ── Rust ──────────────────────────────────────────────────────────────────
# Rust test files exercise the filesystem with known-size fixtures; the
# whole-file buffering rule applies to production code paths, not test
# assertion helpers.
RUST_GLOBS=('*.rs' ':!*/tests/*' ':!*/test/*' ':!*/src/tests.rs')

# std::fs::read_to_string is unbounded by design.
check_pattern \
  'std::fs::read_to_string — entire file into a String' \
  'fs::read_to_string\(' \
  "${RUST_GLOBS[@]}"

# tokio/std Read::read_to_end on an untrusted-size reader.
check_pattern \
  'read_to_end on an untrusted-size reader — allocates unbounded Vec' \
  '\.read_to_end\(' \
  "${RUST_GLOBS[@]}"

if [ "$VIOLATIONS" -gt 0 ]; then
  printf '\n%s violation(s) — add a streaming alternative or annotate with `audit:bounded <reason>`.\n' "$VIOLATIONS"
  exit 1
fi

printf '✓ no whole-file buffering patterns found\n'
exit 0
