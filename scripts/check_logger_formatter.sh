#!/usr/bin/env bash
# Guard against the two Logger formatter misconfigurations that keep
# regressing.
#
# Both are silent: the project compiles, boots, and only misbehaves at the
# moment a log line is rendered — which for the first one means exactly the
# lines you need when diagnosing a failure arrive destroyed.
#
# 1. `formatter: {Logger.Formatter, [keyword]}`
#
#    `Logger.Formatter.format/2` only matches a `%Logger.Formatter{}` struct.
#    `Logger.App` normalises `:default_formatter` into one but passes a
#    `:default_handler` `:formatter` value through to `:logger` untouched, so
#    the tuple form raises on every format attempt and OTP reports
#    `FORMATTER CRASH` with all metadata dropped. Regressed twice before this
#    check existed.
#
# 2. Setting both `:default_formatter` and `:default_handler` in one file
#
#    `:default_handler`'s formatter wins, so the `:default_formatter` list —
#    which is what Credo's `MissedMetadataKeyInLoggerConfig` reads — becomes
#    decorative. A key declared there and missing from the handler's copy is
#    reported as configured and dropped at render. Three packages had drifted
#    this way before it was noticed.
#
# The fix for both is the same: configure metadata under
# `:default_formatter` alone. `runtime.exs` is exempt — it sets
# `:default_handler` deliberately, with `LoggerJSON.Formatters.Basic.new/1`,
# which is already a struct.
#
# See AGENTS.md and the Codebase Patterns wiki page.

set -u

cd "$(git rev-parse --show-toplevel)"

VIOLATIONS=0

report() {
  printf '❌ %s\n    %s\n' "$1" "$2"
  VIOLATIONS=$((VIOLATIONS + 1))
}

# ── 1. The tuple form, anywhere ───────────────────────────────────────────
# Not restricted to config/: it is wrong wherever it appears.
while IFS= read -r hit; do
  [ -z "$hit" ] && continue
  file=$(printf '%s' "$hit" | cut -d: -f1)
  lineno=$(printf '%s' "$hit" | cut -d: -f2)
  content=$(printf '%s' "$hit" | cut -d: -f3- | sed 's/^[[:space:]]*//')
  report "$file:$lineno — formatter must be built with Logger.Formatter.new/1, not a tuple" \
    "$content"
done <<< "$(git grep -n -F '{Logger.Formatter,' -- '*.ex' '*.exs' 2>/dev/null || true)"

# ── 2. Both formatter keys in one config file ─────────────────────────────
# `git grep -l` twice and intersect, so this stays a pure grep with no
# per-file shelling out.
both=$(comm -12 \
  <(git grep -l -F 'config :logger, :default_formatter' -- '*/config/config.exs' 2>/dev/null | sort) \
  <(git grep -l -F 'config :logger, :default_handler' -- '*/config/config.exs' 2>/dev/null | sort))

while IFS= read -r file; do
  [ -z "$file" ] && continue
  report "$file — sets both :default_formatter and :default_handler" \
    ":default_handler silently wins; declare metadata under :default_formatter alone"
done <<< "$both"

if [ "$VIOLATIONS" -gt 0 ]; then
  printf '\n%s Logger formatter violation(s) found.\n' "$VIOLATIONS"
  exit 1
fi

printf '✅ No Logger formatter misconfigurations found.\n'
