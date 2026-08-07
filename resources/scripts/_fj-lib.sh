# Shared library for fj-* scripts. Source from each script:
#   source "$(dirname "$0")/_fj-lib.sh"
#
# Provides:
#   FJ_TOKEN  — Forgejo API token (from ~/.local/share/forgejo-cli/keys.json)
#   FJ_HOST   — Forgejo host (default: harton.dev, override via env)
#   FJ_REPO   — owner/name (default: project-neon/neonfs, override via env)
#   FJ_API    — base API URL: https://$FJ_HOST/api/v1/repos/$FJ_REPO
#   fj_curl   — curl wrapper that injects auth, dies on HTTP >= 400
#   fj_json   — fj_curl plus "the body must parse as JSON"
#   fj_die    — print to stderr and exit 1
#
# Both wrappers bound each request with --max-time and retry transient
# failures (connection errors, 429, 5xx) before giving up, so a blip on the
# instance does not abort a caller that is mid-poll. Tunable via
# FJ_MAX_TIME, FJ_RETRIES and FJ_RETRY_DELAY.

set -euo pipefail

: "${FJ_HOST:=harton.dev}"
: "${FJ_REPO:=project-neon/neonfs}"
: "${FJ_MAX_TIME:=45}"
: "${FJ_RETRIES:=3}"
: "${FJ_RETRY_DELAY:=5}"

# Budget for endpoints that make the server *do* something — merge a branch,
# create a PR or issue — rather than read a record back. One budget for every
# endpoint means the cheap calls set it and the expensive one is the first to
# fail: with the instance answering a plain GET in ~35s, every merge blew past
# the 45s read budget, curl aborted, and the wrapper reported HTTP 000 — which
# is indistinguishable from the server refusing. Three merges "failed" that
# way before the fourth, with this budget, went through.
: "${FJ_WRITE_MAX_TIME:=300}"

FJ_API="https://$FJ_HOST/api/v1/repos/$FJ_REPO"

if [ -z "${FJ_TOKEN:-}" ]; then
  FJ_KEYS="$HOME/.local/share/forgejo-cli/keys.json"
  if [ ! -r "$FJ_KEYS" ]; then
    echo "fj: cannot read $FJ_KEYS — run 'fj login' first" >&2
    exit 1
  fi
  FJ_TOKEN=$(jq -r --arg host "$FJ_HOST" '.hosts[$host].token // empty' "$FJ_KEYS")
  if [ -z "$FJ_TOKEN" ]; then
    echo "fj: no token for $FJ_HOST in $FJ_KEYS" >&2
    exit 1
  fi
fi
export FJ_TOKEN FJ_HOST FJ_REPO FJ_API

fj_die() {
  echo "fj: $*" >&2
  exit 1
}

# fj_curl <method> <path-or-url> [curl-args...]
# Path may be absolute (https://...) or repo-relative (/issues/123).
# Repo-relative paths are joined onto $FJ_API.
# Body for POST/PATCH: pipe JSON on stdin or pass --data via curl-args.
#
# Retries transient failures — a connection error (curl exit, reported as
# HTTP 000), 429, or any 5xx — up to $FJ_RETRIES times. A 4xx other than 429
# is the server answering, so it fails immediately.
#
# NOTE for POST/PATCH callers: a retry re-sends the request, and a request
# whose *response* was lost has still been applied. Only retry-safe verbs
# should rely on this; `fj-pr-create` and friends must still check whether
# the resource exists before retrying by hand.
fj_curl() {
  local method="$1"
  local target="$2"
  shift 2

  local url
  case "$target" in
    https://*) url="$target" ;;
    /*)        url="$FJ_API$target" ;;
    *)         url="$FJ_API/$target" ;;
  esac

  local body status attempt=1
  body=$(mktemp)

  while :; do
    status=$(curl -sS -m "$FJ_MAX_TIME" -w '%{http_code}' -o "$body" \
      -X "$method" \
      -H "Authorization: token $FJ_TOKEN" \
      -H "Content-Type: application/json" \
      -H "Accept: application/json" \
      "$@" \
      "$url" 2>/dev/null) || status=000

    if [ "$status" -lt 400 ] && [ "$status" != "000" ]; then
      cat "$body"
      rm -f "$body"
      return 0
    fi

    if ! fj_transient_status "$status" || [ "$attempt" -ge "$FJ_RETRIES" ]; then
      echo "fj: $method $url → HTTP $status" >&2
      cat "$body" >&2
      echo >&2
      rm -f "$body"
      return 1
    fi

    echo "fj: $method $url → HTTP $status, retrying ($attempt/$FJ_RETRIES)" >&2
    attempt=$((attempt + 1))
    sleep "$FJ_RETRY_DELAY"
  done
}

# fj_transient_status <http-status> — true when the status is worth retrying.
# 000 is curl's stand-in for "never got a response" (timeout, reset, DNS).
fj_transient_status() {
  case "$1" in
    000|429|5??) return 0 ;;
    *)           return 1 ;;
  esac
}

# fj_json <method> <path-or-url> [curl-args...]
# fj_curl, plus the body must parse as JSON — retried if it does not.
#
# A degraded Forgejo answers with an HTML error page under a 2xx often
# enough to matter, and a caller that pipes that into jq gets `null` for
# every field. `null` then reads as a legitimate value: this is exactly how
# `fj-pr-merge-when-green` came to announce an open PR as "already merged"
# and exit 5. An unreadable answer is not an answer.
fj_json() {
  local out attempt=1

  while :; do
    if out=$(fj_curl "$@") && printf '%s' "$out" | jq -e . >/dev/null 2>&1; then
      printf '%s' "$out"
      return 0
    fi

    if [ "$attempt" -ge "$FJ_RETRIES" ]; then
      fj_die "$1 $2 → no parseable JSON after $FJ_RETRIES attempts"
    fi

    echo "fj: $1 $2 → unparseable body, retrying ($attempt/$FJ_RETRIES)" >&2
    attempt=$((attempt + 1))
    sleep "$FJ_RETRY_DELAY"
  done
}

# fj_create <path> <payload> <verify-cmd...> — POST once, then verify.
# Prints the created resource's `.number`.
#
# A create is not idempotent, so `fj_curl`'s retry is unsafe here: a POST
# whose *response* was lost has still been applied, and re-sending it creates
# a second resource. That is not hypothetical — one `fj-issue-create`
# invocation produced three identical issues when the instance answered
# HTTP 000 while applying every attempt.
#
# So: send once, and if the answer is lost, ask the server what happened
# rather than asking it to do the thing again. `verify-cmd` is run in that
# case and must print the number of the resource if it exists, or nothing.
#
# `fj-pr-create` survived this only by accident — Forgejo rejects a second PR
# for the same head/base with a 4xx, which is not retried. Issues have no
# such uniqueness constraint, so nothing caught the duplicates.
fj_create() {
  local path="$1" payload="$2"
  shift 2

  local out
  if out=$(FJ_RETRIES=1 FJ_MAX_TIME="$FJ_WRITE_MAX_TIME" \
             fj_curl POST "$path" --data-binary "$payload"); then
    printf '%s' "$out" | jq -r '.number'
    return 0
  fi

  echo "fj: create failed or its response was lost — verifying" >&2

  local existing
  existing=$("$@" 2>/dev/null) || existing=""

  if [ -n "$existing" ] && [ "$existing" != "null" ]; then
    echo "fj: the create had in fact been applied" >&2
    printf '%s' "$existing"
    return 0
  fi

  return 1
}

# fj_pr_head_sha <pr-number> — print head SHA for a PR.
fj_pr_head_sha() {
  fj_json GET "/pulls/$1" | jq -r '.head.sha'
}

# fj_statuses <sha> — print latest-per-context status entries as JSON array.
# Forgejo returns oldest→newest; we keep the last entry per context.
#
# Deduplicating is not optional: the endpoint returns every status ever
# posted for the SHA, so a re-run leaves the superseded entries in place and
# a raw `select(.status == "pending")` count never reaches zero.
fj_statuses() {
  fj_json GET "/commits/$1/statuses?limit=100" \
    | jq '[.[]] | sort_by(.id) | reverse | unique_by(.context) | sort_by(.context)'
}

# fj_run_id <sha> — print the API id of the newest Actions run for a commit.
#
# Runs have two identifiers and they are not interchangeable: `index_in_repo`
# is the number in web URLs (/actions/runs/3624), `id` is what the REST API
# keys on (13801). Passing the former to the API returns "resource does not
# exist", which is indistinguishable from an unimplemented endpoint.
fj_run_id() {
  fj_json GET "/actions/runs?head_sha=$1" \
    | jq -r '[.workflow_runs[].id] | max // empty'
}
