# Shared library for fj-* scripts. Source from each script:
#   source "$(dirname "$0")/_fj-lib.sh"
#
# Provides:
#   FJ_TOKEN  — Forgejo API token (from ~/.local/share/forgejo-cli/keys.json)
#   FJ_HOST   — Forgejo host (default: harton.dev, override via env)
#   FJ_REPO   — owner/name (default: project-neon/neonfs, override via env)
#   FJ_API    — base API URL: https://$FJ_HOST/api/v1/repos/$FJ_REPO
#   fj_curl   — curl wrapper that injects auth, dies on HTTP >= 400
#   fj_die    — print to stderr and exit 1

set -euo pipefail

: "${FJ_HOST:=harton.dev}"
: "${FJ_REPO:=project-neon/neonfs}"

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

  local body status
  body=$(mktemp)
  status=$(curl -sS -w '%{http_code}' -o "$body" \
    -X "$method" \
    -H "Authorization: token $FJ_TOKEN" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    "$@" \
    "$url")

  if [ "$status" -ge 400 ]; then
    echo "fj: $method $url → HTTP $status" >&2
    cat "$body" >&2
    echo >&2
    rm -f "$body"
    return 1
  fi

  cat "$body"
  rm -f "$body"
}

# fj_pr_head_sha <pr-number> — print head SHA for a PR.
fj_pr_head_sha() {
  fj_curl GET "/pulls/$1" | jq -r '.head.sha'
}

# fj_statuses <sha> — print latest-per-context status entries as JSON array.
# Forgejo returns oldest→newest; we keep the last entry per context.
fj_statuses() {
  fj_curl GET "/commits/$1/statuses" \
    | jq '[.[]] | sort_by(.id) | reverse | unique_by(.context) | sort_by(.context)'
}
