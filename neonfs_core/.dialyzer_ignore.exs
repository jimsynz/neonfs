# Dialyzer ignore patterns
#
# The :ra package does not provide complete type specifications, causing
# Dialyzer to report errors for valid Ra API usage. Ra's type specs claim
# functions like `start_server/1` return `ok | {:error, _}` but they actually
# return `{:ok, pid}` in some cases. Similarly, `members/1` can return
# `:undefined` as the leader value, but the spec says `ra_server_id()`.

[
  # Ra machine behaviour callback info
  ~r"callback_info_missing",

  # Ra return type pattern matching issues
  # Ra's type specs are incomplete - functions return values not in the specs:
  # - start_server can return {:ok, pid} but spec says ok | {:error, _}
  # - members/1,2 can return :undefined as leader but spec says ra_server_id()
  # - Our wrapper returns {:ok, :started} or {:ok, :restarted} which dialyzer
  #   then incorrectly thinks is the only possible return type
  ~r"lib/neon_fs/core/ra_server\.ex:\d+:\d+:pattern_match",
  ~r"lib/neon_fs/core/ra_server\.ex:\d+:\d+:guard_fail",
  ~r"lib/neon_fs/cluster/join\.ex:\d+:\d+:pattern_match",

  # Test support module uses ExUnit macros that inject functions at compile time
  # Dialyzer doesn't see these injected functions when analysing test support
  ~r"test/support/test_case\.ex:\d+.*ExUnit"
]
