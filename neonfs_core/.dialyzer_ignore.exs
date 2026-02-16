# Dialyzer ignore patterns
#
# The :ra package does not provide complete type specifications, causing
# Dialyzer to report errors for valid Ra API usage. Ra's type specs claim
# functions like `start_server/1` return `ok | {:error, _}` but they actually
# return `{:ok, pid}` in some cases. Similarly, `members/1` can return
# `:undefined` as the leader value, but the spec says `ra_server_id()`.

[
  # Ra return type pattern matching issues
  # Ra's type specs are incomplete - functions return values not in the specs:
  # - start_server can return {:ok, pid} but spec says ok | {:error, _}
  # - members/1,2 can return :undefined as leader but spec says ra_server_id()
  # - Our wrapper returns {:ok, :started} or {:ok, :restarted} which dialyzer
  #   then incorrectly thinks is the only possible return type
  ~r"lib/neon_fs/core/ra_server\.ex:\d+:\d+:pattern_match",
  ~r"lib/neon_fs/core/ra_server\.ex:\d+:\d+:guard_fail",
  ~r"lib/neon_fs/cluster/join\.ex:\d+:\d+:pattern_match",
  ~r"lib/neon_fs/cluster/join\.ex:\d+:\d+:call_to_missing",

  # MapSet is an opaque type - Dialyzer tracks its internal structure through
  # struct construction and flags it when passed to other functions
  ~r"lib/neon_fs/core/stripe_repair\.ex:\d+:\d+:call_without_opaque",

  # x509 dep uses internal ASN.1 record types and :public_key types that are
  # not in Dialyzer's type database (X509.ASN1.record/1, :public_key.ec_private_key/0)
  ~r"lib/x509/certificate\.ex:\d+:\d+:unknown_type",
  ~r"lib/x509/csr\.ex:\d+:\d+:unknown_type",
  ~r"lib/x509/private_key\.ex:\d+:\d+:unknown_type",

  # x509 library bug: CRL.Extension.extension_id() type says :crl_reason but
  # find/2 only matches :reason_code. We must use :reason_code for runtime correctness.
  # Also, certificate_authority pattern-matches on opaque x509 ASN.1 record types
  # (Extension tuples, CRL entries) which dialyzer reports as no_return.
  ~r"lib/neon_fs/core/certificate_authority\.ex:\d+:\d+:call",
  ~r"lib/neon_fs/core/certificate_authority\.ex:\d+:\d+:no_return",

  # Test support module uses ExUnit macros that inject functions at compile time.
  # Under MIX_ENV=test (used in CI), test/support is compiled via elixirc_paths
  # but ExUnit internals aren't in the PLT. Unnecessary skip under MIX_ENV=dev.
  ~r"test/support/test_case\.ex"
]
