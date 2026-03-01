defmodule NeonFS.FUSE.HandlerTest do
  use ExUnit.Case, async: true

  alias NeonFS.FUSE.Handler

  describe "relatime_stale?/3" do
    test "stale when accessed_at is older than modified_at" do
      accessed_at = ~U[2026-01-01 10:00:00Z]
      modified_at = ~U[2026-01-01 12:00:00Z]
      now = ~U[2026-01-01 12:30:00Z]

      assert Handler.relatime_stale?(accessed_at, modified_at, now)
    end

    test "not stale when accessed_at is newer than modified_at and less than 24h old" do
      accessed_at = ~U[2026-01-01 14:00:00Z]
      modified_at = ~U[2026-01-01 12:00:00Z]
      now = ~U[2026-01-01 15:00:00Z]

      refute Handler.relatime_stale?(accessed_at, modified_at, now)
    end

    test "stale when accessed_at is newer than modified_at but more than 24h old" do
      accessed_at = ~U[2026-01-01 10:00:00Z]
      modified_at = ~U[2026-01-01 08:00:00Z]
      now = ~U[2026-01-02 11:00:00Z]

      assert Handler.relatime_stale?(accessed_at, modified_at, now)
    end

    test "not stale when accessed_at equals modified_at and less than 24h old" do
      accessed_at = ~U[2026-01-01 12:00:00Z]
      modified_at = ~U[2026-01-01 12:00:00Z]
      now = ~U[2026-01-01 13:00:00Z]

      refute Handler.relatime_stale?(accessed_at, modified_at, now)
    end

    test "stale when accessed_at equals modified_at but more than 24h old" do
      accessed_at = ~U[2026-01-01 12:00:00Z]
      modified_at = ~U[2026-01-01 12:00:00Z]
      now = ~U[2026-01-02 13:00:00Z]

      assert Handler.relatime_stale?(accessed_at, modified_at, now)
    end
  end
end
