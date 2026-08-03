defmodule NeonFS.TestSupport.KeyManagerStub do
  @moduledoc """
  A `KeyManager` that knows no keys, for scrub tests asserting the
  unreadable-chunk path.

  Lives here rather than at the bottom of `scrub_test.exs` for the reason
  `NeonFS.TestSupport.TieringMocks` does: a module defined after the test
  module in the same file is still compiling when `ExUnit.async_run/0`
  starts that module's tests. `scrub_test.exs` is serial today so it never
  hit that, but the trap is invisible until someone flips the flag.
  """

  def get_volume_key(_volume_id, _key_version) do
    {:error, :unknown_key_version}
  end

  def get_current_key(_volume_id) do
    {:error, :unknown_key_version}
  end
end
