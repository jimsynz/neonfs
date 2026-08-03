defmodule NeonFS.TestSupport.StubIntentLog do
  @moduledoc """
  An always-granting conflict lease for `NeonFS.Core.FileIndex` unit
  tests.

  The real `NeonFS.Core.IntentLog` needs Ra, and now fails closed when Ra
  is absent rather than pretending acquisition succeeded. `FileIndex`'s
  unit tests exercise index behaviour, not cluster serialisation, so
  standing up Ra per test to obtain a lease costs far more than it proves
  — they inject this instead.

  Tests that care about *leasing* behaviour should use
  `NeonFS.TestSupport.RefusingIntentLog`, or the real module against a
  running Ra.
  """

  alias NeonFS.Core.Intent

  @doc false
  @spec try_acquire(Intent.t()) :: {:ok, binary()}
  def try_acquire(%Intent{} = intent), do: {:ok, intent.id}

  @doc false
  @spec complete(binary()) :: :ok
  def complete(intent_id) when is_binary(intent_id), do: :ok

  @doc false
  @spec fail(binary(), term()) :: :ok
  def fail(intent_id, _reason) when is_binary(intent_id), do: :ok
end

defmodule NeonFS.TestSupport.RefusingIntentLog do
  @moduledoc """
  A conflict lease that never grants, standing in for an unreachable Ra.
  Used to assert that operations refuse rather than proceeding
  unserialised.
  """

  alias NeonFS.Core.Intent
  alias NeonFS.Error.Unavailable

  @doc false
  @spec try_acquire(Intent.t()) :: {:error, Unavailable.t()}
  def try_acquire(%Intent{}), do: {:error, Unavailable.from_reason(:ra_not_available)}

  @doc false
  @spec complete(binary()) :: {:error, Unavailable.t()}
  def complete(intent_id) when is_binary(intent_id),
    do: {:error, Unavailable.from_reason(:ra_not_available)}

  @doc false
  @spec fail(binary(), term()) :: {:error, Unavailable.t()}
  def fail(intent_id, _reason) when is_binary(intent_id),
    do: {:error, Unavailable.from_reason(:ra_not_available)}
end
