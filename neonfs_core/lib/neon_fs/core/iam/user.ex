defmodule NeonFS.Core.IAM.User do
  @moduledoc """
  Identity record for a NeonFS IAM user.

  Persisted via `NeonFS.Core.IAM.Manager` (Ra-replicated, ETS-cached
  under `:iam_users`). Created by `NeonFS.Core.IAM.register/1` and
  matched against by `NeonFS.Core.IAM.authenticate/2`.

  ## Field notes

  * `:id` — UUIDv7, generated server-side on register.
  * `:email` — case-insensitive unique. The `t:t/0` struct stores
    the canonical (downcased) form; the original input is rejected
    at the manager layer if its downcased form clashes with an
    existing user.
  * `:password_hash` — bcrypt-hashed, never leaked from the public
    API. `Inspect` is overridden to redact it so accidental
    `IO.inspect` calls don't surface credentials in logs.
  * `:role` — `:user | :admin`, default `:user`. The authorisation
    evaluator (#291) is the only consumer.
  * `:disabled?` — when `true`, `authenticate/2` rejects with
    `:disabled` even on a valid password.

  ## Why a plain struct (not an Ash resource)

  The IAM domain is Ra-backed via `IAMManager`, with no Ecto data
  layer. `ash_authentication`'s password strategy expects a data
  layer with a uniqueness constraint, so adopting it here would
  require a custom strategy + per-action manual-action plumbing.
  We're deferring full Ash resourcefication until either (a)
  `ash_authentication` grows manual-action support, or (b) we
  introduce relationships/calculations that genuinely benefit from
  the Ash query DSL. The empty `NeonFS.Core.IAM` Ash domain stays
  in place to anchor that future work.
  """

  @derive {Inspect, only: [:id, :email, :role, :disabled?, :created_at, :updated_at]}

  defstruct [
    :id,
    :email,
    :password_hash,
    :role,
    :disabled?,
    :created_at,
    :updated_at
  ]

  @type role :: :user | :admin

  @type t :: %__MODULE__{
          id: binary(),
          email: String.t(),
          password_hash: String.t(),
          role: role(),
          disabled?: boolean(),
          created_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  @doc """
  Project the public-API view of a user — drops the password hash.
  Use this whenever returning a user from a public function.
  """
  @spec public(t()) :: map()
  def public(%__MODULE__{} = user) do
    %{
      id: user.id,
      email: user.email,
      role: user.role,
      disabled?: user.disabled?,
      created_at: user.created_at,
      updated_at: user.updated_at
    }
  end

  @doc """
  Canonicalise an email for storage / lookup. Trims whitespace and
  downcases. Email comparisons MUST go through this to satisfy the
  case-insensitive uniqueness contract.
  """
  @spec normalise_email(String.t()) :: String.t()
  def normalise_email(email) when is_binary(email) do
    email |> String.trim() |> String.downcase()
  end
end
