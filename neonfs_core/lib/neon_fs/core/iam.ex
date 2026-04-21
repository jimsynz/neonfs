defmodule NeonFS.Core.IAM do
  @moduledoc """
  Ash domain for the NeonFS identity and access management subsystem.

  Today this domain is a skeleton — no resources are registered yet.
  Subsequent slices under the IAM epic (#135) layer the user-facing
  resources onto the Ra-backed plumbing provided by the state-machine
  `iam_put` / `iam_delete` primitives and the
  `NeonFS.Core.IAM.Manager` ETS/Ra cache:

    * `#288` — `User` resource with password authentication.
    * `#289` — TOTP 2FA for users.
    * `#290` — `Group` resource and user↔group membership.
    * `#291` — `AccessPolicy` resource and authorisation evaluator.
    * `#292` — `IdentityMapping` resource and protocol-bridge resolver.

  The `resources` block is intentionally empty — each slice listed
  above adds its own resource.
  """

  use Ash.Domain

  resources do
  end
end
