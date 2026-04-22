defmodule NeonFS.IAM do
  @moduledoc """
  Identity and access management domain for NeonFS.

  This Ash domain hosts the IAM resources — `User`, `Group`, `AccessPolicy`,
  and `IdentityMapping` — plus the public authentication and authorisation
  surface consumed by `NeonFS.Core.Authorise` and the protocol bridges.

  No resources are registered yet; they land in subsequent slices of the
  IAM epic (see #288, #290, #291, #292).
  """

  use Ash.Domain

  resources do
  end
end
