defmodule NeonFS.Api.Accounts do
  use Ash.Domain,
    otp_app: :neonfs_api

  resources do
    resource NeonFS.Api.Accounts.User
  end
end
