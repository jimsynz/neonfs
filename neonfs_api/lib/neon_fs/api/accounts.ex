defmodule NeonFS.Api.Accounts do
  alias Logger.Backends.Console

  use Ash.Domain,
    otp_app: :neonfs_api,
    extensions: [AshGraphql.Domain]

  resources do
    resource NeonFS.Api.Accounts.User
    resource NeonFS.Api.Accounts.Token
  end
end
