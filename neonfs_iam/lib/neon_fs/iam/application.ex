defmodule NeonFS.IAM.Application do
  @moduledoc """
  OTP application callback for `neonfs_iam`.

  The package currently registers no children; once authentication
  strategies and session state land, their supervisors plug in here.
  """

  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
  end
end
