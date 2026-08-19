defmodule NeonFS.CSI.Provision do
  @moduledoc """
  Release command that puts cluster credentials on the host and exits.

  This is what a Kubernetes init container runs, from the CSI image itself so
  there is no second artefact to ship or version. It redeems an invite into the
  host's state directory and stops — nothing is left running, and the host does
  not become a cluster member. Only the pods that later start on it register as
  services.

  Invoked as `bin/neonfs_csi eval 'NeonFS.CSI.Provision.main()'`.

  ## Configuration

  Both from the environment, because they come from different Kubernetes objects
  and must: the via address from a ConfigMap, the token from a Secret. A
  ConfigMap must not carry the token.

    * `NEONFS_BOOTSTRAP_TOKEN` — the invite. Required.
    * `NEONFS_CORE_NODE` — `host:port` of a cluster member's redemption
      endpoint. Required.

  ## Exit status

  `0` when the host holds credentials on return, whether this invocation
  obtained them or found them already there. Non-zero otherwise, having said
  why — an init container that exits 0 without credentials produces a pod that
  starts, fails every mount, and blames the mount.
  """

  alias NeonFS.Client.Join

  require Logger

  @doc """
  Redeem an invite into local credentials, then halt with an exit status.
  """
  @spec main() :: no_return()
  def main do
    case provision() do
      {:ok, :provisioned} ->
        Logger.info("Cluster credentials provisioned")
        System.halt(0)

      {:ok, :already_provisioned} ->
        Logger.info("Cluster credentials already present; nothing to do")
        System.halt(0)

      {:error, reason} ->
        Logger.error("Could not provision cluster credentials", reason: inspect(reason))
        System.halt(1)
    end
  end

  @doc """
  The work `main/0` wraps, without the `System.halt/1`.

  Separate so it can be called in a test, which `main/0` cannot be.
  """
  @spec provision(keyword()) ::
          {:ok, :provisioned | :already_provisioned} | {:error, term()}
  def provision(opts \\ []) do
    with {:ok, token} <- required_env("NEONFS_BOOTSTRAP_TOKEN"),
         {:ok, via} <- required_env("NEONFS_CORE_NODE") do
      Join.redeem_credentials(token, via, Keyword.put_new(opts, :on_wait, &log_waiting/0))
    end
  end

  defp required_env(name) do
    case System.get_env(name) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _absent -> {:error, {:missing_env, name}}
    end
  end

  defp log_waiting do
    Logger.info("Another pod on this host is provisioning credentials; waiting")
  end
end
