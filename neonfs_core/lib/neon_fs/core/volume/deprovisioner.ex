defmodule NeonFS.Core.Volume.Deprovisioner do
  @moduledoc """
  Mirror of `NeonFS.Core.Volume.Provisioner` for the volume delete
  path (#806).

  Submits `:unregister_volume_root` to Ra so the bootstrap layer
  (#779) stops trying to read the volume's (soon-to-be-orphaned)
  root segment. The Ra command is idempotent — calling it for a
  volume that was never registered (or already unregistered) is a
  no-op-equivalent and returns `:ok`.

  No explicit chunk cascade is needed: once the bootstrap entry is
  gone, the root segment chunk + every index tree node + every data
  chunk owned by the volume become unreferenced and are reaped by
  the existing GC sweep on each drive.

  The Ra submission is injectable via opts so tests don't need a
  live cluster.
  """

  alias NeonFS.Core.RaSupervisor

  @type deprovision_error :: {:error, {:bootstrap_unregister_failed, term()}}

  @doc """
  Tells the bootstrap layer to forget the given volume. Returns
  `{:ok, :unregistered}` on success.

  Opts:

  - `:bootstrap_registrar` — `(command -> {:ok, _} | :ok | {:error, _})`,
    defaults to `RaSupervisor.command/1`.
  """
  @spec deprovision(volume_id :: binary(), keyword()) ::
          {:ok, :unregistered} | deprovision_error()
  def deprovision(volume_id, opts \\ []) when is_binary(volume_id) do
    bootstrap_registrar =
      Keyword.get(opts, :bootstrap_registrar, &default_registrar/1)

    case bootstrap_registrar.({:unregister_volume_root, volume_id}) do
      :ok -> {:ok, :unregistered}
      {:ok, _} -> {:ok, :unregistered}
      {:ok, _, _} -> {:ok, :unregistered}
      {:error, reason} -> {:error, {:bootstrap_unregister_failed, reason}}
      other -> {:error, {:bootstrap_unregister_failed, other}}
    end
  end

  ## Internals

  # Wraps `RaSupervisor.command/1` so its `{:ok, result, leader}` reply
  # shape matches the simpler `:ok | {:ok, _} | {:error, _}` contract
  # callers expect.
  defp default_registrar(command) do
    case RaSupervisor.command(command) do
      {:ok, result, _leader} -> {:ok, result}
      {:error, _} = err -> err
      other -> {:error, other}
    end
  end
end
