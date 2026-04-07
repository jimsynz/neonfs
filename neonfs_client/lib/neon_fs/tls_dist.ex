defmodule NeonFS.TLSDist do
  @moduledoc """
  Custom TLS verify_fun for Erlang distribution.

  With the local CA approach, both local certs (daemon and CLI) and cluster
  certs are properly CA-signed. Standard chain validation via `ca_bundle.crt`
  handles both trust domains — the local CA and the cluster CA.

  This module provides a verify_fun that delegates to standard behaviour,
  accepting any certificate that chains to a CA in the `cacertfile` bundle.

  ## Constraints

  This module is loaded at BEAM boot time (before OTP applications start)
  because it's referenced in `ssl_dist.conf`. It must not call any OTP
  application code — no `Application.get_env/3`, no GenServer calls.
  Only `:public_key` is available (started by the SSL application as part
  of distribution startup).
  """

  @doc """
  TLS distribution verify_fun callback.

  Accepts certificates that pass standard chain validation against the
  CA bundle. Rejects certificates that fail validation.

  This is referenced in `ssl_dist.conf` as:

      {verify_fun, {'Elixir.NeonFS.TLSDist', verify_peer, []}}

  """
  @spec verify_peer(term(), term(), term()) ::
          {:valid, term()} | {:fail, term()} | {:unknown, term()}
  def verify_peer(_cert, {:extension, _extension}, state) do
    {:unknown, state}
  end

  def verify_peer(_cert, :valid, state) do
    {:valid, state}
  end

  def verify_peer(_cert, :valid_peer, state) do
    {:valid, state}
  end

  def verify_peer(_cert, {:bad_cert, reason}, _state) do
    {:fail, reason}
  end
end
