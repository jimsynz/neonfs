defmodule NeonFS.Transport.Handler do
  @moduledoc """
  Handles an accepted TLS connection for inbound chunk operations.

  Processes `put_chunk`, `get_chunk`, and `has_chunk` requests from peer nodes,
  dispatching to the local blob store. Uses `{active, N}` for backpressure —
  the BEAM delivers N frames then pauses until re-armed via `:ssl_passive`.

  ## Options

    * `:socket` (required) — accepted TLS socket
    * `:dispatch_module` — module implementing the blob store interface
      (default: `NeonFS.Core.BlobStore`)

  """

  use GenServer, restart: :temporary

  require Logger

  @active_n 10
  @default_dispatch NeonFS.Core.BlobStore

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  # GenServer callbacks

  @impl GenServer
  def init(opts) do
    socket = Keyword.fetch!(opts, :socket)
    dispatch = Keyword.get(opts, :dispatch_module, @default_dispatch)
    {:ok, %{socket: socket, dispatch: dispatch}}
  end

  @impl GenServer
  def handle_info(:activate, state) do
    :ssl.setopts(state.socket, [{:active, @active_n}])
    {:noreply, state}
  end

  def handle_info({:ssl, _socket, data}, state) do
    message = :erlang.binary_to_term(data, [:safe])
    response = dispatch(message, state)
    :ssl.send(state.socket, :erlang.term_to_binary(response))
    {:noreply, state}
  end

  def handle_info({:ssl_passive, socket}, state) do
    :ssl.setopts(socket, [{:active, @active_n}])
    {:noreply, state}
  end

  def handle_info({:ssl_closed, _}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:ssl_error, _, _reason}, state) do
    {:stop, :normal, state}
  end

  # Dispatch functions

  defp dispatch({:put_chunk, ref, _hash, volume_id, _write_id, tier, data}, state) do
    case state.dispatch.write_chunk(data, volume_id, tier, []) do
      {:ok, _hash, _info} -> {:ok, ref}
      {:error, reason} -> {:error, ref, reason}
    end
  end

  defp dispatch({:get_chunk, ref, hash, volume_id}, state) do
    dispatch({:get_chunk, ref, hash, volume_id, "hot"}, state)
  end

  defp dispatch({:get_chunk, ref, hash, volume_id, tier}, state) do
    case state.dispatch.read_chunk(hash, volume_id, tier: tier) do
      {:ok, chunk_bytes} -> {:ok, ref, chunk_bytes}
      {:error, _reason} -> {:error, ref, :not_found}
    end
  end

  defp dispatch({:has_chunk, ref, hash}, state) do
    case state.dispatch.chunk_info(hash) do
      {:ok, tier, size} -> {:ok, ref, tier, size}
      {:error, _reason} -> {:error, ref, :not_found}
    end
  end
end
