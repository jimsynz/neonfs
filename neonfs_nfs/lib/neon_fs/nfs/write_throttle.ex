defmodule NeonFS.NFS.WriteThrottle do
  @moduledoc """
  Shared semaphore that limits concurrent NFS write pressure.

  All NFS handler processes acquire a permit before making write RPCs to core
  nodes. When the system is at capacity (too many in-flight writes or bytes),
  `acquire/1` blocks the caller via a normal GenServer call. If the caller's
  timeout expires before a permit becomes available, it returns `{:error, :overloaded}`
  and the handler translates this to NFS3ERR_JUKEBOX.

  This provides idiomatic Elixir backpressure — callers block on GenServer calls
  rather than inspecting mailbox depth or polling counters.
  """

  use GenServer

  @default_max_in_flight_writes 32
  @default_max_in_flight_bytes 256 * 1024 * 1024
  @default_acquire_timeout 100

  @type permit :: reference()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Acquires a write permit for `byte_count` bytes.

  Blocks if the system is at capacity. Returns `{:ok, permit}` on success
  or `{:error, :overloaded}` if the acquire times out.
  """
  @spec acquire(non_neg_integer()) :: {:ok, permit()} | {:error, :overloaded} | :not_running
  def acquire(byte_count) do
    timeout = acquire_timeout()
    GenServer.call(__MODULE__, {:acquire, byte_count}, timeout)
  catch
    :exit, {:timeout, _} -> {:error, :overloaded}
    :exit, {:noproc, _} -> :not_running
  end

  @doc """
  Releases a previously acquired write permit.
  """
  @spec release(permit()) :: :ok
  def release(permit) do
    GenServer.call(__MODULE__, {:release, permit}, 5_000)
  catch
    :exit, {:noproc, _} -> :ok
  end

  @doc """
  Acquires a permit, runs the given function, then releases the permit.

  Returns `{:error, :overloaded}` if the permit cannot be acquired.
  """
  @spec with_permit(non_neg_integer(), (-> result)) :: result | {:error, :overloaded}
        when result: term()
  def with_permit(byte_count, fun) do
    case acquire(byte_count) do
      {:ok, permit} ->
        try do
          fun.()
        after
          release(permit)
        end

      :not_running ->
        fun.()

      {:error, :overloaded} ->
        {:error, :overloaded}
    end
  end

  @doc """
  Returns the current number of in-flight writes.
  """
  @spec in_flight_count :: non_neg_integer()
  def in_flight_count do
    GenServer.call(__MODULE__, :in_flight_count)
  catch
    :exit, {:noproc, _} -> 0
  end

  @doc """
  Returns the current number of in-flight bytes.
  """
  @spec in_flight_bytes :: non_neg_integer()
  def in_flight_bytes do
    GenServer.call(__MODULE__, :in_flight_bytes)
  catch
    :exit, {:noproc, _} -> 0
  end

  # Server

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    {:ok,
     %{
       in_flight_count: 0,
       in_flight_bytes: 0,
       permits: %{},
       waiters: :queue.new()
     }}
  end

  @impl true
  def handle_call({:acquire, byte_count}, from, state) do
    if under_limits?(state, byte_count) do
      {permit, state} = grant_permit(state, byte_count)
      {:reply, {:ok, permit}, state}
    else
      waiters = :queue.in({from, byte_count}, state.waiters)
      {:noreply, %{state | waiters: waiters}}
    end
  end

  def handle_call({:release, permit}, _from, state) do
    state = revoke_permit(state, permit)
    state = flush_waiters(state)
    {:reply, :ok, state}
  end

  def handle_call(:in_flight_count, _from, state) do
    {:reply, state.in_flight_count, state}
  end

  def handle_call(:in_flight_bytes, _from, state) do
    {:reply, state.in_flight_bytes, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  defp under_limits?(state, byte_count) do
    state.in_flight_count < max_in_flight_writes() and
      state.in_flight_bytes + byte_count <= max_in_flight_bytes()
  end

  defp grant_permit(state, byte_count) do
    permit = make_ref()
    permits = Map.put(state.permits, permit, %{bytes: byte_count})

    state = %{
      state
      | in_flight_count: state.in_flight_count + 1,
        in_flight_bytes: state.in_flight_bytes + byte_count,
        permits: permits
    }

    {permit, state}
  end

  defp revoke_permit(state, permit) do
    case Map.pop(state.permits, permit) do
      {nil, _permits} ->
        state

      {%{bytes: byte_count}, permits} ->
        %{
          state
          | in_flight_count: max(state.in_flight_count - 1, 0),
            in_flight_bytes: max(state.in_flight_bytes - byte_count, 0),
            permits: permits
        }
    end
  end

  defp flush_waiters(state) do
    case :queue.out(state.waiters) do
      {:empty, _} ->
        state

      {{:value, {from, byte_count}}, rest} ->
        if under_limits?(state, byte_count) do
          {permit, state} = grant_permit(state, byte_count)
          GenServer.reply(from, {:ok, permit})
          flush_waiters(%{state | waiters: rest})
        else
          state
        end
    end
  end

  defp max_in_flight_writes do
    Application.get_env(:neonfs_nfs, :max_in_flight_writes, @default_max_in_flight_writes)
  end

  defp max_in_flight_bytes do
    Application.get_env(:neonfs_nfs, :max_in_flight_bytes, @default_max_in_flight_bytes)
  end

  defp acquire_timeout do
    Application.get_env(:neonfs_nfs, :write_acquire_timeout, @default_acquire_timeout)
  end
end
