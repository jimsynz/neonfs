defmodule NeonFS.CIFS.HandleRegistry do
  @moduledoc """
  Node-wide registry of open CIFS file handles, keyed by file identity.

  Handles used to live in each connection's own state, numbered from 1 per
  connection. That made them neither globally unique nor reachable from
  another connection, so a handle opened through one connection could not be
  used through another — and every fd operation re-resolved the stored path,
  so a rename or unlink between open and use changed or broke what the handle
  referred to.

  An entry holds the immutable `{volume, file_id}` identity and the identity
  pin taken at open, so the file survives an unlink for as long as the
  handle is open and a rename does not move the pin off it.

  It also holds the path the handle was opened at, for the one thing with
  no identity to be held by: the volume root, whose `FileMeta` carries
  `id: nil`. smbd opens a pathref on the share root and stats it *through
  that handle* on every tree connect, so with no name to fall back on the
  root is reachable by path and unreachable by handle — and smbd only ever
  asks by handle. The path serves that case alone; anything with an id
  still resolves by identity, which is what keeps a handle working across
  a rename.

  ## Releasing the pin exactly once

  Two things can end a handle: an explicit `close/1`, and the owning
  connection process dying. Both must release the pin, and between them they
  must release it once. `close/1` deletes the entry before releasing, so a
  `:DOWN` arriving concurrently finds nothing to release; the monitor is
  demonitored on the way out so it does not arrive at all in the ordinary
  case. A crashed bridge process is caught by the monitor, and the
  coordinator's holder-DOWN bulk release remains the backstop beneath both —
  the same arrangement FUSE relies on.

  Directory handles stay in per-connection state: directory-handle pinning is
  not part of the identity-pinned handle work.
  """

  use GenServer

  require Logger

  alias NeonFS.Client

  @type handle :: pos_integer()
  @type entry :: %{
          volume: String.t(),
          file_id: String.t() | nil,
          path: String.t(),
          flags: integer(),
          claim_id: String.t() | nil,
          owner: pid()
        }

  @table __MODULE__.Table

  ## Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Registers an open file and returns its globally unique handle.

  `owner` is the connection process; its death releases this handle's pin and
  no others.
  """
  @spec open(String.t(), String.t() | nil, String.t(), integer(), String.t() | nil, pid()) ::
          {:ok, handle()} | {:error, term()}
  def open(volume, file_id, path, flags, claim_id, owner \\ self()) do
    GenServer.call(__MODULE__, {:open, volume, file_id, path, flags, claim_id, owner})
  end

  @doc """
  Looks a handle up. Reads hit ETS directly, so an fd operation costs no
  GenServer round trip.
  """
  @spec fetch(handle()) :: {:ok, entry()} | :error
  def fetch(handle) do
    case :ets.lookup(@table, handle) do
      [{^handle, entry}] -> {:ok, entry}
      [] -> :error
    end
  rescue
    ArgumentError -> :error
  end

  @doc """
  Closes a handle and releases its pin. Answers `:error` for a handle that is
  not open, which the caller turns into `EBADF`.
  """
  @spec close(handle()) :: :ok | :error
  def close(handle) do
    GenServer.call(__MODULE__, {:close, handle})
  end

  @doc """
  Handles currently open for `owner`. For tests and diagnostics.
  """
  @spec handles_for(pid()) :: [handle()]
  def handles_for(owner) do
    @table
    |> :ets.tab2list()
    |> Enum.filter(fn {_handle, entry} -> entry.owner == owner end)
    |> Enum.map(&elem(&1, 0))
    |> Enum.sort()
  rescue
    ArgumentError -> []
  end

  ## GenServer callbacks

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:named_table, :set, :protected, read_concurrency: true])
    {:ok, %{table: table, next_handle: 1, monitors: %{}}}
  end

  @impl true
  def handle_call({:open, volume, file_id, path, flags, claim_id, owner}, _from, state) do
    handle = state.next_handle

    entry = %{
      volume: volume,
      file_id: file_id,
      path: path,
      flags: flags,
      claim_id: claim_id,
      owner: owner
    }

    :ets.insert(@table, {handle, entry})

    {:reply, {:ok, handle}, %{state | next_handle: handle + 1, monitors: monitor(state, owner)}}
  end

  @impl true
  def handle_call({:close, handle}, _from, state) do
    case :ets.lookup(@table, handle) do
      [{^handle, entry}] ->
        # Delete before releasing: a concurrent `:DOWN` for the same owner
        # then finds nothing and cannot release the pin a second time.
        :ets.delete(@table, handle)
        release_pin(entry)
        {:reply, :ok, %{state | monitors: demonitor_if_last(state, entry.owner)}}

      [] ->
        {:reply, :error, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    for handle <- handles_for(pid) do
      case :ets.lookup(@table, handle) do
        [{^handle, entry}] ->
          :ets.delete(@table, handle)
          release_pin(entry)

        [] ->
          :ok
      end
    end

    {:noreply, %{state | monitors: Map.delete(state.monitors, pid)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Private

  defp monitor(state, owner) do
    if Map.has_key?(state.monitors, owner) do
      state.monitors
    else
      Map.put(state.monitors, owner, Process.monitor(owner))
    end
  end

  defp demonitor_if_last(state, owner) do
    if handles_for(owner) == [] do
      case Map.fetch(state.monitors, owner) do
        {:ok, ref} ->
          Process.demonitor(ref, [:flush])
          Map.delete(state.monitors, owner)

        :error ->
          state.monitors
      end
    else
      state.monitors
    end
  end

  defp release_pin(%{claim_id: nil}), do: :ok

  defp release_pin(%{claim_id: claim_id}) do
    Client.core_call(NeonFS.Core, :unpin_file, [claim_id])
    :ok
  catch
    :exit, reason ->
      Logger.warning("Releasing a CIFS handle pin failed",
        claim_id: claim_id,
        reason: inspect(reason)
      )

      :ok
  end
end
