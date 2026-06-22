defmodule NeonFS.Cluster.PeerConnector do
  @moduledoc """
  Opens Erlang distribution links to known cluster peers once on startup.

  A restarted core node relies on Ra messaging to re-establish links to
  its peers, but Ra only dials a peer when it next has cause to reach it.
  A node that booted before its network was up (e.g. a tailnet that came
  online after the BEAM) can therefore sit islanded until something
  prompts Ra to act. This nudges every known link open immediately on
  startup; ongoing reconnection remains Ra's responsibility.

  Peers come from the persisted `known_peers` in `cluster.json`. With no
  cluster state — a greenfield or single node — there are no peers and
  this is a no-op.
  """

  use GenServer

  require Logger

  alias NeonFS.Cluster.State

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    state = %{
      peers: Keyword.get(opts, :peers, &known_peer_nodes/0),
      connect: Keyword.get(opts, :connect, &Node.connect/1)
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    state.peers
    |> resolve_peers()
    |> Enum.each(&connect_peer(&1, state.connect))

    {:noreply, state}
  end

  defp resolve_peers(peers) when is_function(peers, 0), do: peers.()
  defp resolve_peers(peers) when is_list(peers), do: peers

  defp known_peer_nodes do
    case State.load() do
      {:ok, %{known_peers: peers}} -> Enum.map(peers, & &1.name)
      _ -> []
    end
  end

  defp connect_peer(node, connect) do
    case connect.(node) do
      true ->
        Logger.info("Connected to cluster peer on startup", peer: node)

      result ->
        Logger.warning("Could not connect to cluster peer on startup",
          peer: node,
          result: inspect(result)
        )
    end
  end
end
