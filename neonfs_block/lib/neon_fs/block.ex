defmodule NeonFS.Block do
  @moduledoc """
  What this node can serve a block device over, and how to take one.

  Two frontends reach the same device, and they are not symmetrical in who
  performs the attach:

    * **NBD** is a network protocol. A client anywhere runs `nbd-client`
      against this node's listener, so the attach happens on the *client's*
      host and this module has nothing to do but say where to dial.
    * **ublk** is local. The device node is created by the kernel of the host
      running the target, so the attach happens *here* and `/dev/ublkbN`
      appears here.

  That asymmetry is the whole reason selection is not a detail of the IO
  path. A caller on another host can only ever use NBD, however capable this
  node is — so choosing a frontend is a question about the pair, and only the
  caller knows both halves of it. What this module provides is its own half:
  what this node can do, why not when it cannot, and the ublk attach itself.

  ## Preference

  `:neonfs_block, :frontend` is `:auto`, `:ublk` or `:nbd`. `:auto` prefers
  ublk where it works. Forcing one that does not work is an error naming the
  check that failed, never a fallback: a silent fallback is how a benchmark
  measures NBD and reports it as ublk.
  """

  alias NeonFS.Block.Ublk
  alias NeonFS.Block.Ublk.Capability

  @type frontend :: :nbd | :ublk
  @type preference :: :auto | frontend()

  @doc """
  The frontends this node can serve, NBD always among them.

  This is what the service registration advertises, so a caller can tell
  before dialling anything whether ublk is even on the table here.
  """
  @spec frontends() :: [frontend()]
  def frontends do
    case Capability.check() do
      :ok -> [:nbd, :ublk]
      {:error, _reason} -> [:nbd]
    end
  end

  @doc "This node's configured frontend preference."
  @spec preference() :: preference()
  def preference, do: Application.get_env(:neonfs_block, :frontend, :auto)

  @doc """
  Resolves `preference` against what this node can actually do.

  `:auto` answers `:nbd` where ublk does not work, which is the fallback the
  issue asks for. A forced frontend does not fall back — it fails, carrying
  `Capability`'s reason so the caller can say whether the driver or the
  helper is what is missing.
  """
  @spec select() :: {:ok, frontend()} | {:error, term()}
  def select, do: select(preference())

  @spec select(preference()) :: {:ok, frontend()} | {:error, term()}
  def select(:nbd), do: {:ok, :nbd}

  def select(:ublk) do
    case Capability.check() do
      :ok -> {:ok, :ublk}
      {:error, reason} -> {:error, {:frontend_forced_unavailable, :ublk, reason}}
    end
  end

  def select(:auto) do
    case Capability.check() do
      :ok -> {:ok, :ublk}
      {:error, _reason} -> {:ok, :nbd}
    end
  end

  def select(other), do: {:error, {:unknown_frontend, other}}

  @doc """
  Attaches `export` here as a ublk device, answering with its path.

  Returns only once `/dev/ublkbN` exists: the caller's next act is to hand
  that path to something that will open it, and a path that is not yet a
  device would fail there instead of here.
  """
  @spec attach_ublk(String.t(), keyword()) :: {:ok, Path.t()} | {:error, term()}
  def attach_ublk(export, opts \\ []) do
    with {:ok, :ublk} <- select(:ublk),
         {:ok, target} <- Ublk.Supervisor.attach(export, opts) do
      Ublk.Target.device_path(target)
    end
  end

  @doc """
  Drops the ublk device serving `export`, if this node has one.

  Idempotent, so a caller retrying after a target already died of its own
  accord is not an error.
  """
  @spec detach_ublk(String.t()) :: :ok
  def detach_ublk(export), do: Ublk.Supervisor.detach(export)
end
