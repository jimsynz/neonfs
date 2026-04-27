defmodule NFSServer.NFSv3.MockBackend do
  @moduledoc false
  # Test-only backend that resolves callbacks against a per-process
  # registry. Set fixtures with `put/2`; lookups not in the registry
  # return `{:error, :stale}` so the handler tests can exercise the
  # error arms.

  @behaviour NFSServer.NFSv3.Backend

  alias NFSServer.NFSv3.Types

  @doc "Set the response for a callback. `key` is `{callback_atom, args_tuple}`."
  def put(key, value), do: Process.put({__MODULE__, key}, value)

  @doc "Clear the entire mock registry."
  def reset, do: for(k <- Process.get_keys(), match?({__MODULE__, _}, k), do: Process.delete(k))

  defp fetch(key, default \\ {:error, :stale}) do
    Process.get({__MODULE__, key}, default)
  end

  @impl true
  def getattr(fh, _auth, _ctx), do: fetch({:getattr, fh})

  @impl true
  def access(fh, mask, _auth, _ctx), do: fetch({:access, {fh, mask}})

  @impl true
  def lookup(dir, name, _auth, _ctx), do: fetch({:lookup, {dir, name}})

  @impl true
  def readlink(fh, _auth, _ctx), do: fetch({:readlink, fh})

  @impl true
  def read(fh, off, count, _auth, _ctx),
    do: fetch({:read, {fh, off, count}}, {:error, :stale, nil})

  @impl true
  def readdir(fh, cookie, verf, count, _auth, _ctx),
    do: fetch({:readdir, {fh, cookie, verf, count}}, {:error, :stale, nil})

  @impl true
  def readdirplus(fh, cookie, verf, dircount, maxcount, _auth, _ctx),
    do: fetch({:readdirplus, {fh, cookie, verf, dircount, maxcount}}, {:error, :stale, nil})

  @impl true
  def fsstat(fh, _auth, _ctx), do: fetch({:fsstat, fh})

  @impl true
  def fsinfo(fh, _auth, _ctx), do: fetch({:fsinfo, fh})

  @impl true
  def pathconf(fh, _auth, _ctx), do: fetch({:pathconf, fh})

  @impl true
  def setattr(fh, sattr, guard, _auth, _ctx), do: fetch({:setattr, {fh, sattr, guard}})

  @impl true
  def create(dir, name, mode, _auth, _ctx), do: fetch({:create, {dir, name, mode}})

  @doc "A simple `Fattr3` fixture for tests that don't care about the values."
  def sample_fattr3 do
    %Types.Fattr3{
      type: :reg,
      mode: 0o644,
      nlink: 1,
      uid: 1000,
      gid: 1000,
      size: 12,
      used: 4096,
      rdev: %Types.Specdata3{specdata1: 0, specdata2: 0},
      fsid: 1,
      fileid: 7,
      atime: %Types.Nfstime3{seconds: 1, nseconds: 0},
      mtime: %Types.Nfstime3{seconds: 2, nseconds: 0},
      ctime: %Types.Nfstime3{seconds: 3, nseconds: 0}
    }
  end
end
