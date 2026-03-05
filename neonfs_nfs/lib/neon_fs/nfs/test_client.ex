defmodule NeonFS.NFS.TestClient do
  @moduledoc """
  Friendly Elixir wrapper around the NFS test client NIFs.

  Provides a high-level API for exercising the full NFSv3 protocol path
  in ExUnit tests. Each operation connects through TCP to the Rust
  nfs3_server, which dispatches to the Elixir Handler.
  """

  alias NeonFS.NFS.Native

  @type client :: reference()

  @doc """
  Connect to an NFS server and mount the given export.
  """
  @spec connect(String.t(), non_neg_integer(), String.t()) :: {:ok, client()} | {:error, term()}
  def connect(host \\ "127.0.0.1", port, export \\ "/") do
    Native.test_client_connect(host, port, export)
  end

  @doc """
  Disconnect from the NFS server.
  """
  @spec disconnect(client()) :: :ok | {:error, term()}
  def disconnect(client) do
    case Native.test_client_disconnect(client) do
      {:ok, {}} -> :ok
      error -> error
    end
  end

  @doc """
  Get the root file handle from the mount.
  """
  @spec root_handle(client()) :: {:ok, binary()} | {:error, term()}
  def root_handle(client) do
    Native.test_client_root_handle(client)
  end

  @doc """
  NFS NULL (ping) — verifies the server is alive.
  """
  @spec null(client()) :: :ok | {:error, term()}
  def null(client) do
    case Native.test_client_null(client) do
      {:ok, {}} -> :ok
      error -> error
    end
  end

  @doc """
  GETATTR — get file attributes for a file handle.
  """
  @spec getattr(client(), binary()) :: {:ok, map()} | {:error, term()}
  def getattr(client, fh) do
    Native.test_client_getattr(client, fh)
  end

  @doc """
  LOOKUP — resolve a filename in a directory.

  Returns `{:ok, %{"handle" => binary, "attrs" => map}}` on success.
  """
  @spec lookup(client(), binary(), String.t()) :: {:ok, map()} | {:error, term()}
  def lookup(client, dir_fh, name) do
    Native.test_client_lookup(client, dir_fh, name)
  end

  @doc """
  READDIRPLUS — list directory entries with attributes.

  Returns `{:ok, [%{"name" => string, "handle" => binary, "attrs" => map, "fileid" => integer}]}`.
  """
  @spec readdirplus(client(), binary()) :: {:ok, [map()]} | {:error, term()}
  def readdirplus(client, dir_fh) do
    Native.test_client_readdirplus(client, dir_fh)
  end

  @doc """
  CREATE — create a new file in a directory.

  Returns `{:ok, %{"handle" => binary, "attrs" => map}}` on success.
  """
  @spec create(client(), binary(), String.t()) :: {:ok, map()} | {:error, term()}
  def create(client, dir_fh, name) do
    Native.test_client_create(client, dir_fh, name)
  end

  @doc """
  MKDIR — create a directory.

  Returns `{:ok, %{"handle" => binary, "attrs" => map}}` on success.
  """
  @spec mkdir(client(), binary(), String.t()) :: {:ok, map()} | {:error, term()}
  def mkdir(client, dir_fh, name) do
    Native.test_client_mkdir(client, dir_fh, name)
  end

  @doc """
  WRITE — write data to a file at the given offset.

  Returns `{:ok, bytes_written}` on success.
  """
  @spec write(client(), binary(), non_neg_integer(), binary()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def write(client, fh, offset, data) do
    Native.test_client_write(client, fh, offset, data)
  end

  @doc """
  READ — read data from a file.

  Returns `{:ok, %{"data" => binary, "eof" => boolean}}` on success.
  """
  @spec read(client(), binary(), non_neg_integer(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()}
  def read(client, fh, offset, count) do
    Native.test_client_read(client, fh, offset, count)
  end

  @doc """
  REMOVE — delete a file from a directory.
  """
  @spec remove(client(), binary(), String.t()) :: :ok | {:error, term()}
  def remove(client, dir_fh, name) do
    case Native.test_client_remove(client, dir_fh, name) do
      {:ok, {}} -> :ok
      error -> error
    end
  end

  @doc """
  SETATTR — modify file attributes.

  Accepts a keyword list with optional `:mode`, `:uid`, `:gid`, `:size`.
  Returns `{:ok, attrs_map}` on success.
  """
  @spec setattr(client(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def setattr(client, fh, opts \\ []) do
    mode = Keyword.get(opts, :mode)
    uid = Keyword.get(opts, :uid)
    gid = Keyword.get(opts, :gid)
    size = Keyword.get(opts, :size)
    Native.test_client_setattr(client, fh, mode, uid, gid, size)
  end

  @doc """
  RENAME — move a file or directory.
  """
  @spec rename(client(), binary(), String.t(), binary(), String.t()) :: :ok | {:error, term()}
  def rename(client, from_dir_fh, from_name, to_dir_fh, to_name) do
    case Native.test_client_rename(client, from_dir_fh, from_name, to_dir_fh, to_name) do
      {:ok, {}} -> :ok
      error -> error
    end
  end

  @doc """
  SYMLINK — create a symbolic link.

  Returns `{:ok, %{"handle" => binary, "attrs" => map}}` on success.
  """
  @spec symlink(client(), binary(), String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def symlink(client, dir_fh, name, target) do
    Native.test_client_symlink(client, dir_fh, name, target)
  end

  @doc """
  READLINK — read symlink target.

  Returns `{:ok, target_string}` on success.
  """
  @spec readlink(client(), binary()) :: {:ok, String.t()} | {:error, term()}
  def readlink(client, fh) do
    Native.test_client_readlink(client, fh)
  end

  @doc """
  CREATE_EXCLUSIVE — atomic create-if-absent.

  Returns `{:ok, %{"handle" => binary, "attrs" => map}}` on success.
  """
  @spec create_exclusive(client(), binary(), String.t()) :: {:ok, map()} | {:error, term()}
  def create_exclusive(client, dir_fh, name) do
    Native.test_client_create_exclusive(client, dir_fh, name)
  end
end
