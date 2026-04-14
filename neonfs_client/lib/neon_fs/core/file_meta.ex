defmodule NeonFS.Core.FileMeta do
  @moduledoc """
  File metadata structure for NeonFS.

  Tracks logical files with POSIX attributes, chunk lists, and version history.
  Each file belongs to a volume and is identified by a unique ID.

  For Phase 1, only replicated volumes are supported (chunks field).
  Stripe references for erasure coding come in Phase 4.
  """

  @type acl_entry :: %{
          type: :user | :group | :mask | :other,
          id: non_neg_integer() | nil,
          permissions: MapSet.t(:r | :w | :x)
        }

  @type t :: %__MODULE__{
          id: String.t(),
          volume_id: String.t(),
          path: String.t(),
          chunks: [binary()],
          stripes: nil | [%{stripe_id: String.t(), byte_range: Range.t()}],
          size: non_neg_integer(),
          content_type: String.t(),
          mode: non_neg_integer(),
          uid: non_neg_integer(),
          gid: non_neg_integer(),
          acl_entries: [acl_entry()],
          default_acl: [acl_entry()] | nil,
          metadata: %{optional(String.t()) => term()},
          created_at: DateTime.t(),
          modified_at: DateTime.t(),
          accessed_at: DateTime.t(),
          changed_at: DateTime.t(),
          version: non_neg_integer(),
          previous_version_id: String.t() | nil,
          hlc_timestamp: term()
        }

  defstruct [
    :id,
    :volume_id,
    :path,
    :chunks,
    :stripes,
    :size,
    :content_type,
    :mode,
    :uid,
    :gid,
    :created_at,
    :modified_at,
    :accessed_at,
    :changed_at,
    :version,
    :previous_version_id,
    :hlc_timestamp,
    acl_entries: [],
    default_acl: nil,
    metadata: %{}
  ]

  @doc """
  Creates a new FileMeta for a given volume and path.

  ## Parameters
  - `volume_id`: ID of the volume this file belongs to
  - `path`: Absolute path within the volume (e.g., "/documents/report.pdf")
  - `opts`: Keyword list with optional fields:
    - `:id` - Custom file ID (default: generated UUID)
    - `:chunks` - Initial chunk list (default: [])
    - `:size` - Initial file size (default: 0)
    - `:content_type` - MIME content type (default: auto-detected from path extension)
    - `:mode` - POSIX mode (default: 0o644)
    - `:uid` - Owner user ID (default: 0)
    - `:gid` - Owner group ID (default: 0)
    - `:version` - Initial version (default: 1)
    - `:previous_version_id` - Previous version ID (default: nil)
    - `:acl_entries` - Extended ACL entries (default: [])
    - `:default_acl` - Default ACL for directories (default: nil)
    - `:metadata` - Arbitrary key-value metadata map (default: %{})

  ## Examples
      iex> FileMeta.new("vol1", "/test.txt")
      %FileMeta{volume_id: "vol1", path: "/test.txt", ...}
  """
  @spec new(String.t(), String.t(), keyword()) :: t()
  def new(volume_id, path, opts \\ []) do
    now = DateTime.utc_now()
    normalized = normalize_path(path)

    %__MODULE__{
      id: Keyword.get(opts, :id, generate_id()),
      volume_id: volume_id,
      path: normalized,
      chunks: Keyword.get(opts, :chunks, []),
      stripes: nil,
      size: Keyword.get(opts, :size, 0),
      content_type: Keyword.get(opts, :content_type, MIME.from_path(normalized)),
      mode: Keyword.get(opts, :mode, 0o644),
      uid: Keyword.get(opts, :uid, 0),
      gid: Keyword.get(opts, :gid, 0),
      acl_entries: Keyword.get(opts, :acl_entries, []),
      default_acl: Keyword.get(opts, :default_acl),
      metadata: Keyword.get(opts, :metadata, %{}),
      created_at: now,
      modified_at: now,
      accessed_at: now,
      changed_at: now,
      version: Keyword.get(opts, :version, 1),
      previous_version_id: Keyword.get(opts, :previous_version_id)
    }
  end

  @doc """
  Updates file metadata with new values.

  Returns a new FileMeta struct with the updated fields and incremented version.

  ## Parameters
  - `file`: The original FileMeta struct
  - `updates`: Keyword list of fields to update

  ## Examples
      iex> file = FileMeta.new("vol1", "/test.txt")
      iex> FileMeta.update(file, size: 1024, mode: 0o755)
      %FileMeta{size: 1024, mode: 0o755, version: 2, ...}
  """
  @spec update(t(), keyword()) :: t()
  def update(%__MODULE__{} = file, updates) do
    # Increment version on any update
    updated_version = file.version + 1

    # Update modified timestamp
    now = DateTime.utc_now()

    # Merge updates with version and timestamp changes
    updates_with_meta =
      updates
      |> Keyword.put(:version, updated_version)
      |> Keyword.put(:modified_at, now)
      |> Keyword.put(:changed_at, now)

    struct(file, updates_with_meta)
  end

  @doc """
  Updates the accessed_at timestamp without incrementing version.

  ## Examples
      iex> file = FileMeta.new("vol1", "/test.txt")
      iex> FileMeta.touch(file)
      %FileMeta{accessed_at: ~U[2024-01-28 ...], ...}
  """
  @spec touch(t()) :: t()
  def touch(%__MODULE__{} = file) do
    %{file | accessed_at: DateTime.utc_now()}
  end

  @doc """
  Validates a file path.

  Returns `:ok` if valid, `{:error, reason}` if invalid.

  ## Rules
  - Must start with "/"
  - Cannot contain ".." (no parent directory references)
  - Cannot end with "/" unless it's the root path
  - Cannot be empty

  ## Examples
      iex> FileMeta.validate_path("/valid/path.txt")
      :ok

      iex> FileMeta.validate_path("no-leading-slash")
      {:error, :invalid_path}

      iex> FileMeta.validate_path("/../escape")
      {:error, :invalid_path}
  """
  @spec validate_path(String.t()) :: :ok | {:error, :invalid_path}
  def validate_path(path) when is_binary(path) do
    cond do
      path == "" ->
        {:error, :invalid_path}

      not String.starts_with?(path, "/") ->
        {:error, :invalid_path}

      String.contains?(path, "..") ->
        {:error, :invalid_path}

      path != "/" and String.ends_with?(path, "/") ->
        {:error, :invalid_path}

      true ->
        :ok
    end
  end

  @doc """
  Normalizes a file path by removing trailing slashes.

  ## Examples
      iex> FileMeta.normalize_path("/test/path/")
      "/test/path"

      iex> FileMeta.normalize_path("/")
      "/"
  """
  @spec normalize_path(String.t()) :: String.t()
  def normalize_path("/"), do: "/"

  def normalize_path(path) do
    path |> String.trim_trailing("/")
  end

  @doc """
  Extracts the parent directory path from a file path.

  ## Examples
      iex> FileMeta.parent_path("/documents/report.pdf")
      "/documents"

      iex> FileMeta.parent_path("/test.txt")
      "/"

      iex> FileMeta.parent_path("/")
      nil
  """
  @spec parent_path(String.t()) :: String.t() | nil
  def parent_path("/"), do: nil

  def parent_path(path) do
    case String.split(path, "/") |> Enum.drop(-1) do
      [""] -> "/"
      parts -> Enum.join(parts, "/")
    end
  end

  # Private helpers

  defp generate_id do
    UUIDv7.generate()
  end
end
