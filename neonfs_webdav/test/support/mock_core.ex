defmodule NeonFS.WebDAV.Test.MockCore do
  @moduledoc """
  In-memory mock of NeonFS.Core operations for testing the WebDAV backend
  without requiring a running core cluster.

  Uses the process dictionary to store state per-test, avoiding ETS
  table name collisions between concurrent tests.
  """

  alias NeonFS.Core.FileMeta
  alias NeonFS.Core.Volume

  @s_ifdir 0o040755
  @s_ifreg 0o100644

  @spec setup :: :ok
  def setup do
    Process.put(:mock_volumes, %{})
    Process.put(:mock_files, %{})
    :ok
  end

  # Volume operations

  @spec list_volumes :: {:ok, [Volume.t()]}
  def list_volumes do
    volumes =
      Process.get(:mock_volumes, %{})
      |> Map.values()
      |> Enum.sort_by(& &1.name)

    {:ok, volumes}
  end

  @spec get_volume(String.t()) :: {:ok, Volume.t()} | {:error, :not_found}
  def get_volume(name) do
    volumes = Process.get(:mock_volumes, %{})

    case Map.get(volumes, name) do
      nil -> {:error, :not_found}
      volume -> {:ok, volume}
    end
  end

  @spec create_volume(String.t()) :: {:ok, Volume.t()} | {:error, :already_exists}
  def create_volume(name) do
    volumes = Process.get(:mock_volumes, %{})

    if Map.has_key?(volumes, name) do
      {:error, :already_exists}
    else
      volume = Volume.new(name)
      Process.put(:mock_volumes, Map.put(volumes, name, volume))
      {:ok, volume}
    end
  end

  # File operations

  @spec write_file(String.t(), String.t(), binary(), keyword()) ::
          {:ok, FileMeta.t()} | {:error, term()}
  def write_file(volume_name, path, content, write_opts \\ []) do
    volumes = Process.get(:mock_volumes, %{})

    if Map.has_key?(volumes, volume_name) do
      files = Process.get(:mock_files, %{})
      volume = Map.get(volumes, volume_name)
      normalised = normalise_path(path)

      file_opts =
        [size: byte_size(content), mode: @s_ifreg]
        |> maybe_forward_opt(write_opts, :content_type)

      meta = FileMeta.new(volume.id, normalised, file_opts)

      key = {volume_name, normalised}
      Process.put(:mock_files, Map.put(files, key, {meta, content}))
      {:ok, meta}
    else
      {:error, :not_found}
    end
  end

  @spec read_file(String.t(), String.t()) :: {:ok, binary()} | {:error, :not_found}
  def read_file(volume_name, path) do
    files = Process.get(:mock_files, %{})
    key = {volume_name, normalise_path(path)}

    case Map.get(files, key) do
      {_meta, content} -> {:ok, content}
      nil -> {:error, :not_found}
    end
  end

  @spec delete_file(String.t(), String.t()) :: :ok | {:error, :not_found}
  def delete_file(volume_name, path) do
    files = Process.get(:mock_files, %{})
    normalised = normalise_path(path)
    key = {volume_name, normalised}

    if Map.has_key?(files, key) do
      # Also delete children for directories
      updated =
        files
        |> Enum.reject(fn {{vol, fp}, _} ->
          vol == volume_name and
            (fp == normalised or String.starts_with?(fp, normalised <> "/"))
        end)
        |> Map.new()

      Process.put(:mock_files, updated)
      :ok
    else
      {:error, :not_found}
    end
  end

  @spec get_file_meta(String.t(), String.t()) :: {:ok, FileMeta.t()} | {:error, :not_found}
  def get_file_meta(volume_name, path) do
    files = Process.get(:mock_files, %{})
    key = {volume_name, normalise_path(path)}

    case Map.get(files, key) do
      {meta, _content} -> {:ok, meta}
      nil -> {:error, :not_found}
    end
  end

  @spec update_file_meta(String.t(), String.t(), keyword()) ::
          {:ok, FileMeta.t()} | {:error, :not_found}
  def update_file_meta(volume_name, path, updates) do
    files = Process.get(:mock_files, %{})
    key = {volume_name, normalise_path(path)}

    case Map.get(files, key) do
      {meta, content} ->
        updated_meta = FileMeta.update(meta, updates)
        Process.put(:mock_files, Map.put(files, key, {updated_meta, content}))
        {:ok, updated_meta}

      nil ->
        {:error, :not_found}
    end
  end

  @spec list_files_recursive(String.t(), String.t()) ::
          {:ok, [FileMeta.t()]} | {:error, :not_found}
  def list_files_recursive(volume_name, path \\ "/") do
    volumes = Process.get(:mock_volumes, %{})

    if Map.has_key?(volumes, volume_name) do
      files = Process.get(:mock_files, %{})
      normalised = normalise_path(path)

      entries =
        files
        |> Enum.filter(fn {{vol, file_path}, _} ->
          vol == volume_name and file_path != normalised and
            String.starts_with?(file_path, normalised)
        end)
        |> Enum.map(fn {_key, {meta, _content}} -> meta end)
        |> Enum.sort_by(& &1.path)

      {:ok, entries}
    else
      {:error, :not_found}
    end
  end

  @spec list_dir(String.t(), String.t()) :: {:ok, [FileMeta.t()]} | {:error, :not_found}
  def list_dir(volume_name, path \\ "/") do
    volumes = Process.get(:mock_volumes, %{})

    if Map.has_key?(volumes, volume_name) do
      files = Process.get(:mock_files, %{})
      normalised = normalise_path(path)

      entries =
        files
        |> Enum.filter(fn {{vol, file_path}, _} ->
          vol == volume_name and direct_child?(normalised, file_path)
        end)
        |> Enum.map(fn {_key, {meta, _content}} -> meta end)
        |> Enum.sort_by(& &1.path)

      {:ok, entries}
    else
      {:error, :not_found}
    end
  end

  @spec mkdir(String.t(), String.t()) :: {:ok, FileMeta.t()} | {:error, term()}
  def mkdir(volume_name, path) do
    volumes = Process.get(:mock_volumes, %{})

    if Map.has_key?(volumes, volume_name) do
      files = Process.get(:mock_files, %{})
      volume = Map.get(volumes, volume_name)
      normalised = normalise_path(path)
      key = {volume_name, normalised}

      if Map.has_key?(files, key) do
        {:error, :already_exists}
      else
        meta =
          FileMeta.new(volume.id, normalised,
            size: 0,
            mode: @s_ifdir
          )

        Process.put(:mock_files, Map.put(files, key, {meta, <<>>}))
        {:ok, meta}
      end
    else
      {:error, :not_found}
    end
  end

  @spec rename_file(String.t(), String.t(), String.t()) :: :ok | {:error, :not_found}
  def rename_file(volume_name, old_path, new_path) do
    files = Process.get(:mock_files, %{})
    old_normalised = normalise_path(old_path)
    new_normalised = normalise_path(new_path)
    old_key = {volume_name, old_normalised}

    if Map.has_key?(files, old_key) do
      {meta, content} = Map.get(files, old_key)
      updated_meta = %{meta | path: new_normalised}

      new_key = {volume_name, new_normalised}

      updated =
        files
        |> Map.delete(old_key)
        |> Map.put(new_key, {updated_meta, content})

      Process.put(:mock_files, updated)
      :ok
    else
      {:error, :not_found}
    end
  end

  # Helpers

  defp normalise_path("/"), do: "/"

  defp normalise_path(path) do
    path
    |> String.trim_trailing("/")
    |> then(fn
      "/" <> _ = p -> p
      p -> "/" <> p
    end)
  end

  defp maybe_forward_opt(target, source, key) do
    case Keyword.fetch(source, key) do
      {:ok, value} -> Keyword.put(target, key, value)
      :error -> target
    end
  end

  defp direct_child?("/", file_path) do
    trimmed = String.trim_leading(file_path, "/")
    trimmed != "" and not String.contains?(trimmed, "/")
  end

  defp direct_child?(parent, file_path) do
    parent_with_slash = if String.ends_with?(parent, "/"), do: parent, else: parent <> "/"

    String.starts_with?(file_path, parent_with_slash) and
      not String.contains?(String.trim_leading(file_path, parent_with_slash), "/")
  end
end
