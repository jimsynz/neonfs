defmodule NeonFS.S3.Test.MockCore do
  @moduledoc """
  In-memory mock of NeonFS.Core operations for testing the S3 backend
  without requiring a running core cluster.

  Uses the process dictionary to store state per-test, avoiding ETS
  table name collisions between concurrent tests.
  """

  alias NeonFS.Core.FileMeta
  alias NeonFS.Core.Volume

  @spec setup :: :ok
  def setup do
    Process.put(:mock_volumes, %{})
    Process.put(:mock_files, %{})
    Process.put(:mock_credentials, %{})
    :ok
  end

  # Volume operations

  @spec create_volume(String.t(), keyword()) :: {:ok, Volume.t()} | {:error, term()}
  def create_volume(name, _opts \\ []) do
    volumes = Process.get(:mock_volumes, %{})

    if Map.has_key?(volumes, name) do
      {:error, :already_exists}
    else
      volume = Volume.new(name)
      Process.put(:mock_volumes, Map.put(volumes, name, volume))
      {:ok, volume}
    end
  end

  @spec delete_volume(String.t()) :: :ok | {:error, term()}
  def delete_volume(name) do
    volumes = Process.get(:mock_volumes, %{})

    if Map.has_key?(volumes, name) do
      Process.put(:mock_volumes, Map.delete(volumes, name))
      :ok
    else
      {:error, :not_found}
    end
  end

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

  @spec volume_exists?(String.t()) :: boolean()
  def volume_exists?(name) do
    volumes = Process.get(:mock_volumes, %{})
    Map.has_key?(volumes, name)
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
        [size: byte_size(content)]
        |> maybe_forward_opt(write_opts, :content_type)

      meta = FileMeta.new(volume.id, normalised, file_opts)

      key = {volume_name, normalised}
      Process.put(:mock_files, Map.put(files, key, {meta, content}))
      {:ok, meta}
    else
      {:error, :not_found}
    end
  end

  @spec read_file(String.t(), String.t(), keyword()) :: {:ok, binary()} | {:error, :not_found}
  def read_file(volume_name, path, opts \\ []) do
    files = Process.get(:mock_files, %{})
    key = {volume_name, normalise_path(path)}

    case Map.get(files, key) do
      {_meta, content} ->
        offset = Keyword.get(opts, :offset, 0)
        length = Keyword.get(opts, :length, :all)
        {:ok, slice_content(content, offset, length)}

      nil ->
        {:error, :not_found}
    end
  end

  defp slice_content(content, 0, :all), do: content

  defp slice_content(content, offset, :all) do
    if offset >= byte_size(content),
      do: <<>>,
      else: binary_part(content, offset, byte_size(content) - offset)
  end

  defp slice_content(content, offset, length) do
    if offset >= byte_size(content) do
      <<>>
    else
      actual_length = min(length, byte_size(content) - offset)
      binary_part(content, offset, actual_length)
    end
  end

  @spec read_file_stream(String.t(), String.t(), keyword()) ::
          {:ok, %{stream: Enumerable.t(), file_size: non_neg_integer()}} | {:error, :not_found}
  def read_file_stream(volume_name, path, opts \\ []) do
    files = Process.get(:mock_files, %{})
    key = {volume_name, normalise_path(path)}

    case Map.get(files, key) do
      {meta, content} ->
        sliced =
          slice_content(content, Keyword.get(opts, :offset, 0), Keyword.get(opts, :length, :all))

        stream =
          Stream.unfold(sliced, fn
            <<>> -> nil
            data -> {data, <<>>}
          end)

        {:ok, %{stream: stream, file_size: meta.size}}

      nil ->
        {:error, :not_found}
    end
  end

  @spec delete_file(String.t(), String.t()) :: :ok | {:error, :not_found}
  def delete_file(volume_name, path) do
    files = Process.get(:mock_files, %{})
    key = {volume_name, normalise_path(path)}

    if Map.has_key?(files, key) do
      Process.put(:mock_files, Map.delete(files, key))
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

  @spec exists?(String.t(), String.t()) :: boolean()
  def exists?(volume_name, path) do
    files = Process.get(:mock_files, %{})
    key = {volume_name, normalise_path(path)}
    Map.has_key?(files, key)
  end

  @spec lookup_s3_credential(String.t()) :: {:ok, map()} | {:error, :not_found}
  def lookup_s3_credential(access_key_id) do
    creds = Process.get(:mock_credentials, %{})

    case Map.get(creds, access_key_id) do
      nil -> {:error, :not_found}
      cred -> {:ok, cred}
    end
  end

  @spec add_credential(String.t(), String.t()) :: :ok
  def add_credential(access_key_id, secret_access_key) do
    creds = Process.get(:mock_credentials, %{})

    cred = %{
      secret_access_key: secret_access_key,
      identity: %{user: access_key_id}
    }

    Process.put(:mock_credentials, Map.put(creds, access_key_id, cred))
    :ok
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
end
