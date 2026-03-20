defmodule NeonFS.NFS.MockCore do
  @moduledoc """
  In-memory mock for NeonFS.Core services used by the NFS Handler.

  Provides a `core_call_fn` that routes calls to mock implementations of
  VolumeRegistry, FileIndex, ReadOperation, and WriteOperation. All state
  is kept in an ETS table scoped to the test.

  ## Usage

      mock = MockCore.start(volumes: ["testvol"])
      handler_opts = [core_call_fn: mock.core_call_fn]
  """

  import Bitwise

  @s_ifdir 0o040000
  @s_iflnk 0o120000
  @s_ifreg 0o100000

  defstruct [:table, :core_call_fn]

  @doc """
  Start a new mock core with the given options.

  Options:
  - `:volumes` - list of volume names to register (default: `[]`)
  """
  @spec start(keyword()) :: %__MODULE__{}
  def start(opts \\ []) do
    table = :ets.new(:mock_core, [:set, :public])
    volumes = Keyword.get(opts, :volumes, [])

    :ets.insert(table, {:volumes, volumes})
    :ets.insert(table, {:next_id, 1})

    core_call_fn = fn module, function, args ->
      dispatch(table, module, function, args)
    end

    %__MODULE__{table: table, core_call_fn: core_call_fn}
  end

  @doc """
  Stop the mock core and clean up the ETS table.
  """
  @spec stop(%__MODULE__{}) :: :ok
  def stop(%__MODULE__{table: table}) do
    :ets.delete(table)
    :ok
  end

  ## Dispatch

  defp dispatch(table, NeonFS.Core.VolumeRegistry, :list, []) do
    [{:volumes, volumes}] = :ets.lookup(table, :volumes)
    Enum.map(volumes, fn name -> %{name: name, id: name} end)
  end

  defp dispatch(table, NeonFS.Core.VolumeRegistry, :get_by_name, [name]) do
    [{:volumes, volumes}] = :ets.lookup(table, :volumes)

    if name in volumes do
      {:ok, %{name: name, id: name}}
    else
      {:error, :not_found}
    end
  end

  defp dispatch(table, NeonFS.Core.FileIndex, :get_by_path, [volume, path]) do
    case :ets.lookup(table, {:file, volume, path}) do
      [{_, file}] -> {:ok, file}
      [] -> {:error, :not_found}
    end
  end

  defp dispatch(table, NeonFS.Core.FileIndex, :list_volume, [volume]) do
    :ets.foldl(
      fn
        {{:file, ^volume, _path}, file}, acc -> [file | acc]
        _, acc -> acc
      end,
      [],
      table
    )
  end

  defp dispatch(table, NeonFS.Core.FileIndex, :list_dir, [volume, dir_path]) do
    prefix = String.trim_trailing(dir_path, "/") <> "/"

    children =
      :ets.foldl(
        fn
          {{:file, ^volume, path}, file}, acc when is_binary(path) ->
            maybe_add_child(acc, path, prefix, file)

          _, acc ->
            acc
        end,
        %{},
        table
      )

    {:ok, children}
  end

  defp dispatch(table, NeonFS.Core.FileIndex, :delete, [file_id]) do
    # Find and delete the file by id
    :ets.foldl(
      fn
        {{:file, _vol, _path} = key, %{id: ^file_id}}, _acc ->
          :ets.delete(table, key)
          :ok

        _, acc ->
          acc
      end,
      {:error, :not_found},
      table
    )
  end

  defp dispatch(table, NeonFS.Core.FileIndex, :update, [file_id, updates]) do
    :ets.foldl(
      fn
        {{:file, vol, old_path} = key, %{id: ^file_id} = file}, _acc ->
          updated = Enum.reduce(updates, file, fn {k, v}, f -> Map.put(f, k, v) end)
          new_path = Keyword.get(updates, :path, old_path)
          rekey_file_entry(table, key, vol, old_path, new_path, updated)
          {:ok, updated}

        _, acc ->
          acc
      end,
      {:error, :not_found},
      table
    )
  end

  defp dispatch(table, NeonFS.Core.FileIndex, :truncate, [file_id, new_size, updates]) do
    # Truncate stored data for the file
    :ets.foldl(
      fn
        {{:file, vol, path}, %{id: ^file_id}}, _acc ->
          case :ets.lookup(table, {:data, vol, path}) do
            [{_, data}] when byte_size(data) > new_size ->
              :ets.insert(table, {{:data, vol, path}, binary_part(data, 0, new_size)})

            _ ->
              :ok
          end

        _, acc ->
          acc
      end,
      :ok,
      table
    )

    dispatch(table, NeonFS.Core.FileIndex, :update, [file_id, [{:size, new_size} | updates]])
  end

  defp dispatch(table, NeonFS.Core.ReadOperation, :read_file, [volume, path, opts]) do
    case :ets.lookup(table, {:data, volume, path}) do
      [{_, data}] ->
        offset = Keyword.get(opts, :offset, 0)
        length = Keyword.get(opts, :length, byte_size(data))

        {:ok,
         binary_part(data, min(offset, byte_size(data)), min(length, byte_size(data) - offset))}

      [] ->
        {:error, :not_found}
    end
  end

  defp dispatch(table, NeonFS.Core.WriteOperation, :create_symlink, [volume, path, target]) do
    now = DateTime.utc_now()
    file_id = next_id(table)

    file = %{
      id: file_id,
      path: path,
      size: 0,
      mode: @s_iflnk ||| 0o777,
      uid: 0,
      gid: 0,
      symlink_target: target,
      accessed_at: now,
      modified_at: now,
      changed_at: now
    }

    :ets.insert(table, {{:file, volume, path}, file})
    {:ok, file}
  end

  defp dispatch(table, NeonFS.Core.WriteOperation, :write_file, [volume, path, data, opts]) do
    mode = Keyword.get(opts, :mode, @s_ifreg ||| 0o644)
    now = DateTime.utc_now()

    file_id =
      case :ets.lookup(table, {:file, volume, path}) do
        [{_, existing}] -> existing.id
        [] -> next_id(table)
      end

    file = %{
      id: file_id,
      path: path,
      size: byte_size(data),
      mode: mode,
      uid: 0,
      gid: 0,
      accessed_at: now,
      modified_at: now,
      changed_at: now
    }

    :ets.insert(table, {{:file, volume, path}, file})
    :ets.insert(table, {{:data, volume, path}, data})
    {:ok, file}
  end

  defp dispatch(_table, module, function, _args) do
    {:error, {:mock_not_implemented, module, function}}
  end

  ## Helpers

  defp next_id(table) do
    [{:next_id, id}] = :ets.lookup(table, :next_id)
    :ets.insert(table, {:next_id, id + 1})
    id
  end

  defp rekey_file_entry(table, key, _vol, path, path, updated) do
    :ets.insert(table, {key, updated})
  end

  defp rekey_file_entry(table, key, vol, old_path, new_path, updated) do
    :ets.delete(table, key)
    :ets.insert(table, {{:file, vol, new_path}, updated})

    case :ets.lookup(table, {:data, vol, old_path}) do
      [{_, data}] ->
        :ets.delete(table, {:data, vol, old_path})
        :ets.insert(table, {{:data, vol, new_path}, data})

      [] ->
        :ok
    end
  end

  defp directory?(mode), do: (mode &&& 0o170000) == @s_ifdir

  defp maybe_add_child(acc, path, prefix, file) do
    if String.starts_with?(path, prefix) do
      rest = String.replace_prefix(path, prefix, "")
      add_direct_child(acc, rest, file)
    else
      acc
    end
  end

  defp add_direct_child(acc, rest, file) when not is_nil(rest) do
    if String.contains?(rest, "/") do
      acc
    else
      type = if directory?(file.mode), do: :dir, else: :file
      Map.put(acc, rest, %{type: type})
    end
  end
end
