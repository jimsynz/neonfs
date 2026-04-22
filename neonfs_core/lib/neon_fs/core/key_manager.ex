defmodule NeonFS.Core.KeyManager do
  @moduledoc """
  Per-volume encryption key lifecycle management.

  Stateless functions for generating, wrapping, unwrapping, and rotating
  AES-256 volume keys. Volume keys are wrapped (encrypted) with the cluster
  master key using AES-256-GCM before storage in Ra via MetadataStateMachine
  commands.

  Reads go through `RaSupervisor.local_query/2` — every core node has
  the same committed state after `apply/3`, so the leader round-trip
  of a consistent-query is unnecessary overhead for key lookups.
  Writes still go through `RaSupervisor.command/2` (leader-routed).

  The master key is read from the local cluster.json on each operation to
  minimise exposure window — it is never cached in process state or ETS.

  ## Key wrapping format

  The `wrapped_key` binary stored in Ra has the format:

      <<nonce::binary-12, ciphertext::binary-32, tag::binary-16>>

  where `nonce` is a random 96-bit IV, `ciphertext` is the AES-256-GCM
  encrypted volume key, and `tag` is the 128-bit authentication tag.
  The volume ID and key version are included as AAD (additional authenticated
  data) to prevent key entry reuse across volumes or versions.
  """

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.RaSupervisor

  @aes_key_size 32
  @nonce_size 12
  @tag_size 16

  @doc """
  Generates a new volume key, wraps it with the master key, and stores
  the wrapped key in Ra.

  Returns the assigned key version number on success.
  """
  @spec generate_volume_key(binary()) :: {:ok, pos_integer()} | {:error, term()}
  def generate_volume_key(volume_id) do
    with {:ok, master_key} <- load_master_key(),
         {:ok, next_version} <- next_key_version(volume_id) do
      volume_key = :crypto.strong_rand_bytes(@aes_key_size)
      wrapped_key = wrap_key(volume_key, master_key, volume_id, next_version)

      wrapped_entry = %{
        wrapped_key: wrapped_key,
        created_at: DateTime.utc_now(),
        deprecated_at: nil
      }

      case RaSupervisor.command({:put_encryption_key, volume_id, next_version, wrapped_entry}) do
        {:ok, :ok, _leader} -> {:ok, next_version}
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Retrieves and unwraps a specific key version for a volume.

  Reads the wrapped key from Ra and decrypts it with the cluster master key.
  """
  @spec get_volume_key(binary(), pos_integer()) :: {:ok, binary()} | {:error, term()}
  def get_volume_key(volume_id, key_version) do
    with {:ok, master_key} <- load_master_key(),
         {:ok, wrapped_entry} <- get_wrapped_entry(volume_id, key_version) do
      unwrap_key(wrapped_entry.wrapped_key, master_key, volume_id, key_version)
    end
  end

  @doc """
  Retrieves the current key and its version for a volume.

  Determines the current key version from the volume's encryption config
  in Ra, then unwraps that key.
  """
  @spec get_current_key(binary()) :: {:ok, {binary(), pos_integer()}} | {:error, term()}
  def get_current_key(volume_id) do
    with {:ok, current_version} <- current_key_version(volume_id),
         {:ok, key} <- get_volume_key(volume_id, current_version) do
      {:ok, {key, current_version}}
    end
  end

  @doc """
  Sets up encryption for a volume by generating the first key (version 1)
  and updating the volume's current key version in Ra.

  Called during encrypted volume creation.
  """
  @spec setup_volume_encryption(binary()) :: {:ok, pos_integer()} | {:error, term()}
  def setup_volume_encryption(volume_id) do
    with {:ok, version} <- generate_volume_key(volume_id),
         {:ok, :ok, _leader} <-
           RaSupervisor.command({:set_current_key_version, volume_id, version}) do
      {:ok, version}
    end
  end

  @doc """
  Rotates the volume key by generating a new version and updating the
  volume's current key version.

  The previous key is marked as deprecated but retained for decrypting
  existing chunks. Re-encryption of chunks is handled by task 0074.
  """
  @spec rotate_volume_key(binary()) :: {:ok, pos_integer()} | {:error, term()}
  def rotate_volume_key(volume_id) do
    with {:ok, old_version} <- current_key_version(volume_id),
         {:ok, new_version} <- generate_volume_key(volume_id),
         :ok <- deprecate_key_version(volume_id, old_version),
         {:ok, :ok, _leader} <-
           RaSupervisor.command({:set_current_key_version, volume_id, new_version}) do
      {:ok, new_version}
    end
  end

  # Private helpers

  defp load_master_key do
    case ClusterState.load() do
      {:ok, %{master_key: encoded}} ->
        case Base.decode64(encoded) do
          {:ok, <<key::binary-size(@aes_key_size)>>} -> {:ok, key}
          {:ok, _} -> {:error, :invalid_master_key}
          :error -> {:error, :invalid_master_key}
        end

      {:error, :not_found} ->
        {:error, :master_key_not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp wrap_key(plaintext_key, master_key, volume_id, key_version) do
    nonce = :crypto.strong_rand_bytes(@nonce_size)
    aad = build_aad(volume_id, key_version)

    {ciphertext, tag} =
      :crypto.crypto_one_time_aead(:aes_256_gcm, master_key, nonce, plaintext_key, aad, true)

    <<nonce::binary, ciphertext::binary, tag::binary>>
  end

  defp unwrap_key(wrapped_key, master_key, volume_id, key_version) do
    ciphertext_size = byte_size(wrapped_key) - @nonce_size - @tag_size

    if ciphertext_size > 0 do
      <<nonce::binary-size(@nonce_size), ciphertext::binary-size(ciphertext_size),
        tag::binary-size(@tag_size)>> = wrapped_key

      aad = build_aad(volume_id, key_version)

      case :crypto.crypto_one_time_aead(
             :aes_256_gcm,
             master_key,
             nonce,
             ciphertext,
             aad,
             tag,
             false
           ) do
        :error -> {:error, :unwrap_failed}
        plaintext -> {:ok, plaintext}
      end
    else
      {:error, :invalid_wrapped_key}
    end
  end

  defp build_aad(volume_id, key_version) do
    <<"neonfs-key-wrap:", volume_id::binary, ":", Integer.to_string(key_version)::binary>>
  end

  defp next_key_version(volume_id) do
    case RaSupervisor.local_query(fn state ->
           MetadataStateMachine.get_encryption_keys(state, volume_id)
         end) do
      {:ok, nil} ->
        {:ok, 1}

      {:ok, keys_map} when is_map(keys_map) ->
        max_version = keys_map |> Map.keys() |> Enum.max(fn -> 0 end)
        {:ok, max_version + 1}

      {:error, _} = error ->
        error
    end
  end

  defp current_key_version(volume_id) do
    case query_volume_encryption(volume_id) do
      {:ok, %{current_key_version: version}} when is_integer(version) and version >= 1 ->
        {:ok, version}

      {:ok, _} ->
        {:error, :not_encrypted}

      {:error, _} = error ->
        error
    end
  end

  defp query_volume_encryption(volume_id) do
    case RaSupervisor.local_query(&Map.get(&1.volumes, volume_id)) do
      {:ok, nil} -> {:error, :volume_not_found}
      {:ok, volume_data} -> {:ok, Map.get(volume_data, :encryption, %{})}
      {:error, _} = error -> error
    end
  end

  defp get_wrapped_entry(volume_id, key_version) do
    case RaSupervisor.local_query(fn state ->
           MetadataStateMachine.get_encryption_keys(state, volume_id)
         end) do
      {:ok, nil} ->
        {:error, :unknown_key_version}

      {:ok, keys_map} ->
        case Map.get(keys_map, key_version) do
          nil -> {:error, :unknown_key_version}
          entry -> {:ok, entry}
        end

      {:error, _} = error ->
        error
    end
  end

  defp deprecate_key_version(volume_id, key_version) do
    case get_wrapped_entry(volume_id, key_version) do
      {:ok, entry} ->
        deprecated_entry = %{entry | deprecated_at: DateTime.utc_now()}

        case RaSupervisor.command({:put_encryption_key, volume_id, key_version, deprecated_entry}) do
          {:ok, :ok, _leader} -> :ok
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end
  end
end
