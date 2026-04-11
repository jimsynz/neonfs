defmodule NeonFS.Core.S3CredentialManager do
  @moduledoc """
  Manages S3 access credentials for NeonFS.

  Maps S3 access key IDs to secret keys and user identities, enabling
  AWS SigV4 authentication in the S3-compatible API.

  Uses ETS for concurrent read access with serialised writes through GenServer.
  Backed by Ra for cluster-wide persistence and replication.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Persistence
  alias NeonFS.Core.RaServer
  alias NeonFS.Core.RaSupervisor

  @type access_key_id :: String.t()
  @type secret_access_key :: String.t()

  @type credential :: %{
          access_key_id: access_key_id(),
          secret_access_key: secret_access_key(),
          identity: term(),
          created_at: DateTime.t()
        }

  @ets_table :s3_credentials

  # Client API

  @doc """
  Creates a new S3 credential for the given user identity.

  Generates a random access key ID and secret access key, stores them,
  and returns the full credential (including the secret — only shown once).
  """
  @spec create(term()) :: {:ok, credential()} | {:error, term()}
  def create(identity) do
    GenServer.call(__MODULE__, {:create, identity}, 10_000)
  end

  @doc """
  Deletes an S3 credential by its access key ID.
  """
  @spec delete(access_key_id()) :: :ok | {:error, :not_found}
  def delete(access_key_id) do
    GenServer.call(__MODULE__, {:delete, access_key_id}, 10_000)
  end

  @doc """
  Rotates the secret access key for an existing S3 credential.

  Generates a new secret key while keeping the same access key ID and identity.
  Returns the updated credential including the new secret (shown once).
  """
  @spec rotate(access_key_id()) :: {:ok, credential()} | {:error, :not_found}
  def rotate(access_key_id) do
    GenServer.call(__MODULE__, {:rotate, access_key_id}, 10_000)
  end

  @doc """
  Lists all S3 credentials, optionally filtered by identity.

  Returns credentials without their secret keys for security.
  """
  @spec list(keyword()) :: [map()]
  def list(opts \\ []) do
    identity_filter = Keyword.get(opts, :identity)

    @ets_table
    |> :ets.tab2list()
    |> Enum.map(fn {_key, cred} -> Map.delete(cred, :secret_access_key) end)
    |> filter_by_identity(identity_filter)
    |> Enum.sort_by(& &1.created_at, DateTime)
  end

  @doc """
  Looks up a credential by access key ID.

  Returns the full credential including the secret key (needed for signature
  verification).
  """
  @spec lookup(access_key_id()) :: {:ok, credential()} | {:error, :not_found}
  def lookup(access_key_id) do
    case :ets.lookup(@ets_table, access_key_id) do
      [{^access_key_id, cred}] -> {:ok, cred}
      [] -> lookup_from_ra(access_key_id)
    end
  end

  @doc """
  Starts the S3 credential manager.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    :ets.new(@ets_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    restored =
      case restore_from_ra() do
        {:ok, count} ->
          Logger.info("S3CredentialManager started, restored credentials from Ra", count: count)
          true

        {:error, reason} ->
          Logger.debug("S3CredentialManager started but Ra not ready yet, will retry",
            reason: reason
          )

          schedule_restore_retry(1_000)
          false
      end

    {:ok, %{restored: restored, restore_backoff: 1_000}}
  end

  @impl true
  def terminate(_reason, _state) do
    Logger.info("S3CredentialManager shutting down, saving table...")
    meta_dir = Persistence.meta_dir()

    Persistence.snapshot_table(
      @ets_table,
      Path.join(meta_dir, "s3_credentials.dets")
    )

    Logger.info("S3CredentialManager table saved")
    :ok
  end

  @impl true
  def handle_call({:create, identity}, _from, state) do
    reply = do_create(identity)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, access_key_id}, _from, state) do
    reply = do_delete(access_key_id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:rotate, access_key_id}, _from, state) do
    reply = do_rotate(access_key_id)
    {:reply, reply, state}
  end

  @impl true
  def handle_info(:retry_restore_from_ra, %{restored: true} = state) do
    {:noreply, state}
  end

  def handle_info(:retry_restore_from_ra, state) do
    case restore_from_ra() do
      {:ok, count} ->
        Logger.info("S3CredentialManager restored credentials from Ra on retry", count: count)
        {:noreply, %{state | restored: true}}

      {:error, _reason} ->
        next_backoff = min(state.restore_backoff * 2, 30_000)
        schedule_restore_retry(next_backoff)
        {:noreply, %{state | restore_backoff: next_backoff}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Private helpers

  defp filter_by_identity(creds, nil), do: creds

  defp filter_by_identity(creds, identity) do
    Enum.filter(creds, fn c -> c.identity == identity end)
  end

  defp do_create(identity) do
    access_key_id = generate_access_key_id()
    secret_access_key = generate_secret_access_key()

    credential = %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      identity: identity,
      created_at: DateTime.utc_now()
    }

    case persist_credential(credential) do
      :ok -> {:ok, credential}
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_delete(access_key_id) do
    case :ets.lookup(@ets_table, access_key_id) do
      [{^access_key_id, _cred}] ->
        case delete_credential_persisted(access_key_id) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      [] ->
        {:error, :not_found}
    end
  end

  defp do_rotate(access_key_id) do
    case :ets.lookup(@ets_table, access_key_id) do
      [{^access_key_id, cred}] ->
        rotated = %{cred | secret_access_key: generate_secret_access_key()}

        case persist_credential(rotated) do
          :ok -> {:ok, rotated}
          {:error, reason} -> {:error, reason}
        end

      [] ->
        {:error, :not_found}
    end
  end

  defp persist_credential(credential) do
    case maybe_ra_command({:put_s3_credential, credential_to_map(credential)}) do
      {:ok, :ok} ->
        :ets.insert(@ets_table, {credential.access_key_id, credential})
        :ok

      {:error, :ra_not_available} ->
        :ets.insert(@ets_table, {credential.access_key_id, credential})
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp delete_credential_persisted(access_key_id) do
    case maybe_ra_command({:delete_s3_credential, access_key_id}) do
      {:ok, :ok} ->
        :ets.delete(@ets_table, access_key_id)
        :ok

      {:error, :ra_not_available} ->
        :ets.delete(@ets_table, access_key_id)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp lookup_from_ra(access_key_id) do
    query_fn = fn state ->
      state
      |> Map.get(:s3_credentials, %{})
      |> Map.get(access_key_id)
    end

    case RaSupervisor.query(query_fn) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, cred_map} -> cache_and_return(cred_map)
      {:error, _} -> {:error, :not_found}
    end
  catch
    :exit, _ -> {:error, :not_found}
  end

  defp cache_and_return(cred_map) do
    credential = map_to_credential(cred_map)
    :ets.insert(@ets_table, {credential.access_key_id, credential})
    {:ok, credential}
  end

  defp restore_from_ra do
    case RaSupervisor.query(fn state -> Map.get(state, :s3_credentials, %{}) end) do
      {:ok, creds} when is_map(creds) ->
        count =
          Enum.reduce(creds, 0, fn {_id, cred_map}, acc ->
            credential = map_to_credential(cred_map)
            :ets.insert(@ets_table, {credential.access_key_id, credential})
            acc + 1
          end)

        {:ok, count}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp maybe_ra_command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, result, _leader} ->
        {:ok, result}

      {:error, :noproc} ->
        if RaServer.initialized?() do
          {:error, :ra_unavailable}
        else
          {:error, :ra_not_available}
        end

      {:error, reason} ->
        {:error, reason}

      {:timeout, _node} ->
        {:error, :timeout}
    end
  catch
    :exit, {:noproc, _} ->
      if RaServer.initialized?() do
        {:error, :ra_unavailable}
      else
        {:error, :ra_not_available}
      end

    kind, reason ->
      Logger.debug("Ra command error", kind: kind, reason: reason)

      if RaServer.initialized?() do
        {:error, {:ra_error, {kind, reason}}}
      else
        {:error, :ra_not_available}
      end
  end

  defp schedule_restore_retry(delay_ms) do
    Process.send_after(self(), :retry_restore_from_ra, delay_ms)
  end

  defp credential_to_map(credential) do
    %{
      access_key_id: credential.access_key_id,
      secret_access_key: credential.secret_access_key,
      identity: credential.identity,
      created_at: credential.created_at
    }
  end

  defp map_to_credential(cred_map) when is_map(cred_map) do
    %{
      access_key_id: cred_map.access_key_id,
      secret_access_key: cred_map.secret_access_key,
      identity: cred_map[:identity],
      created_at: cred_map[:created_at] || DateTime.utc_now()
    }
  end

  defp generate_access_key_id do
    "NEONFS" <> random_alphanumeric(14)
  end

  defp generate_secret_access_key do
    :crypto.strong_rand_bytes(30) |> Base.encode64(padding: false)
  end

  defp random_alphanumeric(length) do
    alphabet = ~c"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    :crypto.strong_rand_bytes(length)
    |> :binary.bin_to_list()
    |> Enum.map(fn byte -> Enum.at(alphabet, rem(byte, length(alphabet))) end)
    |> List.to_string()
  end
end
