defmodule NeonFS.Core.DRSnapshot do
  @moduledoc """
  Point-in-time DR snapshot of cluster metadata + CA state, written to
  the `_system` volume.

  ## What gets captured

  Per-index files under `/dr/<timestamp>/<index>.snapshot`, plus the
  cluster-wide CA material under `/dr/<timestamp>/ca/<file>`, plus a
  `manifest.json` that lists every file with its byte length and
  SHA-256 digest. Indexes:

    * `chunks` — `ChunkIndex` rows
    * `files` — `FileIndex` rows
    * `stripes` — `StripeIndex` rows
    * `volumes` — `VolumeRegistry` rows
    * `services` — `ServiceRegistry` rows
    * `segment_assignments` — consistent-hash → replica-set table
    * `encryption_keys` — `KeyManager` wrapped keys
    * `volume_acls` — per-volume ACL tables
    * `s3_credentials` — `NeonFS.S3.Credential` records
    * `escalations` — `NeonFS.Core.Escalation` records
    * `kv` — generic `KVStore` table

  ## Consistency model

  A single `RaSupervisor.local_query/2` call returns the entire
  `MetadataStateMachine` state at one Ra log index — every sub-map is
  read from the same committed point, so the per-index snapshots are
  cluster-internally consistent without blocking writes.

  CA material lives in the `_system` volume itself
  (`/tls/ca.crt`, `/tls/serial`, optional `/tls/crl.pem`). Those reads
  happen after the Ra query and may reflect a later write; the manifest
  records the read time so an operator can spot a near-rotation
  snapshot if it matters. The CA files are tiny (a few KiB) so the
  brief copy is bounded — no streaming gymnastics needed.

  ## Streaming

  Each per-index file is a sequence of length-prefixed ETF terms
  (`<<size::big-32, term>>` per entry) so a restorer can iterate
  without materialising the whole file. We compute the SHA-256
  digest in the same pass we serialise — see `serialise_index/2`.

  ## Out of scope (for this slice)

    * Scheduling / retention (lives in #323).
    * CLI surface (lives in #324).
    * Restore — separate runbook + restore command (`#446`-equivalent).
  """

  alias NeonFS.Core.{RaSupervisor, SystemVolume, VolumeRegistry, WriteOperation}

  require Logger

  @snapshot_root "/dr"
  @ca_files ["/tls/ca.crt", "/tls/serial", "/tls/crl.pem"]

  @indexes_to_capture [
    :chunks,
    :files,
    :stripes,
    :volumes,
    :services,
    :segment_assignments,
    :encryption_keys,
    :volume_acls,
    :s3_credentials,
    :escalations,
    :kv
  ]

  @typedoc "One file's manifest entry."
  @type manifest_entry :: %{
          path: String.t(),
          bytes: non_neg_integer(),
          sha256: String.t(),
          kind: :index | :ca
        }

  @typedoc "Full snapshot manifest."
  @type manifest :: %{
          version: pos_integer(),
          created_at: String.t(),
          state_version: non_neg_integer() | nil,
          files: [manifest_entry()]
        }

  @doc """
  Capture a snapshot. Returns `{:ok, %{path: dir, manifest: ...}}` on
  success, where `dir` is the snapshot directory under the `_system`
  volume.

  ## Options

    * `:timestamp` — override the directory name (default:
      `DateTime.utc_now/0` formatted as `YYYY-MM-DD-HHMMSS`).
    * `:state` — pre-fetched state (test seam; production callers
      omit this so the function does its own `local_query/1`).
  """
  @spec create(keyword()) ::
          {:ok, %{path: String.t(), manifest: manifest()}} | {:error, term()}
  def create(opts \\ []) do
    timestamp = Keyword.get_lazy(opts, :timestamp, &default_timestamp/0)
    snapshot_dir = Path.join(@snapshot_root, timestamp)

    with {:ok, state} <- fetch_state(opts),
         {:ok, volume} <- get_system_volume(),
         {:ok, index_entries} <- write_indexes(volume.id, snapshot_dir, state),
         {:ok, ca_entries} <- copy_ca_files(volume.id, snapshot_dir),
         manifest <- build_manifest(timestamp, state, index_entries ++ ca_entries),
         :ok <- write_manifest(volume.id, snapshot_dir, manifest) do
      Logger.info("DR snapshot created",
        path: snapshot_dir,
        files: length(manifest.files),
        state_version: manifest.state_version
      )

      {:ok, %{path: snapshot_dir, manifest: manifest}}
    end
  end

  ## Private

  defp default_timestamp do
    DateTime.utc_now()
    |> DateTime.to_iso8601(:basic)
    |> String.replace(~r/[^0-9TZ]/, "")
  end

  defp fetch_state(opts) do
    case Keyword.fetch(opts, :state) do
      {:ok, state} -> {:ok, state}
      :error -> RaSupervisor.local_query(fn state -> state end)
    end
  end

  defp get_system_volume do
    case VolumeRegistry.get_by_name("_system") do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :system_volume_not_found}
    end
  end

  defp write_indexes(volume_id, snapshot_dir, state) do
    @indexes_to_capture
    |> Enum.reduce_while({:ok, []}, fn name, {:ok, acc} ->
      map = Map.get(state, name, %{})
      path = Path.join(snapshot_dir, "#{name}.snapshot")

      case write_index(volume_id, path, map) do
        {:ok, entry} -> {:cont, {:ok, [entry | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      err -> err
    end
  end

  # Serialise a single index map as a stream of length-prefixed ETF
  # records and feed it to `WriteOperation.write_file_streamed/4`. The
  # SHA-256 digest is computed in the same pass via a tee-style
  # transform.
  defp write_index(volume_id, path, map) do
    {iolist, hasher, total_bytes} = serialise_index(map)
    digest = :crypto.hash_final(hasher) |> Base.encode16(case: :lower)
    # The streaming write path's chunker NIF expects binaries, not
    # arbitrary iolists — flatten before handing over.
    chunks = [:erlang.iolist_to_binary(iolist)]

    case WriteOperation.write_file_streamed(volume_id, path, chunks) do
      {:ok, _file_meta} ->
        {:ok, %{path: path, bytes: total_bytes, sha256: digest, kind: :index}}

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Serialise an index map as an `iolist` of length-prefixed ETF
  records, plus the running SHA-256 hasher and total byte count.

  Public so tests can exercise the round-trip without going through
  the `_system` volume.
  """
  @spec serialise_index(map()) :: {iodata(), :crypto.hash_state(), non_neg_integer()}
  def serialise_index(map) when is_map(map) do
    initial = {[], :crypto.hash_init(:sha256), 0}

    map
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce(initial, fn entry, {acc, hasher, total} ->
      bin = :erlang.term_to_binary(entry)
      framed = <<byte_size(bin)::big-32, bin::binary>>
      {[acc, framed], :crypto.hash_update(hasher, framed), total + byte_size(framed)}
    end)
  end

  @doc """
  Inverse of `serialise_index/1` — decode a flat binary back to its
  list of entries. Test helper; restore code paths build on this.
  """
  @spec deserialise_index(binary()) :: {:ok, [{term(), term()}]} | {:error, :malformed}
  def deserialise_index(binary) when is_binary(binary), do: do_decode(binary, [])

  defp do_decode(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp do_decode(<<size::big-32, bin::binary-size(size), rest::binary>>, acc) do
    do_decode(rest, [:erlang.binary_to_term(bin, [:safe]) | acc])
  rescue
    ArgumentError -> {:error, :malformed}
  end

  defp do_decode(_, _), do: {:error, :malformed}

  defp copy_ca_files(volume_id, snapshot_dir) do
    @ca_files
    |> Enum.reduce_while({:ok, []}, fn ca_path, {:ok, acc} ->
      case copy_ca_file(volume_id, snapshot_dir, ca_path) do
        :skip -> {:cont, {:ok, acc}}
        {:ok, entry} -> {:cont, {:ok, [entry | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      err -> err
    end
  end

  defp copy_ca_file(volume_id, snapshot_dir, source_path) do
    case SystemVolume.read(source_path) do
      {:ok, content} ->
        target = Path.join([snapshot_dir, "ca", Path.basename(source_path)])
        digest = :crypto.hash(:sha256, content) |> Base.encode16(case: :lower)

        case WriteOperation.write_file_streamed(volume_id, target, [content]) do
          {:ok, _} ->
            {:ok, %{path: target, bytes: byte_size(content), sha256: digest, kind: :ca}}

          {:error, _} = err ->
            err
        end

      {:error, %{class: :not_found}} ->
        :skip

      {:error, :not_found} ->
        :skip

      {:error, _} = err ->
        err
    end
  end

  defp build_manifest(timestamp, state, file_entries) do
    %{
      version: 1,
      created_at: timestamp,
      state_version: Map.get(state, :version),
      files: file_entries
    }
  end

  defp write_manifest(volume_id, snapshot_dir, manifest) do
    # Streaming write expects an Enumerable of binary chunks; flatten
    # the JSON encoder's iolist before handing it over.
    body = manifest |> Jason.encode!() |> :erlang.iolist_to_binary()
    target = Path.join(snapshot_dir, "manifest.json")

    case WriteOperation.write_file_streamed(volume_id, target, [body]) do
      {:ok, _} -> :ok
      {:error, _} = err -> err
    end
  end
end
