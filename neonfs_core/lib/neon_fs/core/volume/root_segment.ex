defmodule NeonFS.Core.Volume.RootSegment do
  @moduledoc """
  On-disk format for a volume's root segment.

  Each volume has exactly one *root segment*: a small ETF-encoded blob
  stored as a chunk in the volume itself, holding everything needed to
  bootstrap reads of the volume's metadata trees:

  - Cluster identity (`cluster_id`, `cluster_name`) — for crash-recovery
    of the bootstrap layer (#779) and to refuse to mount a volume whose
    chunks belong to a different cluster.
  - Versions (`format_version`, `created_by_neonfs_version`,
    `last_written_by_neonfs_version`, `on_disk_format_version`).
  - The volume's durability config (replicate/erasure parameters).
  - Per-volume HLC state.
  - Index tree roots — chunk hashes for the FileIndex / ChunkIndex /
    StripeIndex tree roots within the volume.
  - Per-volume schedules for GC, scrub, and anti-entropy.

  The bootstrap layer (#779) holds the chunk hash of the *current*
  root segment for each volume; reading the volume goes:

      bootstrap layer → root chunk hash → root segment → index tree roots
        → index tree → key/value

  Writes are copy-on-write: every metadata write produces a new root
  segment chunk, and the bootstrap layer's atomic-swap of the root
  chunk hash is the commit point. This module handles the encode /
  decode / validation half of that flow; the BlobStore round-trip and
  bootstrap-layer pointer swap live in the read/write path sub-issues
  (#784, #785) and the bootstrap layer (#779).

  ## Wire format

  The on-disk bytes are an ETF-encoded tagged tuple:

      {:neonfs_root_segment, format_version :: pos_integer(), payload :: map()}

  The tag + version live outside the payload map so a reader can detect
  an unsupported version after a single ETF decode (no need to parse
  the whole payload first). The payload's shape is documented under
  `t:t/0`.
  """

  alias NeonFS.Core.HLC

  @format_version 1
  @tag :neonfs_root_segment

  @typedoc """
  A volume's root segment.

  - `format_version` — the wire version of *this struct's* serialisation.
    Bumped when the payload schema changes; unrelated to
    `on_disk_format_version`.
  - `volume_id`, `volume_name`, `cluster_id`, `cluster_name` — identity.
  - `durability` — opaque map (`replicate` / `erasure` config; same
    shape as `NeonFS.Core.Volume.durability_config`).
  - `created_by_neonfs_version` — set once at volume create, never
    updated.
  - `last_written_by_neonfs_version` — updated on every metadata write.
  - `on_disk_format_version` — per-volume; supports staged upgrades
    (different volumes can be at different versions during a cluster
    upgrade).
  - `hlc` — per-volume HLC state, advanced on each metadata write.
  - `index_roots` — chunk hashes for the volume's three index trees.
    `nil` for an empty index that hasn't been written yet.
  - `schedules` — per-volume cadence for GC / scrub / anti-entropy.
  """
  @type t :: %__MODULE__{
          format_version: pos_integer(),
          volume_id: String.t(),
          volume_name: String.t(),
          cluster_id: String.t(),
          cluster_name: String.t(),
          durability: map(),
          created_by_neonfs_version: String.t(),
          last_written_by_neonfs_version: String.t(),
          on_disk_format_version: pos_integer(),
          hlc: map(),
          index_roots: %{
            file_index: binary() | nil,
            chunk_index: binary() | nil,
            stripe_index: binary() | nil
          },
          schedules: %{
            gc: schedule(),
            scrub: schedule(),
            anti_entropy: schedule()
          }
        }

  @typedoc """
  A schedule for one of the per-volume background jobs (GC, scrub,
  anti-entropy). `last_run` is `nil` until the first run completes.
  """
  @type schedule :: %{
          interval_ms: pos_integer(),
          last_run: DateTime.t() | nil
        }

  @type decode_error ::
          :invalid_etf
          | :not_a_root_segment
          | {:unsupported_format_version, integer()}
          | {:malformed, String.t()}

  @type cluster_mismatch :: {:cluster_mismatch, expected: String.t(), actual: String.t()}

  @enforce_keys [
    :volume_id,
    :volume_name,
    :cluster_id,
    :cluster_name,
    :durability,
    :created_by_neonfs_version,
    :last_written_by_neonfs_version,
    :hlc
  ]
  defstruct format_version: @format_version,
            volume_id: nil,
            volume_name: nil,
            cluster_id: nil,
            cluster_name: nil,
            durability: nil,
            created_by_neonfs_version: nil,
            last_written_by_neonfs_version: nil,
            on_disk_format_version: 1,
            hlc: nil,
            index_roots: %{file_index: nil, chunk_index: nil, stripe_index: nil},
            schedules: %{
              gc: %{interval_ms: 86_400_000, last_run: nil},
              scrub: %{interval_ms: 7 * 86_400_000, last_run: nil},
              anti_entropy: %{interval_ms: 60 * 60 * 1_000, last_run: nil}
            }

  @doc """
  Returns the current wire format version this module writes.
  """
  @spec format_version() :: pos_integer()
  def format_version, do: @format_version

  @doc """
  Builds a fresh root segment for a brand-new volume.

  Required keys: `volume_id`, `volume_name`, `cluster_id`,
  `cluster_name`, `durability`. Optional keys override the per-field
  defaults documented in `t:t/0`.

  Stamps `created_by_neonfs_version` and
  `last_written_by_neonfs_version` from the running `:neonfs_core`
  application's `:vsn`. Initialises a per-volume HLC seeded by
  `Node.self/0`.
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    version = current_neonfs_version()

    struct!(
      __MODULE__,
      [
        created_by_neonfs_version: version,
        last_written_by_neonfs_version: version,
        hlc: HLC.new(node())
      ] ++ opts
    )
  end

  @doc """
  Encodes a root segment to its on-disk binary form.
  """
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{format_version: version} = segment) do
    payload = %{
      volume_id: segment.volume_id,
      volume_name: segment.volume_name,
      cluster_id: segment.cluster_id,
      cluster_name: segment.cluster_name,
      durability: segment.durability,
      created_by_neonfs_version: segment.created_by_neonfs_version,
      last_written_by_neonfs_version: segment.last_written_by_neonfs_version,
      on_disk_format_version: segment.on_disk_format_version,
      hlc: hlc_to_map(segment.hlc),
      index_roots: segment.index_roots,
      schedules: schedules_to_map(segment.schedules)
    }

    :erlang.term_to_binary({@tag, version, payload})
  end

  @doc """
  Decodes the on-disk binary form back into a `t/0` struct.

  Validates the tag and format version. Returns
  `{:error, {:unsupported_format_version, version}}` for a future-
  version blob; the caller is expected to surface this to the operator
  rather than attempt an in-place upgrade (per the no-migration design
  call on epic #750).
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, decode_error()}
  def decode(bin) when is_binary(bin) do
    case safe_decode(bin) do
      {:ok, {@tag, @format_version, payload}} when is_map(payload) ->
        from_payload(payload)

      {:ok, {@tag, other_version, _payload}} when is_integer(other_version) ->
        {:error, {:unsupported_format_version, other_version}}

      {:ok, {@tag, _, _}} ->
        {:error, {:malformed, "version field is not an integer"}}

      {:ok, _} ->
        {:error, :not_a_root_segment}

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Verifies the segment's `cluster_id` matches the given expected id.

  Read paths use this to refuse to mount a volume whose chunks belong
  to a different cluster.
  """
  @spec validate_cluster(t(), String.t()) :: :ok | {:error, cluster_mismatch()}
  def validate_cluster(%__MODULE__{cluster_id: cluster_id}, expected_cluster_id)
      when cluster_id == expected_cluster_id,
      do: :ok

  def validate_cluster(%__MODULE__{cluster_id: actual}, expected),
    do: {:error, {:cluster_mismatch, expected: expected, actual: actual}}

  @doc """
  Returns a copy of the segment with `last_written_by_neonfs_version`
  refreshed to the running daemon's version. Called from the write
  path before encoding for a new chunk.
  """
  @spec touch(t()) :: t()
  def touch(%__MODULE__{} = segment) do
    %{segment | last_written_by_neonfs_version: current_neonfs_version()}
  end

  ## Private

  defp safe_decode(bin) do
    {:ok, :erlang.binary_to_term(bin, [:safe])}
  rescue
    ArgumentError -> {:error, :invalid_etf}
  end

  defp from_payload(payload) do
    with :ok <- require_keys(payload),
         {:ok, hlc} <- decode_hlc(Map.get(payload, :hlc, %{})),
         {:ok, schedules} <- decode_schedules(Map.get(payload, :schedules, %{})) do
      {:ok,
       %__MODULE__{
         format_version: @format_version,
         volume_id: Map.fetch!(payload, :volume_id),
         volume_name: Map.fetch!(payload, :volume_name),
         cluster_id: Map.fetch!(payload, :cluster_id),
         cluster_name: Map.fetch!(payload, :cluster_name),
         durability: Map.fetch!(payload, :durability),
         created_by_neonfs_version: Map.fetch!(payload, :created_by_neonfs_version),
         last_written_by_neonfs_version: Map.fetch!(payload, :last_written_by_neonfs_version),
         on_disk_format_version: Map.fetch!(payload, :on_disk_format_version),
         hlc: hlc,
         index_roots: Map.get(payload, :index_roots, default_index_roots()),
         schedules: schedules
       }}
    end
  end

  defp require_keys(payload) do
    required = [
      :volume_id,
      :volume_name,
      :cluster_id,
      :cluster_name,
      :durability,
      :created_by_neonfs_version,
      :last_written_by_neonfs_version,
      :on_disk_format_version
    ]

    missing = Enum.reject(required, &Map.has_key?(payload, &1))

    if missing == [] do
      :ok
    else
      {:error, {:malformed, "missing required keys: #{inspect(missing)}"}}
    end
  end

  defp decode_hlc(%HLC{} = hlc), do: {:ok, hlc}

  defp decode_hlc(%{
         node_id: node_id,
         last_wall: last_wall,
         last_counter: last_counter,
         max_clock_skew_ms: max_skew
       })
       when is_integer(last_wall) and is_integer(last_counter) and is_integer(max_skew) do
    {:ok,
     %HLC{
       node_id: node_id,
       last_wall: last_wall,
       last_counter: last_counter,
       max_clock_skew_ms: max_skew
     }}
  end

  defp decode_hlc(_), do: {:error, {:malformed, "hlc is not a recognised shape"}}

  defp hlc_to_map(%HLC{} = hlc) do
    %{
      node_id: hlc.node_id,
      last_wall: hlc.last_wall,
      last_counter: hlc.last_counter,
      max_clock_skew_ms: hlc.max_clock_skew_ms
    }
  end

  defp decode_schedules(map) when is_map(map) do
    schedules = %{
      gc: decode_one_schedule(Map.get(map, :gc, %{})),
      scrub: decode_one_schedule(Map.get(map, :scrub, %{})),
      anti_entropy: decode_one_schedule(Map.get(map, :anti_entropy, %{}))
    }

    {:ok, schedules}
  end

  defp decode_schedules(_), do: {:error, {:malformed, "schedules is not a map"}}

  defp decode_one_schedule(%{interval_ms: interval, last_run: last_run})
       when is_integer(interval) and interval > 0 do
    %{interval_ms: interval, last_run: last_run}
  end

  defp decode_one_schedule(_), do: %{interval_ms: 86_400_000, last_run: nil}

  defp schedules_to_map(schedules) do
    %{
      gc: schedules.gc,
      scrub: schedules.scrub,
      anti_entropy: schedules.anti_entropy
    }
  end

  defp default_index_roots, do: %{file_index: nil, chunk_index: nil, stripe_index: nil}

  defp current_neonfs_version do
    case Application.spec(:neonfs_core, :vsn) do
      nil -> "unknown"
      vsn -> List.to_string(vsn)
    end
  end
end
