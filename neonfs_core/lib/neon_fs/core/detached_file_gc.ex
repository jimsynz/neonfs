defmodule NeonFS.Core.DetachedFileGC do
  @moduledoc """
  Telemetry handler that GC's `:detached` FileMetas when their pins
  release.

  POSIX unlink-while-open (sub-issue #644 of #638) keeps the chunks of
  an unlinked file reachable for as long as any open handle holds a
  `:pinned` namespace claim against the path. Each detached
  `FileMeta` snapshots the pin claim ids that block its GC at detach
  time. When any of those pins releases the BEAM's namespace
  coordinator emits a release-telemetry event; this module subscribes
  to those events and decrements the matching tombstones via
  `FileIndex.decrement_pin/2`.

  Two events are observed:

    * `[:neonfs, :ra, :command, :release_namespace_claim]` — a
      single claim was released. Metadata carries `:claim_id`. We
      scan the local `:file_index_by_id` ETS table for detached
      files referencing that id and decrement each.
    * `[:neonfs, :ra, :command, :release_namespace_claims_for_holder]` —
      a holder DOWN released a batch of claims. Metadata carries
      `:released_claim_ids`. We iterate them and do the same.

  Detached-file rows have `pinned_claim_ids` populated, so the local
  ETS scan filters cheaply on `length(pinned_claim_ids) > 0`. The set
  of detached files is small in practice — only files actively in the
  unlink-while-open state — so the linear scan is fine.

  The handler runs on every core node. `decrement_pin/2` itself is
  idempotent: a duplicate notification (the telemetry event fires on
  every Ra follower as commands replay) is a no-op once the local
  state has converged.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{FileIndex, FileMeta}

  @telemetry_handler_id "neonfs-detached-file-gc"

  @release_event [:neonfs, :ra, :command, :release_namespace_claim]
  @bulk_release_event [:neonfs, :ra, :command, :release_namespace_claims_for_holder]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  @spec handle_event([atom()], map(), map(), term()) :: :ok
  def handle_event(@release_event, _measurements, %{claim_id: claim_id}, _config)
      when is_binary(claim_id) do
    decrement_for_claim(claim_id)
    :ok
  end

  def handle_event(@bulk_release_event, _measurements, %{released_claim_ids: ids}, _config)
      when is_list(ids) do
    Enum.each(ids, &decrement_for_claim/1)
    :ok
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  # Server callbacks

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)
    attach_telemetry()
    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    :telemetry.detach(@telemetry_handler_id)
    :ok
  end

  # Private helpers

  defp attach_telemetry do
    :telemetry.attach_many(
      @telemetry_handler_id,
      [@release_event, @bulk_release_event],
      &__MODULE__.handle_event/4,
      nil
    )
  end

  # Find every detached FileMeta in the local ETS cache whose
  # `pinned_claim_ids` references `claim_id`, and decrement each.
  # The cache is a write-through materialisation so the per-node view
  # is sufficient — every node's own GC handler runs the same scan
  # against its own cache.
  defp decrement_for_claim(claim_id) do
    case file_ids_pinned_by(claim_id) do
      [] ->
        :ok

      file_ids ->
        Enum.each(file_ids, &safely_decrement_pin(&1, claim_id))
    end
  end

  defp file_ids_pinned_by(claim_id) do
    :ets.foldl(
      fn
        {file_id, %FileMeta{detached: true, pinned_claim_ids: ids}}, acc ->
          if claim_id in ids, do: [file_id | acc], else: acc

        _, acc ->
          acc
      end,
      [],
      :file_index_by_id
    )
  rescue
    ArgumentError -> []
  end

  defp safely_decrement_pin(file_id, claim_id) do
    case FileIndex.decrement_pin(file_id, claim_id) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("DetachedFileGC.decrement_pin failed",
          file_id: file_id,
          claim_id: claim_id,
          reason: inspect(reason)
        )
    end
  catch
    :exit, _ -> :ok
  end
end
