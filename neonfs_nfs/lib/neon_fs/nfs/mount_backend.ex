defmodule NeonFS.NFS.MountBackend do
  @moduledoc """
  `NFSServer.Mount.Backend` implementation backed by `ExportManager`.

  Resolves MOUNT3 requests against currently-published NeonFS volume
  exports. The synthetic root (`"/"`) and per-volume roots
  (`"/<volume_name>"`) are the only accepted mount paths; deeper
  sub-paths are not currently honoured because the file-handle
  layout (`NeonFS.NFS.Filehandle`) embeds the volume id but not a
  path prefix — the on-wire compromise is that clients always mount
  the volume root and use `LOOKUP` to walk into it.

  Filehandles are constructed via `NeonFS.NFS.Filehandle.encode/3` so
  the BEAM stack hands clients the same shape `NFSv3Backend` already
  decodes. The synthetic root uses an all-zero volume id and
  `fileid: 1` — same convention `NFSv3Backend.resolve_lookup/3`
  recognises. Sub-issue #656 of #286.
  """

  @behaviour NFSServer.Mount.Backend

  alias NeonFS.Client.Router
  alias NeonFS.NFS.{ExportManager, Filehandle}
  alias NFSServer.Mount.Types.ExportNode

  @synthetic_root_volume_id <<0::128>>
  @synthetic_root_fileid 1

  # All NeonFS exports accept AUTH_SYS (1) and AUTH_NONE (0). No
  # finer-grained auth gating yet — the export list is membership-
  # based at the volume layer, and AUTH_SYS uid/gid maps onto the
  # `Authorise.check/3` call inside `Core`.
  @auth_flavors [0, 1]

  @impl true
  def resolve("/", _ctx) do
    {:ok, Filehandle.encode(@synthetic_root_volume_id, @synthetic_root_fileid), @auth_flavors}
  end

  def resolve("/" <> volume_name, _ctx) do
    case lookup_export(volume_name) do
      {:ok, volume_id_binary} ->
        {:ok, Filehandle.encode(volume_id_binary, @synthetic_root_fileid), @auth_flavors}

      {:error, :not_found} ->
        {:error, :noent}

      {:error, _} ->
        {:error, :serverfault}
    end
  end

  def resolve(_, _ctx), do: {:error, :inval}

  @impl true
  def list_exports(_ctx) do
    Enum.map(ExportManager.list_exports(), fn export ->
      %ExportNode{dir: "/" <> export.volume_name, groups: []}
    end)
  catch
    :exit, _ -> []
  end

  @impl true
  def list_mounts(_ctx), do: []

  @impl true
  def record_mount(_client, _path, _auth), do: :ok

  @impl true
  def forget_mount(_client, _path, _auth), do: :ok

  @impl true
  def forget_all_mounts(_client, _auth), do: :ok

  # Resolve a volume name to its 16-byte UUID binary form (the shape
  # `Filehandle.encode/3` expects). `ExportManager` keeps an in-memory
  # mapping; we also fall back to an RPC against `Core.get_volume`
  # so the synthetic root's `LOOKUP` works for volumes the local
  # `ExportManager` hasn't observed yet.
  defp lookup_export(volume_name) do
    if Enum.any?(ExportManager.list_exports(), &(&1.volume_name == volume_name)) do
      with {:ok, %{id: id}} <- Router.call(NeonFS.Core, :get_volume, [volume_name]),
           {:ok, vol_id_bin} <- Filehandle.volume_uuid_to_binary(id) do
        {:ok, vol_id_bin}
      else
        _ -> {:error, :not_found}
      end
    else
      {:error, :not_found}
    end
  catch
    :exit, _ -> {:error, :coordinator_unavailable}
  end
end
