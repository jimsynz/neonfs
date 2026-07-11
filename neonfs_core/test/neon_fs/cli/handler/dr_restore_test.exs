defmodule NeonFS.CLI.Handler.DRRestoreTest do
  @moduledoc """
  #1005: full-cluster `dr restore` — stage + apply a DR snapshot, then
  restore each volume's content from its backup archive.

  Runs in-process on a `start_provisioned_cluster/2` cluster (the #1008
  provisioned-cluster helpers, the same ones the DR-restore integration
  path was scoped around) rather than a peer cluster: `dr restore` is a
  single-node bootstrap operation (the operator reattaches the remaining
  nodes with `cluster join` afterwards), and an in-process cluster gives a
  real provisioning + backup + snapshot round-trip without peer-boot flake.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core
  alias NeonFS.CLI.Handler.DR

  @moduletag :tmp_dir

  @durability %{type: :replicate, factor: 1, min_copies: 1}

  setup %{tmp_dir: tmp_dir} do
    {:ok, _cluster_id} = start_provisioned_cluster(tmp_dir)

    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    source = Path.join(tmp_dir, "dr")
    File.mkdir_p!(Path.join(source, "volumes"))

    {:ok, source: source}
  end

  test "restores volume content from a snapshot + per-volume backups", %{source: source} do
    {:ok, vol} = create_provisioned_volume("payroll", durability: @durability)
    {:ok, _} = write_file(vol.id, "/wages.csv", "alice,100\nbob,200\n")

    {:ok, _} = DR.handle_dr_snapshot_create()
    [snapshot | _] = list_snapshots()
    {:ok, _} = DR.handle_dr_snapshot_export(snapshot.id, source)

    {:ok, _} =
      NeonFS.CLI.Handler.handle_backup_create(
        "payroll",
        Path.join([source, "volumes", "payroll.backup"])
      )

    :ok = Core.delete_file("payroll", "/wages.csv")
    assert {:error, _} = read_file(vol.name, "/wages.csv")

    assert {:ok, result} = DR.handle_dr_restore(source)

    assert result.snapshot_id == snapshot.id
    assert result.generation >= 1

    payroll = Enum.find(result.volumes, &(&1.name == "payroll"))
    assert payroll.status == "restored"
    assert payroll.files >= 1

    assert {:ok, "alice,100\nbob,200\n"} = read_file(vol.name, "/wages.csv")
  end

  test "leaves a volume without a backup archive as an empty shell", %{source: source} do
    {:ok, _archived} = create_provisioned_volume("archived", durability: @durability)
    {:ok, _bare} = create_provisioned_volume("bare", durability: @durability)

    {:ok, _} = DR.handle_dr_snapshot_create()
    [snapshot | _] = list_snapshots()
    {:ok, _} = DR.handle_dr_snapshot_export(snapshot.id, source)

    {:ok, _} =
      NeonFS.CLI.Handler.handle_backup_create(
        "archived",
        Path.join([source, "volumes", "archived.backup"])
      )

    assert {:ok, result} = DR.handle_dr_restore(source)

    bare = Enum.find(result.volumes, &(&1.name == "bare"))
    assert bare.status == "skipped"
    assert result.volumes_failed == 0
  end

  test "a catalogue pins archive locations and omitted volumes are skipped", %{source: source} do
    {:ok, vol} = create_provisioned_volume("catalogued", durability: @durability)
    {:ok, _} = create_provisioned_volume("omitted", durability: @durability)
    {:ok, _} = write_file(vol.id, "/x", "payload")

    {:ok, _} = DR.handle_dr_snapshot_create()
    [snapshot | _] = list_snapshots()
    {:ok, _} = DR.handle_dr_snapshot_export(snapshot.id, source)

    archive = Path.join(source, "catalogued.tar")
    {:ok, _} = NeonFS.CLI.Handler.handle_backup_create("catalogued", archive)

    catalogue = Path.join(source, "catalogue.json")
    File.write!(catalogue, Jason.encode!(%{"catalogued" => "catalogued.tar"}))

    :ok = Core.delete_file("catalogued", "/x")

    assert {:ok, result} = DR.handle_dr_restore(source, %{"catalogue" => catalogue})

    assert Enum.find(result.volumes, &(&1.name == "catalogued")).status == "restored"
    assert Enum.find(result.volumes, &(&1.name == "omitted")).status == "skipped"
    assert {:ok, "payload"} = read_file(vol.name, "/x")
  end

  test "rejects an invalid catalogue", %{source: source} do
    {:ok, _} = create_provisioned_volume("v", durability: @durability)
    {:ok, _} = DR.handle_dr_snapshot_create()
    [snapshot | _] = list_snapshots()
    {:ok, _} = DR.handle_dr_snapshot_export(snapshot.id, source)

    catalogue = Path.join(source, "bad.json")
    File.write!(catalogue, "not json{")

    assert {:error, _} = DR.handle_dr_restore(source, %{"catalogue" => catalogue})
  end

  defp list_snapshots do
    {:ok, snapshots} = DR.handle_dr_snapshot_list()
    snapshots
  end

  defp write_file(volume_id, path, content) do
    NeonFS.Core.WriteOperation.write_file_streamed(volume_id, path, [content])
  end

  defp read_file(volume_name, path) do
    with {:ok, %{stream: stream}} <- Core.read_file_stream(volume_name, path) do
      {:ok, stream |> Enum.to_list() |> IO.iodata_to_binary()}
    end
  end
end
