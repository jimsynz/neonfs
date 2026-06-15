defmodule NeonFS.CLI.Handler.S3 do
  @moduledoc """
  CLI command handlers for the S3-compatible interface: the
  volumes-as-buckets views.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `handle_s3_*` RPC entry points here, so the CLI wire
  contract is unchanged. Credential lifecycle moved to the
  interface-agnostic `NeonFS.CLI.Handler.Credential` (#1283).
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.Error.NotFound

  @doc """
  Lists all volumes available as S3 buckets.
  """
  @spec handle_s3_list_buckets() :: {:ok, [map()]}
  def handle_s3_list_buckets do
    set_cli_metadata()

    with :ok <- require_cluster() do
      buckets =
        VolumeRegistry.list()
        |> Enum.map(&volume_to_bucket/1)
        |> Enum.sort_by(& &1.name)

      {:ok, buckets}
    end
  end

  @doc """
  Shows details of a single S3 bucket (volume).
  """
  @spec handle_s3_show_bucket(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_s3_show_bucket(bucket_name) when is_binary(bucket_name) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case VolumeRegistry.get_by_name(bucket_name) do
        {:ok, volume} ->
          {:ok, volume_to_bucket(volume)}

        {:error, :not_found} ->
          {:error, NotFound.exception(message: "Bucket '#{bucket_name}' not found")}
      end
    end
  end

  # Private

  defp volume_to_bucket(volume) do
    %{
      name: volume.name,
      created_at: DateTime.to_iso8601(volume.created_at),
      durability: volume.durability,
      compression: volume.compression,
      logical_size: volume.logical_size,
      physical_size: volume.physical_size
    }
  end
end
