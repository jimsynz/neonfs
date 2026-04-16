defmodule NeonFS.Client.ServiceType do
  @moduledoc """
  Service type identifiers for NeonFS cluster members.

  Each node in a NeonFS cluster runs one or more services. The service type
  determines what capabilities a node provides and how it participates in
  cluster operations.
  """

  @type t :: :core | :fuse | :nfs | :s3 | :docker | :csi | :cifs | :webdav

  @service_types [:cifs, :core, :csi, :docker, :fuse, :nfs, :s3, :webdav]

  @doc """
  Guard that checks if a value is a valid service type.
  """
  defguard is_service_type(type) when type in @service_types

  @doc """
  Returns true if the service type is `:core`.
  """
  @spec core?(t()) :: boolean()
  def core?(:core), do: true
  def core?(_), do: false

  @doc """
  Returns all valid service types.
  """
  @spec all() :: [t()]
  def all, do: @service_types
end
