defmodule NeonFS.Omnibus do
  @moduledoc """
  All-in-one NeonFS deployment combining core, FUSE, NFS, S3, and WebDAV
  services in a single BEAM node.

  In omnibus mode, `neonfs_core` starts first via the normal OTP boot
  sequence (it is a runtime dependency). The FUSE, NFS, S3, and WebDAV
  services are then started explicitly by `NeonFS.Omnibus.Application`
  once core is ready, so that service discovery and RPC routing resolve
  locally.
  """
end
