defmodule NeonFS.NFS do
  @moduledoc """
  NFSv3 server interface for NeonFS.

  Provides network filesystem access to NeonFS volumes using the NFSv3
  protocol. Volumes are exported as top-level directories under a virtual
  root, allowing clients to mount individual volumes or browse all exports.

  This package depends on `neonfs_client` only — it has **no dependency on
  `neonfs_core`**. All communication with core nodes happens via Erlang
  distribution, routed through `NeonFS.Client.Router`.
  """
end
