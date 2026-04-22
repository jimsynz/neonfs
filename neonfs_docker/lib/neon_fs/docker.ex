defmodule NeonFS.Docker do
  @moduledoc """
  Docker / Podman VolumeDriver plugin for NeonFS.

  Speaks the [Docker Volume Plugin v1 HTTP
  protocol](https://docs.docker.com/engine/extend/plugins_volume/) over
  a Unix socket. Plugin volumes are recorded in a local store and
  mapped onto NeonFS volumes through `NeonFS.Client.Router`.

  This module is an application-level namespace marker only; the public
  surface lives in `NeonFS.Docker.Plug`, `NeonFS.Docker.VolumeStore`,
  and `NeonFS.Docker.Supervisor`.
  """
end
