defmodule NeonFS.Events do
  @moduledoc """
  Event notification types and subscription API for NeonFS cluster-wide
  metadata change notification.

  Events are hints, not data — they signal that something changed so subscribers
  can invalidate caches and re-fetch as needed. Volume-scoped events include a
  `volume_id` field for routing; cluster-scoped events (drives) are routed to
  dedicated groups.

  ## Subscribing

  Processes subscribe at the volume or cluster-resource level. File-level and
  directory-level subscriptions are intentionally omitted to keep group count
  bounded.

      NeonFS.Events.subscribe(volume_id)
      NeonFS.Events.subscribe_volumes()
      NeonFS.Events.subscribe_drives()

  Subscribed processes receive `{:neonfs_event, %NeonFS.Events.Envelope{}}` messages.
  When the subscribing process exits, `Registry` automatically unregisters it.
  """

  alias NeonFS.Events.Relay

  @type event ::
          NeonFS.Events.FileCreated.t()
          | NeonFS.Events.FileContentUpdated.t()
          | NeonFS.Events.FileTruncated.t()
          | NeonFS.Events.FileDeleted.t()
          | NeonFS.Events.FileAttrsChanged.t()
          | NeonFS.Events.FileRenamed.t()
          | NeonFS.Events.VolumeAclChanged.t()
          | NeonFS.Events.FileAclChanged.t()
          | NeonFS.Events.DirCreated.t()
          | NeonFS.Events.DirDeleted.t()
          | NeonFS.Events.DirRenamed.t()
          | NeonFS.Events.VolumeCreated.t()
          | NeonFS.Events.VolumeUpdated.t()
          | NeonFS.Events.VolumeDeleted.t()
          | NeonFS.Events.DriveAdded.t()
          | NeonFS.Events.DriveRemoved.t()

  @doc """
  Subscribe the calling process to events for a specific volume.

  The process will receive `{:neonfs_event, %Envelope{}}` messages for events
  affecting the given volume. Subscription is automatically cleaned up when the
  process exits.
  """
  @spec subscribe(binary()) :: :ok
  def subscribe(volume_id) do
    Registry.register(NeonFS.Events.Registry, {:volume, volume_id}, [])
    Relay.ensure_volume_group(volume_id)
    :ok
  end

  @doc """
  Subscribe the calling process to volume lifecycle events
  (VolumeCreated, VolumeUpdated, VolumeDeleted).

  No `:pg` join is needed — the Relay always joins the `{:volumes}` group.
  """
  @spec subscribe_volumes() :: :ok
  def subscribe_volumes do
    Registry.register(NeonFS.Events.Registry, {:volumes}, [])
    :ok
  end

  @doc """
  Unsubscribe the calling process from a specific volume's events.
  """
  @spec unsubscribe(binary()) :: :ok
  def unsubscribe(volume_id) do
    Registry.unregister(NeonFS.Events.Registry, {:volume, volume_id})
    Relay.maybe_leave_volume_group(volume_id)
    :ok
  end

  @doc """
  Unsubscribe the calling process from volume lifecycle events.
  """
  @spec unsubscribe_volumes() :: :ok
  def unsubscribe_volumes do
    Registry.unregister(NeonFS.Events.Registry, {:volumes})
    :ok
  end

  @doc """
  Subscribe the calling process to drive lifecycle events
  (DriveAdded, DriveRemoved).

  No `:pg` join is needed — the Relay always joins the `{:drives}` group.
  """
  @spec subscribe_drives() :: :ok
  def subscribe_drives do
    Registry.register(NeonFS.Events.Registry, {:drives}, [])
    :ok
  end

  @doc """
  Unsubscribe the calling process from drive lifecycle events.
  """
  @spec unsubscribe_drives() :: :ok
  def unsubscribe_drives do
    Registry.unregister(NeonFS.Events.Registry, {:drives})
    :ok
  end
end
