defmodule NeonFS.Docker.Plug do
  @moduledoc """
  HTTP router for the Docker Volume Plugin v1 protocol.

  Every endpoint accepts `POST` with a JSON body and returns a JSON
  body. Response shape always includes an `"Err"` field (empty string
  on success), per the Docker plugin contract.

  Injectable dependencies (for testing):

    * `:volume_store` — module or pid implementing the
      `NeonFS.Docker.VolumeStore` API (default: `NeonFS.Docker.VolumeStore`).
    * `:mount_tracker` — pid or registered name of the
      `NeonFS.Docker.MountTracker` GenServer handling ref-counted FUSE
      mounts (default: `NeonFS.Docker.MountTracker`).
    * `:core_create_fn` — 2-arity function called as
      `core_create_fn.(name, opts)` in the `Create` handler to propagate
      the volume to NeonFS core. Default calls `NeonFS.Client.Router`.
  """

  use Plug.Router

  alias NeonFS.Docker.{MountTracker, VolumeStore}

  plug(Plug.Parsers,
    parsers: [:json],
    pass: ["application/json", "application/vnd.docker.plugins.v1.2+json"],
    json_decoder: Jason
  )

  plug(:match)
  plug(:dispatch)

  ## Plugin activation

  post "/Plugin.Activate" do
    reply(conn, 200, %{"Implements" => ["VolumeDriver"]})
  end

  ## Capabilities

  post "/VolumeDriver.Capabilities" do
    reply(conn, 200, %{"Capabilities" => %{"Scope" => "local"}})
  end

  ## Create

  post "/VolumeDriver.Create" do
    with {:ok, name} <- fetch_name(conn.body_params),
         opts <- Map.get(conn.body_params, "Opts", %{}) || %{},
         :ok <- propagate_to_core(conn, name, opts) do
      VolumeStore.put(volume_store(conn), name, opts)
      reply(conn, 200, %{"Err" => ""})
    else
      {:error, message} -> reply(conn, 200, %{"Err" => message})
    end
  end

  ## Remove

  post "/VolumeDriver.Remove" do
    case fetch_name(conn.body_params) do
      {:ok, name} ->
        VolumeStore.delete(volume_store(conn), name)
        reply(conn, 200, %{"Err" => ""})

      {:error, message} ->
        reply(conn, 200, %{"Err" => message})
    end
  end

  ## Get

  post "/VolumeDriver.Get" do
    with {:ok, name} <- fetch_name(conn.body_params),
         {:ok, record} <- VolumeStore.get(volume_store(conn), name) do
      mountpoint = MountTracker.mountpoint_for(mount_tracker(conn), record.name)
      reply(conn, 200, %{"Volume" => volume_view(record, mountpoint), "Err" => ""})
    else
      {:error, :not_found} -> reply(conn, 200, %{"Err" => "volume not found"})
      {:error, message} -> reply(conn, 200, %{"Err" => message})
    end
  end

  ## List

  post "/VolumeDriver.List" do
    tracker = mount_tracker(conn)

    volumes =
      volume_store(conn)
      |> VolumeStore.list()
      |> Enum.map(fn record ->
        volume_view(record, MountTracker.mountpoint_for(tracker, record.name))
      end)

    reply(conn, 200, %{"Volumes" => volumes, "Err" => ""})
  end

  ## Path

  post "/VolumeDriver.Path" do
    case fetch_name(conn.body_params) do
      {:ok, name} ->
        mountpoint = MountTracker.mountpoint_for(mount_tracker(conn), name)
        reply(conn, 200, %{"Mountpoint" => mountpoint, "Err" => ""})

      {:error, message} ->
        reply(conn, 200, %{"Err" => message})
    end
  end

  ## Mount

  post "/VolumeDriver.Mount" do
    with {:ok, name} <- fetch_name(conn.body_params),
         {:ok, mountpoint} <- MountTracker.mount(mount_tracker(conn), name) do
      reply(conn, 200, %{"Mountpoint" => mountpoint, "Err" => ""})
    else
      {:error, message} when is_binary(message) ->
        reply(conn, 200, %{"Err" => message})

      {:error, reason} ->
        reply(conn, 200, %{"Err" => "mount failed: #{inspect(reason)}"})
    end
  end

  ## Unmount

  post "/VolumeDriver.Unmount" do
    with {:ok, name} <- fetch_name(conn.body_params),
         :ok <- MountTracker.unmount(mount_tracker(conn), name) do
      reply(conn, 200, %{"Err" => ""})
    else
      {:error, message} when is_binary(message) ->
        reply(conn, 200, %{"Err" => message})

      {:error, reason} ->
        reply(conn, 200, %{"Err" => "unmount failed: #{inspect(reason)}"})
    end
  end

  match _ do
    reply(conn, 404, %{"Err" => "not found"})
  end

  ## Helpers

  defp fetch_name(body) do
    case Map.get(body, "Name") do
      name when is_binary(name) and byte_size(name) > 0 -> {:ok, name}
      _ -> {:error, "missing or invalid Name"}
    end
  end

  defp volume_view(%{name: name}, mountpoint) do
    %{"Name" => name, "Mountpoint" => mountpoint}
  end

  defp volume_store(conn) do
    conn.private[:volume_store] || NeonFS.Docker.VolumeStore
  end

  defp mount_tracker(conn) do
    conn.private[:mount_tracker] || NeonFS.Docker.MountTracker
  end

  defp propagate_to_core(conn, name, opts) do
    case Map.get(conn.private, :core_create_fn) do
      nil -> default_core_create(name, opts)
      fun when is_function(fun, 2) -> fun.(name, opts)
    end
  end

  defp default_core_create(name, opts) do
    case NeonFS.Client.Router.call(NeonFS.Core, :create_volume, [name, opts]) do
      {:ok, _volume} -> :ok
      {:error, %{class: :already_exists}} -> :ok
      {:error, :already_exists} -> :ok
      {:error, reason} -> {:error, "core create_volume failed: #{inspect(reason)}"}
    end
  end

  defp reply(conn, status, body) do
    conn
    |> Plug.Conn.put_resp_content_type("application/vnd.docker.plugins.v1.2+json")
    |> Plug.Conn.send_resp(status, Jason.encode!(body))
  end
end
