defmodule NeonFS.Docker.SupervisorTest do
  @moduledoc """
  Boots `NeonFS.Docker.Supervisor` against a real Unix domain socket
  in `tmp_dir`, then drives an HTTP request through Bandit + the
  plug to confirm the listener path actually binds.

  This is the regression test for the `ThousandIsland.Transports.Unix`
  bug where the listener referenced a module that doesn't exist —
  every previous test layer (PlugTest, VolumeStoreTest, …) passed
  while the supervisor crashed at boot.
  """
  use ExUnit.Case, async: false

  setup do
    # AF_UNIX `sun_path` is capped at ~108 bytes, so we can't use
    # ExUnit's `:tmp_dir` (which nests under the test module name).
    # Stick the socket directly under `System.tmp_dir!/0` and clean
    # up explicitly.
    socket_path =
      Path.join(
        System.tmp_dir!(),
        "neonfs_docker_supervisor_test_#{System.unique_integer([:positive])}.sock"
      )

    Application.put_env(:neonfs_docker, :listener, :socket)
    Application.put_env(:neonfs_docker, :socket_path, socket_path)
    Application.put_env(:neonfs_docker, :register_service, false)

    on_exit(fn ->
      Application.delete_env(:neonfs_docker, :listener)
      Application.delete_env(:neonfs_docker, :socket_path)
      Application.delete_env(:neonfs_docker, :register_service)
      _ = File.rm(socket_path)
    end)

    {:ok, socket_path: socket_path}
  end

  test "binds a UDS and responds to /Plugin.Activate", %{socket_path: socket_path} do
    start_supervised!({NeonFS.Docker.Supervisor, []})

    assert File.exists?(socket_path), "expected supervisor to bind UDS at #{socket_path}"

    {status, body} = post_json(socket_path, "/Plugin.Activate", "{}")

    assert status == 200
    assert Jason.decode!(body) == %{"Implements" => ["VolumeDriver"]}
  end

  test "parses request bodies sent with Docker's plugin content type",
       %{socket_path: socket_path} do
    start_supervised!({NeonFS.Docker.Supervisor, []})

    {status, body} =
      post_json(
        socket_path,
        "/VolumeDriver.Get",
        ~s|{"Name":"some-vol"}|,
        content_type: "application/vnd.docker.plugins.v1.2+json"
      )

    assert status == 200
    decoded = Jason.decode!(body)
    refute decoded["Err"] == "missing or invalid Name", "body parser dropped the Name field"
    assert decoded["Err"] == "volume not found"
  end

  test "parses request bodies even when dockerd omits Content-Type",
       %{socket_path: socket_path} do
    # The moby plugin client (`pkg/plugins`) sets `Accept` but not
    # `Content-Type` on its plugin requests. Without a hint Plug.Parsers
    # can't pick a parser and `body_params` stays empty, so every
    # endpoint that needs `Name` fails with `"missing or invalid Name"`.
    start_supervised!({NeonFS.Docker.Supervisor, []})

    {status, body} =
      post_json(
        socket_path,
        "/VolumeDriver.Get",
        ~s|{"Name":"some-vol"}|,
        content_type: nil
      )

    assert status == 200
    decoded = Jason.decode!(body)
    refute decoded["Err"] == "missing or invalid Name", "body parser dropped the Name field"
    assert decoded["Err"] == "volume not found"
  end

  defp post_json(socket_path, path, body, opts \\ []) do
    content_type = Keyword.get(opts, :content_type, "application/json")

    {:ok, client} =
      :gen_tcp.connect({:local, socket_path}, 0, [
        :binary,
        active: false,
        packet: :raw
      ])

    content_type_header =
      if content_type, do: "Content-Type: #{content_type}\r\n", else: ""

    request =
      "POST #{path} HTTP/1.1\r\n" <>
        "Host: docker\r\n" <>
        content_type_header <>
        "Content-Length: #{byte_size(body)}\r\n" <>
        "Connection: close\r\n" <>
        "\r\n" <>
        body

    :ok = :gen_tcp.send(client, request)
    response = recv_until_close(client, "")
    :gen_tcp.close(client)

    parse_response(response)
  end

  defp recv_until_close(socket, acc) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, chunk} -> recv_until_close(socket, acc <> chunk)
      {:error, :closed} -> acc
    end
  end

  defp parse_response(response) do
    [headers, body] = String.split(response, "\r\n\r\n", parts: 2)
    [status_line | _] = String.split(headers, "\r\n")
    [_proto, status, _reason] = String.split(status_line, " ", parts: 3)
    {String.to_integer(status), body}
  end
end
