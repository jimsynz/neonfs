defmodule NeonFS.CIFS.LiveListenerTest do
  @moduledoc """
  Drives the C `vfs_neonfs` wire client (`native/vfs_neonfs/wire_probe`)
  against a live `NeonFS.CIFS.Listener` over a Unix socket (#1400).

  The C-only `test_wire` harness encodes replies with `erl_interface`
  (`ei`) on both sides; this test instead exercises the real Elixir
  `:erlang.term_to_binary` / `:erlang.binary_to_term(_, [:safe])` path so
  the `ei` request/reply encoding is verified against the encoder Samba
  will actually talk to — no Samba, no cluster.

  The listener runs with a canned-reply handler whose replies match the
  shared op sequence in `native/vfs_neonfs/probe_ops.c`.
  """

  use ExUnit.Case, async: true

  alias NeonFS.CIFS.Listener

  @native_dir Path.expand("../../../native/vfs_neonfs", __DIR__)
  @probe Path.join(@native_dir, "wire_probe")

  defmodule CannedHandler do
    @moduledoc false
    use ThousandIsland.Handler

    alias NeonFS.CIFS.Listener

    @impl ThousandIsland.Handler
    def handle_connection(_socket, _state), do: {:continue, %{readdir_calls: 0}}

    @impl ThousandIsland.Handler
    def handle_data(body, socket, state) do
      {:ok, request} = Listener.decode(body)
      {reply, state} = reply_for(request, state)
      ThousandIsland.Socket.send(socket, :erlang.term_to_binary(reply))
      {:continue, state}
    end

    defp reply_for({:connect, %{"volume" => "vol1"}}, state), do: {{:ok, %{}}, state}
    defp reply_for({:disconnect, _}, state), do: {{:ok, %{}}, state}

    defp reply_for({:stat, %{"path" => "__enoent__"}}, state), do: {{:error, :enoent}, state}
    defp reply_for({:stat, %{"path" => _}}, state), do: {{:ok, %{stat: stat()}}, state}
    defp reply_for({:lstat, %{"path" => _}}, state), do: {{:ok, %{stat: stat()}}, state}
    defp reply_for({:fstat, %{"handle" => _}}, state), do: {{:ok, %{stat: stat()}}, state}

    defp reply_for({:fchmod, %{"handle" => _, "mode" => _}}, state), do: {{:ok, %{}}, state}
    defp reply_for({:fchown, _}, state), do: {{:error, :enosys}, state}
    defp reply_for({:fntimes, _}, state), do: {{:ok, %{}}, state}

    defp reply_for({:openat, %{"path" => "__eexist__"}}, state), do: {{:error, :eexist}, state}
    defp reply_for({:openat, %{"path" => _}}, state), do: {{:ok, %{handle: 42}}, state}
    defp reply_for({:close, %{"handle" => _}}, state), do: {{:ok, %{}}, state}
    defp reply_for({:pread, %{"handle" => _}}, state), do: {{:ok, %{data: "hello"}}, state}

    defp reply_for({:pwrite, %{"data" => data}}, state),
      do: {{:ok, %{written: byte_size(data)}}, state}

    defp reply_for({:ftruncate, _}, state), do: {{:ok, %{}}, state}
    defp reply_for({:fsync, %{"handle" => _}}, state), do: {{:ok, %{}}, state}

    defp reply_for({:fdopendir, %{"path" => _}}, state), do: {{:ok, %{handle: 42}}, state}

    defp reply_for({:readdir, _}, %{readdir_calls: 0} = state) do
      {{:ok, %{entry: %{name: "file.txt", kind: :file}, eof: false}}, %{state | readdir_calls: 1}}
    end

    defp reply_for({:readdir, _}, state), do: {{:ok, %{eof: true}}, state}
    defp reply_for({:closedir, _}, state), do: {{:ok, %{}}, state}
    defp reply_for({:mkdirat, %{"path" => _}}, state), do: {{:ok, %{}}, state}
    defp reply_for({:unlinkat, %{"path" => _}}, state), do: {{:ok, %{}}, state}

    defp reply_for({:renameat, %{"old_path" => _, "new_path" => _}}, state),
      do: {{:ok, %{}}, state}

    defp reply_for({:disk_free, _}, state), do: {{:ok, statvfs()}, state}
    defp reply_for({:fstatvfs, _}, state), do: {{:ok, statvfs()}, state}

    defp stat do
      %{size: 1234, mode: 0o100644, atime: 111, mtime: 222, ctime: 333, kind: :file}
    end

    defp statvfs, do: %{total_bytes: 1000, free_bytes: 400, available_bytes: 300}
  end

  setup_all do
    {out, status} = System.cmd("make", ["wire_probe"], cd: @native_dir, stderr_to_stdout: true)
    assert status == 0, "building wire_probe failed:\n#{out}"
    :ok
  end

  test "C wire client round-trips every op against a live listener" do
    socket_path =
      Path.join(
        System.tmp_dir!(),
        "neonfs_cifs_probe_#{System.unique_integer([:positive])}.sock"
      )

    on_exit(fn -> _ = File.rm(socket_path) end)

    start_supervised!(Listener.child_spec(socket_path: socket_path, handler: CannedHandler))
    assert File.exists?(socket_path), "expected listener to bind UDS at #{socket_path}"

    {out, status} = System.cmd(@probe, [socket_path], stderr_to_stdout: true)
    assert status == 0, "wire_probe exited #{status}:\n#{out}"
    assert out =~ "ok:"
  end
end
