defmodule NeonFS.TestSupport.Smbd do
  @moduledoc """
  Runs a real `smbd` with `vfs_neonfs.so` in front of a `neonfs_cifs`
  bridge, so the SMB stack can be exercised the way a client sees it.

  Everything below the share — the ETF wire protocol, the handler's op
  dispatch, the `{volume, file_id}` handle registry — has unit coverage
  against a mock responder. What that cannot show is whether an actual
  `smbd` loads the module, drives it with the arguments Samba really
  passes, and serves the result to an SMB2 client. This module supplies
  the missing half.

  ## Version matching

  The module cannot be built out-of-tree: it has to link against the same
  Samba private libraries, with the same symbol-version node, as the
  `smbd` that loads it. `packaging/build-vfs-deb.sh` gets that by building
  inside the distro's own Samba source package, and
  `packaging/verify-smbd-sidecar.sh` installs the pair together. So
  `available?/0` asks whether that install happened rather than whether
  Samba is merely present — an `smbd` from the archive with no matching
  module is not something this can run against.

  ## Ports and state

  Each server picks its own high port and keeps every file Samba writes —
  logs, tdbs, lock and pid directories — inside the caller's directory.
  Samba's compiled-in defaults are shared, system-wide paths; a test that
  let it use them would read another run's state and would need root to
  write its own.
  """

  @required_binaries ~w(smbd smbclient testparm pdbedit)

  # The share runs as a real authenticated account rather than a guest mapped
  # to root. Samba maps an SMB user to a local one under `security = user`, so
  # the account has to already exist on the host — the user running the tests is
  # the only one that reliably does. Its password is Samba's own, kept in the
  # test's `private dir` and never touching the system passdb.
  @smb_password "neonfs-test-pw"

  @typedoc """
  A running server. `port` is where `smbclient` should dial;
  `os_pid` is the process group `stop/1` signals.
  """
  @type t :: %{
          port: pos_integer(),
          os_pid: pos_integer(),
          share: String.t(),
          dir: String.t(),
          erlang_port: port(),
          user: String.t(),
          uid: non_neg_integer(),
          gid: non_neg_integer()
        }

  @doc """
  The uid the share's sessions run as.

  A volume served to this server has to grant this identity write access, or
  every create is refused by core — which is the point: the share no longer
  forces root, so the tests exercise the permission path rather than bypassing
  it.
  """
  @spec identity() :: %{user: String.t(), uid: non_neg_integer(), gid: non_neg_integer()}
  def identity do
    %{user: run_cmd!("id", ["-un"]), uid: run_int!("id", ["-u"]), gid: run_int!("id", ["-g"])}
  end

  @doc """
  Whether this machine can run an `smbd` with the NeonFS VFS module.

  False when Samba's binaries are missing, and false when they are
  present but the module is not installed alongside them — the second
  case is the one worth distinguishing, because it looks like a working
  Samba right up until a tree connect fails.
  """
  @spec available?() :: boolean()
  def available? do
    Enum.all?(@required_binaries, &(System.find_executable(&1) != nil)) and module_installed?()
  end

  @doc """
  Starts an `smbd` serving one share backed by a `neonfs_cifs` bridge.

  Required options:

    * `:dir` — a directory the caller owns. Every file Samba writes goes
      under it, so a run reads its own state and needs no root.
    * `:socket_path` — the bridge's UDS, as
      `PeerCluster` reports it in `node_info.interface_ports.cifs`.
    * `:volume` — the NeonFS volume the share exposes.

  Optional:

    * `:share` — share name (default `"neonfs"`).
    * `:read_only` — default `false`.

  Raises rather than returning an error tuple: a test that cannot start
  its server has nothing left to assert.
  """
  @spec start!(keyword()) :: t()
  def start!(opts) do
    dir = Keyword.fetch!(opts, :dir)
    socket_path = Keyword.fetch!(opts, :socket_path)
    volume = Keyword.fetch!(opts, :volume)
    share = Keyword.get(opts, :share, "neonfs")
    read_only = Keyword.get(opts, :read_only, false)

    File.mkdir_p!(dir)

    port = free_port()

    %{user: user, uid: uid, gid: gid} = identity()

    conf_path = Path.join(dir, "smb.conf")
    File.write!(conf_path, smb_conf(dir, share, socket_path, volume, port, read_only))

    validate_conf!(conf_path)
    add_smb_user!(conf_path, user)

    server =
      conf_path
      |> spawn_smbd(dir, port, share)
      |> Map.merge(%{user: user, uid: uid, gid: gid})

    await_ready!(server)
    server
  end

  # `pdbedit`, not `smbpasswd`: the latter refuses to take a username unless it
  # is running as root ("When run by root: smbpasswd [options] [username]"), and
  # its `-L` local mode refuses too. `pdbedit` has no such restriction, so the
  # tests need no privileges.
  #
  # `--configfile`, not `-c` — on `pdbedit` that is `--account-control`, and it
  # is silently accepted, leaving the tool writing to the *system* passdb at
  # `/var/lib/samba/private/passdb.tdb` and failing on its permissions. The
  # config pins `passdb backend` to a path in the test directory, because
  # `private dir` alone does not move it.
  #
  # `-t` takes the password from stdin twice rather than from a tty.
  defp add_smb_user!(conf_path, user) do
    port =
      Port.open({:spawn_executable, System.find_executable("pdbedit")}, [
        :binary,
        :exit_status,
        :hide,
        :stderr_to_stdout,
        args: ["--configfile=" <> conf_path, "-a", "-u", user, "-t"]
      ])

    Port.command(port, "#{@smb_password}\n#{@smb_password}\n")
    collect_exit!(port, "pdbedit", [])
  end

  defp collect_exit!(port, name, acc) do
    receive do
      {^port, {:data, data}} -> collect_exit!(port, name, [data | acc])
      {^port, {:exit_status, 0}} -> :ok
      {^port, {:exit_status, code}} -> raise "#{name} failed (#{code}):\n#{output(acc)}"
    after
      30_000 -> raise "#{name} did not finish within 30s:\n#{output(acc)}"
    end
  end

  defp output(acc), do: acc |> Enum.reverse() |> Enum.join()

  defp run_cmd!(cmd, args) do
    case System.cmd(cmd, args, stderr_to_stdout: true) do
      {out, 0} -> String.trim(out)
      {out, code} -> raise "#{cmd} #{Enum.join(args, " ")} failed (#{code}): #{out}"
    end
  end

  defp run_int!(cmd, args), do: cmd |> run_cmd!(args) |> String.to_integer()

  @doc """
  Stops the server, leaving nothing of it behind.
  """
  @spec stop(t()) :: :ok
  def stop(%{os_pid: os_pid, erlang_port: erlang_port}) do
    # `smbd` leads its own process group and forks a child per connection,
    # so signalling the pid alone strands the children — and a stranded
    # child keeps the share's port bound for the next test.
    _ = System.cmd("kill", ["--", "-#{os_pid}"], stderr_to_stdout: true)

    receive do
      {^erlang_port, {:exit_status, _status}} -> :ok
    after
      5_000 ->
        _ = System.cmd("kill", ["-9", "--", "-#{os_pid}"], stderr_to_stdout: true)
        :ok
    end
  end

  @doc """
  Runs `smbclient` against the server, returning its combined output.

  `commands` is the `-c` script — `"put local remote; ls"` and so on.
  Returns `{:ok, output}` when `smbclient` exits zero and
  `{:error, {status, output}}` otherwise; several tests are about the
  failure, so a non-zero exit is a result rather than a raise.
  """
  @spec client(t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, {non_neg_integer(), String.t()}}
  def client(server, commands, opts \\ []) do
    share = Keyword.get(opts, :share, server.share)

    args =
      [
        "//localhost/#{share}",
        "-p",
        Integer.to_string(server.port),
        "-U",
        "#{server.user}%#{@smb_password}",
        "-c",
        commands
      ] ++ Keyword.get(opts, :extra_args, [])

    case System.cmd("smbclient", args, stderr_to_stdout: true, cd: server.dir) do
      {output, 0} -> {:ok, output}
      {output, status} -> {:error, {status, output}}
    end
  end

  @doc """
  The server's log, for a failure message worth reading.
  """
  @spec logs(t()) :: String.t()
  def logs(%{dir: dir}) do
    [Path.join(dir, "smbd.stdout"), Path.join(dir, "smbd.log")]
    |> Enum.map_join("\n", fn path ->
      case File.read(path) do
        {:ok, contents} -> "== #{Path.basename(path)} ==\n#{contents}"
        {:error, reason} -> "== #{Path.basename(path)} unreadable: #{inspect(reason)} =="
      end
    end)
  end

  defp module_installed? do
    case System.cmd("dpkg-query", ["-L", "samba-vfs-neonfs"], stderr_to_stdout: true) do
      {output, 0} -> output =~ ~r{/neonfs\.so$}m
      _ -> false
    end
  end

  # `path` names a directory **inside the volume**, not on local disk.
  # smbd's tree connect stats and chdirs to it through the VFS stack, so
  # `vfs_neonfs` resolves it against NeonFS — pointing it at a local
  # directory fails the connect with "does not exist or permission
  # denied" no matter what exists on the host, which reads as a
  # permissions problem and is not one. The volume root always exists.
  defp smb_conf(dir, share, socket_path, volume, port, read_only) do
    """
    [global]
        workgroup = WORKGROUP
        server min protocol = SMB2
        security = user
        smb ports = #{port}
        log file = #{dir}/smbd.log
        log level = 3 vfs:10
        private dir = #{dir}
        lock directory = #{dir}
        state directory = #{dir}
        cache directory = #{dir}
        pid directory = #{dir}
        ncalrpc dir = #{dir}
        # Pinned explicitly: `private dir` does not move the passdb, so without
        # this both `pdbedit` and `smbd` read the host's database at
        # /var/lib/samba/private and the test needs privileges it should not.
        passdb backend = tdbsam:#{dir}/passdb.tdb

    [#{share}]
        path = /
        read only = #{if read_only, do: "yes", else: "no"}
        # No `force user` and no guest access: the session runs as the
        # authenticated account, so every request reaches core carrying that
        # identity and is checked against the volume's own permissions. A share
        # that forced root would pass these tests without the permission path
        # ever running.
        vfs objects = neonfs
        neonfs:socket = #{socket_path}
        neonfs:volume = #{volume}
    """
  end

  defp validate_conf!(conf_path) do
    case System.cmd("testparm", ["-s", conf_path], stderr_to_stdout: true) do
      {_output, 0} -> :ok
      {output, status} -> raise "smb.conf rejected by testparm (#{status}):\n#{output}"
    end
  end

  # `--no-process-group` is not optional here, though it is when a shell
  # backgrounds smbd: a port-spawned process is already a process-group
  # leader, so smbd's own `setsid()` fails with EPERM and it exits with
  # "Failed to create session, error code 1" — after logging that it
  # loaded the config, which reads like a config fault rather than a
  # process-topology one. Skipping the session also keeps smbd and its
  # per-connection children in the port's group, so `stop/1` can signal
  # the group and leave nothing holding the share's port.
  defp spawn_smbd(conf_path, dir, port, share) do
    stdout_path = Path.join(dir, "smbd.stdout")

    erlang_port =
      Port.open({:spawn_executable, System.find_executable("sh")}, [
        :binary,
        :exit_status,
        :hide,
        args: [
          "-c",
          "exec smbd --foreground --no-process-group --debug-stdout " <>
            "--configfile=#{conf_path} > #{stdout_path} 2>&1"
        ]
      ])

    {:os_pid, os_pid} = Port.info(erlang_port, :os_pid)

    %{port: port, os_pid: os_pid, share: share, dir: dir, erlang_port: erlang_port}
  end

  # Polling a share listing rather than the port: a bound socket only says
  # the daemon reached `listen`, and a tree connect issued before it has
  # loaded its modules fails in a way that reads as a module fault.
  defp await_ready!(server, attempts \\ 100)

  defp await_ready!(server, 0) do
    raise "smbd never answered a share listing on port #{server.port}:\n#{logs(server)}"
  end

  defp await_ready!(server, attempts) do
    # Authenticated, like every other call: with guest access gone, `-N` gets
    # NT_STATUS_LOGON_FAILURE and this would spin until it gave up, reporting a
    # server that never came ready when it had.
    case System.cmd(
           "smbclient",
           [
             "-L",
             "localhost",
             "-p",
             Integer.to_string(server.port),
             "-U",
             "#{server.user}%#{@smb_password}"
           ],
           stderr_to_stdout: true
         ) do
      {_output, 0} ->
        :ok

      _ ->
        Process.sleep(100)
        await_ready!(server, attempts - 1)
    end
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
