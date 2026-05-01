defmodule NeonFS.Integration.ContainerdDaemon do
  @moduledoc """
  Spawn and tear down a `containerd` subprocess for integration tests
  (#725 — first slice of #554).

  Each test instance gets:

    * an isolated `--root` and `--state` directory under a per-test
      tmpdir,
    * its own gRPC and ttrpc sockets (no contention with the host
      `containerd`),
    * a generated `config.toml` that:
        - disables `io.containerd.grpc.v1.cri` (we don't need the
          CRI plugin and it requires kernel features the runner may
          lack),
        - disables `io.containerd.content.v1.content` (the default
          bolt-backed store) so the proxy plugin is the only content
          backend, which means `ctr content ingest` / `ctr content
          get` route through the caller's UDS,
        - registers a single `[proxy_plugins.neonfs]` of `type =
          "content"` pointing at the caller-supplied UDS path.

  The caller is responsible for binding `neonfs_containerd`'s gRPC
  endpoint to the same socket *before* `start/1` is called — the
  containerd daemon dials the proxy at boot, and a missing socket
  surfaces as a hung `ctr content` call rather than a clean error.
  """

  alias NeonFS.Integration.ContainerdDaemon

  @enforce_keys [:port, :tmp_dir, :grpc_address, :proxy_socket]
  defstruct [:port, :tmp_dir, :grpc_address, :proxy_socket]

  @type t :: %ContainerdDaemon{
          port: port(),
          tmp_dir: Path.t(),
          grpc_address: Path.t(),
          proxy_socket: Path.t()
        }

  @doc """
  Start a containerd subprocess. Blocks until the gRPC socket
  accepts connections, then returns the daemon handle.

  ## Options

    * `:proxy_socket` (required) — UDS path that
      `neonfs_containerd`'s gRPC endpoint is listening on.
    * `:tmp_dir` (optional) — base directory for `--root`,
      `--state`, and the generated config. Defaults to a fresh
      `System.tmp_dir!() / "neonfs-containerd-<unique>"`.
    * `:boot_timeout_ms` (optional) — how long to wait for the
      gRPC socket to come up. Defaults to 10_000.
  """
  @spec start(keyword()) :: {:ok, t()} | {:error, term()}
  def start(opts) do
    proxy_socket = Keyword.fetch!(opts, :proxy_socket)
    tmp_dir = Keyword.get_lazy(opts, :tmp_dir, &mk_tmp_dir/0)
    boot_timeout = Keyword.get(opts, :boot_timeout_ms, 10_000)

    File.mkdir_p!(Path.join(tmp_dir, "root"))
    File.mkdir_p!(Path.join(tmp_dir, "state"))

    grpc_address = Path.join(tmp_dir, "containerd.sock")
    config_path = Path.join(tmp_dir, "config.toml")
    File.write!(config_path, render_config(tmp_dir, grpc_address, proxy_socket))

    log_path = Path.join(tmp_dir, "containerd.log")
    bin = System.find_executable("containerd") || raise "containerd not on PATH"

    port =
      Port.open(
        {:spawn_executable, bin},
        [
          :binary,
          :exit_status,
          {:args, ["--config", config_path, "--log-level", "info"]},
          {:cd, tmp_dir},
          :stderr_to_stdout
        ]
      )

    handle = %ContainerdDaemon{
      port: port,
      tmp_dir: tmp_dir,
      grpc_address: grpc_address,
      proxy_socket: proxy_socket
    }

    case wait_for_socket(grpc_address, boot_timeout, log_path) do
      :ok -> {:ok, handle}
      {:error, reason} -> {:error, {reason, File.read(log_path)}}
    end
  end

  @doc "Stop the daemon and remove its tmpdir."
  @spec stop(t()) :: :ok
  def stop(%ContainerdDaemon{port: port, tmp_dir: tmp_dir}) do
    case Port.info(port) do
      nil ->
        :ok

      info ->
        os_pid = info[:os_pid]
        Port.close(port)
        if os_pid, do: System.cmd("kill", ["-TERM", "#{os_pid}"], stderr_to_stdout: true)
    end

    File.rm_rf!(tmp_dir)
    :ok
  end

  @doc """
  Run `ctr` against this daemon. Returns `{stdout, exit_code}`.

  `args` is the argument list *after* `--address <sock> --namespace
  <ns>` — i.e. `["content", "ls"]` or `["content", "ingest", ...]`.
  """
  @spec ctr(t(), String.t(), [String.t()], keyword()) :: {String.t(), integer()}
  def ctr(%ContainerdDaemon{grpc_address: addr}, namespace, args, opts \\ []) do
    bin = System.find_executable("ctr") || raise "ctr not on PATH"

    System.cmd(
      bin,
      ["--address", addr, "--namespace", namespace] ++ args,
      [stderr_to_stdout: true] ++ opts
    )
  end

  ## Internal

  defp mk_tmp_dir do
    base = Path.join(System.tmp_dir!(), "neonfs-containerd-#{System.unique_integer([:positive])}")
    File.mkdir_p!(base)
    base
  end

  defp render_config(tmp_dir, grpc_address, proxy_socket) do
    """
    version = 2
    root = "#{Path.join(tmp_dir, "root")}"
    state = "#{Path.join(tmp_dir, "state")}"
    disabled_plugins = ["io.containerd.grpc.v1.cri", "io.containerd.content.v1.content"]
    imports = []

    [grpc]
    address = "#{grpc_address}"

    [ttrpc]
    address = "#{grpc_address}.ttrpc"

    [proxy_plugins]
      [proxy_plugins.neonfs]
      type = "content"
      address = "#{proxy_socket}"
    """
  end

  defp wait_for_socket(path, timeout_ms, log_path) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_socket(path, deadline, log_path)
  end

  defp do_wait_for_socket(path, deadline, log_path) do
    cond do
      socket_listening?(path) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :boot_timeout}

      true ->
        Process.sleep(100)
        do_wait_for_socket(path, deadline, log_path)
    end
  end

  defp socket_listening?(path) do
    case File.stat(path) do
      {:ok, %{type: :other}} ->
        case :gen_tcp.connect({:local, path}, 0, [:binary, active: false], 250) do
          {:ok, sock} ->
            :gen_tcp.close(sock)
            true

          {:error, _} ->
            false
        end

      _ ->
        false
    end
  end
end
