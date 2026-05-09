defmodule NeonFS.TestSupport.CARotateRPC do
  @moduledoc false
  # Test-only RPC mod for `cluster ca rotate` peer-cluster integration
  # tests.
  #
  # The orchestrator walks `[Node.self() | Node.list()]`, which in a
  # peer-cluster test pulls in the ExUnit runner — a connected BEAM
  # peer that does not run `NeonFS.TLSDistConfig` against a real
  # `tls_dir`. Forwarding `install_node_cert` to the runner returns
  # `{:error, :enoent}` and aborts the rotation.
  #
  # This stub forwards every RPC to `:rpc.call/4` for genuine cluster
  # members and short-circuits with `:ok` for the node configured via
  # `:ca_rotate_skip_node`. Tests install it on each peer with
  # `Application.put_env(:neonfs_core, :ca_rotate_rpc_mod, __MODULE__)`
  # before running the orchestrator.

  @doc """
  Drop-in replacement for `:rpc.call/4` with one node bypassed.

  Reads the bypass target from
  `Application.get_env(:neonfs_test_support, :ca_rotate_skip_node)`
  on the local peer; tests must seed it on every peer before starting
  rotation.
  """
  @spec call(node(), module(), atom(), list()) :: term()
  def call(node, mod, fun, args) do
    case Application.get_env(:neonfs_test_support, :ca_rotate_skip_node) do
      ^node -> :ok
      _ -> :rpc.call(node, mod, fun, args)
    end
  end
end
