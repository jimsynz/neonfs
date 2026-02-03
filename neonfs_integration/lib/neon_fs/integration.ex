defmodule NeonFS.Integration do
  @moduledoc """
  Integration testing framework for NeonFS using OTP's `:peer` module.

  This package provides tools for running multi-node NeonFS integration tests
  without Docker/Podman containers, using peer nodes for fast, pure-Elixir testing.

  ## Components

  - `NeonFS.Integration.PeerCluster` - Manages peer node lifecycle
  - `NeonFS.Integration.ClusterCase` - ExUnit case template for cluster tests

  ## Example

      defmodule MyIntegrationTest do
        use NeonFS.Integration.ClusterCase, async: false

        @moduletag nodes: 3

        test "cluster operations", %{cluster: cluster} do
          {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])
          # ...
        end
      end
  """
end
