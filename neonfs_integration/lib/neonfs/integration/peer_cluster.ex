defmodule NeonFS.Integration.PeerCluster do
  @moduledoc """
  Re-export shim — the implementation has moved to
  `NeonFS.TestSupport.PeerCluster` (sub-issue #599 of #582). This
  module forwards every public function and type alias so existing
  test callsites compile unchanged during the per-interface
  migration; deleted by #604 once every interface package has
  switched its aliases.
  """

  alias NeonFS.TestSupport.PeerCluster

  @type cluster :: PeerCluster.cluster()
  @type node_info :: PeerCluster.node_info()

  for {fun, arity} <- PeerCluster.__info__(:functions) do
    args = Macro.generate_arguments(arity, __MODULE__)

    def unquote(fun)(unquote_splicing(args)) do
      apply(PeerCluster, unquote(fun), [unquote_splicing(args)])
    end
  end
end
