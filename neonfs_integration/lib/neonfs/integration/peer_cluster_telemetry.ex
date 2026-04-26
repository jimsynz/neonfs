defmodule NeonFS.Integration.PeerClusterTelemetry do
  @moduledoc """
  Re-export shim — implementation has moved to
  `NeonFS.TestSupport.PeerClusterTelemetry` (sub-issue #599 of #582).
  Deleted by #604 once every interface package has switched its
  aliases.
  """

  alias NeonFS.TestSupport.PeerClusterTelemetry

  for {fun, arity} <- PeerClusterTelemetry.__info__(:functions) do
    args = Macro.generate_arguments(arity, __MODULE__)

    def unquote(fun)(unquote_splicing(args)) do
      apply(PeerClusterTelemetry, unquote(fun), [unquote_splicing(args)])
    end
  end
end
