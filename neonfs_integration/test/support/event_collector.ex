defmodule NeonFS.Integration.EventCollector do
  @moduledoc """
  Re-export shim — implementation has moved to
  `NeonFS.TestSupport.EventCollector` (sub-issue #599 of #582).
  Deleted by #604 once every interface package has switched its
  aliases.
  """

  alias NeonFS.TestSupport.EventCollector

  for {fun, arity} <- EventCollector.__info__(:functions) do
    args = Macro.generate_arguments(arity, __MODULE__)

    def unquote(fun)(unquote_splicing(args)) do
      apply(EventCollector, unquote(fun), [unquote_splicing(args)])
    end
  end
end
