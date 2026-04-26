defmodule NeonFS.Integration.AppProfiler do
  @moduledoc """
  Re-export shim — implementation has moved to
  `NeonFS.TestSupport.AppProfiler` (sub-issue #599 of #582).
  Deleted by #604 once every interface package has switched its
  aliases.
  """

  alias NeonFS.TestSupport.AppProfiler

  @type timing :: AppProfiler.timing()

  for {fun, arity} <- AppProfiler.__info__(:functions) do
    args = Macro.generate_arguments(arity, __MODULE__)

    def unquote(fun)(unquote_splicing(args)) do
      apply(AppProfiler, unquote(fun), [unquote_splicing(args)])
    end
  end
end
