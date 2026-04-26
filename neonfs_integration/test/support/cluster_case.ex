defmodule NeonFS.Integration.ClusterCase do
  @moduledoc """
  Re-export shim — the case template has moved to
  `NeonFS.TestSupport.ClusterCase` (sub-issue #599 of #582). Existing
  callers that `use NeonFS.Integration.ClusterCase` (or `import` it
  for the macros / helpers) continue to compile via this shim;
  deleted by #604 once every interface package has switched its
  aliases.
  """

  alias NeonFS.TestSupport.ClusterCase

  defmacro __using__(opts) do
    quote do
      use NeonFS.TestSupport.ClusterCase, unquote(opts)
    end
  end

  defmacro assert_eventually(opts \\ [], do_block) do
    quote do
      require ClusterCase
      ClusterCase.assert_eventually(unquote(opts), unquote(do_block))
    end
  end

  for {fun, arity} <- ClusterCase.__info__(:functions) do
    args = Macro.generate_arguments(arity, __MODULE__)

    def unquote(fun)(unquote_splicing(args)) do
      apply(ClusterCase, unquote(fun), [unquote_splicing(args)])
    end
  end
end
