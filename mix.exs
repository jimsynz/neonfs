defmodule NeonFS.MixProject do
  use Mix.Project
  @moduledoc false

  @version "0.3.2"

  defmodule DynamicAlias do
    defstruct []

    @behaviour Access

    @workspace_only [
      :loadconfig,
      :new,
      :"archive.check",
      :"deps.loadpaths",
      :"git_ops.release",
      :"local.hex",
      :"local.rebar"
    ]

    @impl Access
    def fetch(_, task) when task in @workspace_only, do: :error

    def fetch(_, task) do
      if System.get_env("WORKSPACE_ONLY") do
        :error
      else
        {:ok, &in_all(&1, task)}
      end
    end

    @impl Access
    def get_and_update(data, _, _), do: data

    @impl Access
    def pop(data, _), do: {nil, data}

    defp in_all(argv, task) do
      task = to_string(task)
      env = if Mix.env() != :dev, do: [{"MIX_ENV", "#{Mix.env()}"}], else: []

      "*/mix.exs"
      |> Path.wildcard()
      |> Enum.map(fn mixfile ->
        project = Path.dirname(mixfile)
        Mix.shell().info("Running `mix #{task}` in `#{project}`")

        {_, exitcode} =
          System.cmd("mix", [task | argv],
            env: env,
            cd: project,
            use_stdio: false
          )

        {project, task, exitcode}
      end)
      |> Enum.reject(&(elem(&1, 2) == 0))
      |> Enum.map(fn {project, task, exitcode} ->
        "Task `mix #{task}` failed in `#{project}`: exit code #{exitcode}"
      end)
      |> case do
        [] -> :ok
        errors -> raise Enum.join(errors, "\n")
      end
    end
  end

  def project,
    do: [
      app: :neonfs,
      aliases: %DynamicAlias{},
      deps: [
        {:git_ops, "~> 2.10", only: [:dev, :test], runtime: false}
      ],
      version: @version
    ]
end
