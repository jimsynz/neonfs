defmodule NeonFS.MixProject do
  use Mix.Project
  @moduledoc false

  @version "0.1.0"

  defmodule DynamicAlias do
    defstruct []

    @behaviour Access

    @localonly [:loadconfig, :new]

    @impl Access
    def fetch(_, task) when task in @localonly, do: :error

    def fetch(_, task) do
      {:ok, &in_all(&1, task)}
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
      version: @version
    ]
end
