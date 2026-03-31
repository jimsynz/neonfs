defmodule NeonFS.Core.DriveCommand do
  @moduledoc """
  Behaviour for drive power management commands.

  Implementations control how drives are spun down and spun up.
  The `Default` implementation uses `hdparm` system commands.
  The `Test` implementation tracks calls and returns configurable results.
  """

  @doc "Puts the drive into standby (spins down the platters)."
  @callback spin_down(path :: String.t()) :: :ok | {:error, term()}

  @doc "Wakes the drive from standby (spins up the platters)."
  @callback spin_up(path :: String.t()) :: :ok | {:error, term()}

  @doc "Checks whether the drive is currently active or in standby."
  @callback check_state(path :: String.t()) :: :active | :standby

  defmodule Default do
    @moduledoc """
    Default drive command implementation using `hdparm` system commands.

    Requires `hdparm` to be installed and the process to have appropriate
    permissions (typically root or CAP_SYS_RAWIO).
    """

    @behaviour NeonFS.Core.DriveCommand

    @impl true
    @spec spin_down(String.t()) :: :ok | {:error, term()}
    def spin_down(path) do
      case System.cmd("hdparm", ["-Y", path], stderr_to_stdout: true) do
        {_output, 0} -> :ok
        {output, _} -> {:error, String.trim(output)}
      end
    rescue
      e -> {:error, Exception.message(e)}
    end

    @impl true
    @spec spin_up(String.t()) :: :ok | {:error, term()}
    def spin_up(path) do
      case System.cmd("hdparm", ["-C", path], stderr_to_stdout: true) do
        {_output, 0} -> :ok
        {output, _} -> {:error, String.trim(output)}
      end
    rescue
      e -> {:error, Exception.message(e)}
    end

    @impl true
    @spec check_state(String.t()) :: :active | :standby
    def check_state(path) do
      case System.cmd("hdparm", ["-C", path], stderr_to_stdout: true) do
        {output, 0} ->
          if String.contains?(output, "standby"), do: :standby, else: :active

        _ ->
          :active
      end
    rescue
      _ -> :active
    end
  end

  defmodule Test do
    @moduledoc """
    Test implementation of `DriveCommand` that tracks calls and returns
    configurable results via an ETS table.

    ## Usage

        NeonFS.Core.DriveCommand.Test.setup()
        NeonFS.Core.DriveCommand.Test.configure(:spin_up_result, {:error, :device_busy})
        # ... run DriveState ...
        calls = NeonFS.Core.DriveCommand.Test.get_calls()

    ## Configurable Keys

      * `:spin_down_result` — return value for `spin_down/1` (default: `:ok`)
      * `:spin_up_result` — return value for `spin_up/1` (default: `:ok`)
      * `:check_state_result` — return value for `check_state/1` (default: `:active`)
      * `:spin_down_delay` — milliseconds to sleep in `spin_down/1` (default: 0)
      * `:spin_up_delay` — milliseconds to sleep in `spin_up/1` (default: 0)
    """

    @behaviour NeonFS.Core.DriveCommand

    @ets_table :drive_command_test

    @doc "Sets up the ETS table. Call once before tests."
    @spec setup() :: :ok
    def setup do
      if :ets.whereis(@ets_table) != :undefined do
        :ets.delete_all_objects(@ets_table)
      else
        :ets.new(@ets_table, [:named_table, :public, :set])
      end

      :ets.insert(@ets_table, [
        {:call_counter, 0},
        {:spin_down_result, :ok},
        {:spin_up_result, :ok},
        {:check_state_result, :active},
        {:spin_up_delay, 0},
        {:spin_down_delay, 0}
      ])

      :ok
    end

    @doc "Configures a result or delay value."
    @spec configure(atom(), term()) :: true
    def configure(key, value) do
      :ets.insert(@ets_table, {key, value})
    end

    @doc "Returns all recorded calls in chronological order."
    @spec get_calls() :: [{atom(), String.t()}]
    def get_calls do
      @ets_table
      |> :ets.match({{:call, :"$1"}, :"$2", :"$3"})
      |> Enum.sort_by(fn [seq, _action, _path] -> seq end)
      |> Enum.map(fn [_seq, action, path] -> {action, path} end)
    end

    @doc "Clears the call log."
    @spec reset() :: true
    def reset do
      :ets.match_delete(@ets_table, {{:call, :_}, :_, :_})
      :ets.insert(@ets_table, {:call_counter, 0})
    end

    @impl true
    @spec spin_down(String.t()) :: :ok | {:error, term()}
    def spin_down(path) do
      record_call(:spin_down, path)
      delay = get_config(:spin_down_delay, 0)
      if delay > 0, do: Process.sleep(delay)
      get_config(:spin_down_result, :ok)
    end

    @impl true
    @spec spin_up(String.t()) :: :ok | {:error, term()}
    def spin_up(path) do
      record_call(:spin_up, path)
      delay = get_config(:spin_up_delay, 0)
      if delay > 0, do: Process.sleep(delay)
      get_config(:spin_up_result, :ok)
    end

    @impl true
    @spec check_state(String.t()) :: :active | :standby
    def check_state(path) do
      record_call(:check_state, path)
      get_config(:check_state_result, :active)
    end

    defp record_call(action, path) do
      seq = :ets.update_counter(@ets_table, :call_counter, 1)
      :ets.insert(@ets_table, {{:call, seq}, action, path})
    end

    defp get_config(key, default) do
      case :ets.lookup(@ets_table, key) do
        [{^key, value}] -> value
        [] -> default
      end
    end
  end
end
