defmodule NeonFS.Core.DriveCommandTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.DriveCommand

  setup do
    DriveCommand.Test.setup()
    :ok
  end

  describe "Test.setup/0" do
    test "initializes with default values" do
      assert DriveCommand.Test.get_calls() == []
    end

    test "can be called multiple times safely" do
      DriveCommand.Test.setup()
      DriveCommand.Test.setup()
      assert DriveCommand.Test.get_calls() == []
    end
  end

  describe "Test.spin_down/1" do
    test "returns :ok by default" do
      assert :ok = DriveCommand.Test.spin_down("/dev/sda")
    end

    test "records the call" do
      DriveCommand.Test.spin_down("/dev/sda")
      assert [{:spin_down, "/dev/sda"}] = DriveCommand.Test.get_calls()
    end

    test "returns configured error" do
      DriveCommand.Test.configure(:spin_down_result, {:error, :device_busy})
      assert {:error, :device_busy} = DriveCommand.Test.spin_down("/dev/sda")
    end

    test "respects configured delay" do
      DriveCommand.Test.configure(:spin_down_delay, 50)
      start = System.monotonic_time(:millisecond)
      DriveCommand.Test.spin_down("/dev/sda")
      elapsed = System.monotonic_time(:millisecond) - start
      assert elapsed >= 40
    end
  end

  describe "Test.spin_up/1" do
    test "returns :ok by default" do
      assert :ok = DriveCommand.Test.spin_up("/dev/sda")
    end

    test "records the call" do
      DriveCommand.Test.spin_up("/dev/sdb")
      assert [{:spin_up, "/dev/sdb"}] = DriveCommand.Test.get_calls()
    end

    test "returns configured error" do
      DriveCommand.Test.configure(:spin_up_result, {:error, :timeout})
      assert {:error, :timeout} = DriveCommand.Test.spin_up("/dev/sda")
    end
  end

  describe "Test.check_state/1" do
    test "returns :active by default" do
      assert :active = DriveCommand.Test.check_state("/dev/sda")
    end

    test "records the call" do
      DriveCommand.Test.check_state("/dev/sdc")
      assert [{:check_state, "/dev/sdc"}] = DriveCommand.Test.get_calls()
    end

    test "returns configured standby" do
      DriveCommand.Test.configure(:check_state_result, :standby)
      assert :standby = DriveCommand.Test.check_state("/dev/sda")
    end
  end

  describe "Test.get_calls/0" do
    test "returns calls in chronological order" do
      DriveCommand.Test.spin_down("/dev/sda")
      DriveCommand.Test.spin_up("/dev/sda")
      DriveCommand.Test.check_state("/dev/sda")

      assert [
               {:spin_down, "/dev/sda"},
               {:spin_up, "/dev/sda"},
               {:check_state, "/dev/sda"}
             ] = DriveCommand.Test.get_calls()
    end
  end

  describe "Test.reset/0" do
    test "clears the call log" do
      DriveCommand.Test.spin_down("/dev/sda")
      DriveCommand.Test.reset()
      assert [] = DriveCommand.Test.get_calls()
    end
  end
end
