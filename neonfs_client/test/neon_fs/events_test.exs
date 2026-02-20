defmodule NeonFS.EventsTest do
  use ExUnit.Case, async: true

  alias NeonFS.Events.{
    DirCreated,
    DirDeleted,
    DirRenamed,
    Envelope,
    FileAclChanged,
    FileAttrsChanged,
    FileContentUpdated,
    FileCreated,
    FileDeleted,
    FileRenamed,
    FileTruncated,
    VolumeAclChanged,
    VolumeCreated,
    VolumeDeleted,
    VolumeUpdated
  }

  @volume_id "vol-test-123"
  @file_id "file-abc-456"

  describe "file content events" do
    test "FileCreated requires volume_id, file_id, path" do
      event = %FileCreated{volume_id: @volume_id, file_id: @file_id, path: "/hello.txt"}
      assert event.volume_id == @volume_id
      assert event.file_id == @file_id
      assert event.path == "/hello.txt"
    end

    test "FileCreated raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(FileCreated, %{}) end
      assert_raise ArgumentError, fn -> struct!(FileCreated, %{volume_id: @volume_id}) end
    end

    test "FileContentUpdated requires volume_id, file_id, path" do
      event = %FileContentUpdated{volume_id: @volume_id, file_id: @file_id, path: "/data.bin"}
      assert event.volume_id == @volume_id
      assert event.file_id == @file_id
      assert event.path == "/data.bin"
    end

    test "FileContentUpdated raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(FileContentUpdated, %{}) end
    end

    test "FileTruncated requires volume_id, file_id, path" do
      event = %FileTruncated{volume_id: @volume_id, file_id: @file_id, path: "/log.txt"}
      assert event.volume_id == @volume_id
      assert event.file_id == @file_id
      assert event.path == "/log.txt"
    end

    test "FileTruncated raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(FileTruncated, %{}) end
    end

    test "FileDeleted requires volume_id, file_id, path" do
      event = %FileDeleted{volume_id: @volume_id, file_id: @file_id, path: "/old.txt"}
      assert event.volume_id == @volume_id
      assert event.file_id == @file_id
      assert event.path == "/old.txt"
    end

    test "FileDeleted raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(FileDeleted, %{}) end
    end
  end

  describe "file attribute events" do
    test "FileAttrsChanged requires volume_id, file_id, path" do
      event = %FileAttrsChanged{volume_id: @volume_id, file_id: @file_id, path: "/config.yml"}
      assert event.volume_id == @volume_id
      assert event.file_id == @file_id
      assert event.path == "/config.yml"
    end

    test "FileAttrsChanged raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(FileAttrsChanged, %{}) end
    end

    test "FileRenamed requires volume_id, file_id, old_path, new_path" do
      event = %FileRenamed{
        volume_id: @volume_id,
        file_id: @file_id,
        old_path: "/old_name.txt",
        new_path: "/new_name.txt"
      }

      assert event.volume_id == @volume_id
      assert event.file_id == @file_id
      assert event.old_path == "/old_name.txt"
      assert event.new_path == "/new_name.txt"
    end

    test "FileRenamed raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(FileRenamed, %{volume_id: @volume_id}) end
    end
  end

  describe "ACL events" do
    test "VolumeAclChanged requires volume_id" do
      event = %VolumeAclChanged{volume_id: @volume_id}
      assert event.volume_id == @volume_id
    end

    test "VolumeAclChanged raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(VolumeAclChanged, %{}) end
    end

    test "FileAclChanged requires volume_id, path" do
      event = %FileAclChanged{volume_id: @volume_id, path: "/secret.txt"}
      assert event.volume_id == @volume_id
      assert event.path == "/secret.txt"
    end

    test "FileAclChanged raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(FileAclChanged, %{}) end
    end
  end

  describe "directory events" do
    test "DirCreated requires volume_id, path" do
      event = %DirCreated{volume_id: @volume_id, path: "/subdir"}
      assert event.volume_id == @volume_id
      assert event.path == "/subdir"
    end

    test "DirCreated raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(DirCreated, %{}) end
    end

    test "DirDeleted requires volume_id, path" do
      event = %DirDeleted{volume_id: @volume_id, path: "/old_dir"}
      assert event.volume_id == @volume_id
      assert event.path == "/old_dir"
    end

    test "DirDeleted raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(DirDeleted, %{}) end
    end

    test "DirRenamed requires volume_id, old_path, new_path" do
      event = %DirRenamed{volume_id: @volume_id, old_path: "/a", new_path: "/b"}
      assert event.volume_id == @volume_id
      assert event.old_path == "/a"
      assert event.new_path == "/b"
    end

    test "DirRenamed raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(DirRenamed, %{}) end
    end
  end

  describe "volume events" do
    test "VolumeCreated requires volume_id" do
      event = %VolumeCreated{volume_id: @volume_id}
      assert event.volume_id == @volume_id
    end

    test "VolumeCreated raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(VolumeCreated, %{}) end
    end

    test "VolumeUpdated requires volume_id" do
      event = %VolumeUpdated{volume_id: @volume_id}
      assert event.volume_id == @volume_id
    end

    test "VolumeUpdated raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(VolumeUpdated, %{}) end
    end

    test "VolumeDeleted requires volume_id" do
      event = %VolumeDeleted{volume_id: @volume_id}
      assert event.volume_id == @volume_id
    end

    test "VolumeDeleted raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(VolumeDeleted, %{}) end
    end
  end

  describe "Envelope" do
    test "wraps a file content event" do
      event = %FileCreated{volume_id: @volume_id, file_id: @file_id, path: "/test.txt"}

      envelope = %Envelope{
        event: event,
        source_node: :core@localhost,
        sequence: 1,
        hlc_timestamp: {1_700_000_000_000, 0, :core@localhost}
      }

      assert envelope.event == event
      assert envelope.source_node == :core@localhost
      assert envelope.sequence == 1
      assert envelope.hlc_timestamp == {1_700_000_000_000, 0, :core@localhost}
    end

    test "wraps a volume event" do
      event = %VolumeDeleted{volume_id: @volume_id}

      envelope = %Envelope{
        event: event,
        source_node: :core@localhost,
        sequence: 42,
        hlc_timestamp: {1_700_000_000_000, 5, :core@localhost}
      }

      assert envelope.event == event
      assert envelope.sequence == 42
    end

    test "wraps a directory event" do
      event = %DirRenamed{volume_id: @volume_id, old_path: "/a", new_path: "/b"}

      envelope = %Envelope{
        event: event,
        source_node: :core@localhost,
        sequence: 7,
        hlc_timestamp: {1_700_000_000_000, 0, :core@localhost}
      }

      assert envelope.event == event
    end

    test "wraps an ACL event" do
      event = %FileAclChanged{volume_id: @volume_id, path: "/secret"}

      envelope = %Envelope{
        event: event,
        source_node: :core@localhost,
        sequence: 3,
        hlc_timestamp: {1_700_000_000_000, 0, :core@localhost}
      }

      assert envelope.event == event
    end

    test "raises when required keys are missing" do
      assert_raise ArgumentError, fn -> struct!(Envelope, %{}) end

      assert_raise ArgumentError, fn ->
        struct!(Envelope, %{event: %VolumeCreated{volume_id: "v"}})
      end
    end
  end

  describe "type consistency" do
    @all_event_modules [
      DirCreated,
      DirDeleted,
      DirRenamed,
      FileAclChanged,
      FileAttrsChanged,
      FileContentUpdated,
      FileCreated,
      FileDeleted,
      FileRenamed,
      FileTruncated,
      VolumeAclChanged,
      VolumeCreated,
      VolumeDeleted,
      VolumeUpdated
    ]

    test "all event structs have a volume_id field" do
      for module <- @all_event_modules do
        fields = module.__struct__() |> Map.keys()
        assert :volume_id in fields, "#{inspect(module)} missing :volume_id field"
      end
    end

    test "there are exactly 14 event types" do
      assert length(@all_event_modules) == 14
    end
  end
end
