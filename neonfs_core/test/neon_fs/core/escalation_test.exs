defmodule NeonFS.Core.EscalationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.Escalation

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_escalation_manager()
    on_exit(fn -> cleanup_test_dirs() end)
    :ok
  end

  @base_attrs %{
    category: "quorum",
    severity: :warning,
    description: "Quorum lost mid-write — retry or abort?",
    options: [
      %{value: "retry", label: "Retry the write"},
      %{value: "abort", label: "Abort the write"}
    ]
  }

  describe "create/1" do
    test "generates a pending escalation with defaults" do
      assert {:ok, escalation} = Escalation.create(@base_attrs)

      assert is_binary(escalation.id)
      assert String.starts_with?(escalation.id, "esc-")
      assert escalation.category == "quorum"
      assert escalation.severity == :warning
      assert escalation.status == :pending
      assert escalation.choice == nil
      assert is_nil(escalation.resolved_at)
      assert %DateTime{} = escalation.created_at
    end

    test "accepts a caller-supplied id" do
      attrs = Map.put(@base_attrs, :id, "esc-custom-1")

      assert {:ok, %{id: "esc-custom-1"}} = Escalation.create(attrs)
    end

    test "normalises plain-string options to {value, label} pairs" do
      attrs = Map.put(@base_attrs, :options, ["retry", :abort])

      assert {:ok, escalation} = Escalation.create(attrs)

      assert escalation.options == [
               %{value: "retry", label: "retry"},
               %{value: "abort", label: "abort"}
             ]
    end

    test "rejects attrs missing a required field" do
      bad = Map.delete(@base_attrs, :description)

      assert {:error, {:missing_field, :description}} = Escalation.create(bad)
    end
  end

  describe "get/1" do
    test "returns a pending escalation by id" do
      {:ok, created} = Escalation.create(@base_attrs)

      assert {:ok, found} = Escalation.get(created.id)
      assert found.id == created.id
    end

    test "returns :not_found for an unknown id" do
      assert {:error, :not_found} = Escalation.get("esc-nope")
    end
  end

  describe "list/1" do
    test "returns escalations sorted by created_at" do
      {:ok, first} = Escalation.create(@base_attrs)
      Process.sleep(5)
      {:ok, second} = Escalation.create(@base_attrs)

      assert [a, b] = Escalation.list()
      assert a.id == first.id
      assert b.id == second.id
    end

    test "filters by status" do
      {:ok, raised} = Escalation.create(@base_attrs)
      {:ok, _resolved} = Escalation.create(@base_attrs)
      {:ok, _} = Escalation.resolve(raised.id, "retry")

      pending = Escalation.list(status: :pending)
      resolved = Escalation.list(status: :resolved)

      assert length(pending) == 1
      assert length(resolved) == 1
    end

    test "filters by category" do
      {:ok, _} = Escalation.create(@base_attrs)
      {:ok, _} = Escalation.create(Map.put(@base_attrs, :category, "drive_health"))

      assert [one] = Escalation.list(category: "quorum")
      assert one.category == "quorum"
    end
  end

  describe "resolve/2" do
    test "moves a pending escalation to :resolved with the chosen option" do
      {:ok, created} = Escalation.create(@base_attrs)

      assert {:ok, resolved} = Escalation.resolve(created.id, "retry")
      assert resolved.status == :resolved
      assert resolved.choice == "retry"
      assert %DateTime{} = resolved.resolved_at
    end

    test "returns :not_found for an unknown id" do
      assert {:error, :not_found} = Escalation.resolve("esc-nope", "retry")
    end

    test "rejects resolution of an already-resolved escalation" do
      {:ok, created} = Escalation.create(@base_attrs)
      {:ok, _} = Escalation.resolve(created.id, "retry")

      assert {:error, :already_resolved} = Escalation.resolve(created.id, "abort")
    end

    test "rejects a choice that isn't one of the offered options" do
      {:ok, created} = Escalation.create(@base_attrs)

      assert {:error, {:invalid_choice, "wibble"}} = Escalation.resolve(created.id, "wibble")
    end
  end

  describe "delete/1" do
    test "removes an escalation" do
      {:ok, created} = Escalation.create(@base_attrs)

      assert :ok = Escalation.delete(created.id)
      assert {:error, :not_found} = Escalation.get(created.id)
    end

    test "returns :not_found when deleting a missing escalation" do
      assert {:error, :not_found} = Escalation.delete("esc-nope")
    end
  end

  describe "telemetry" do
    test "emits :raised event on create/1" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :escalation, :raised]])

      {:ok, _} = Escalation.create(@base_attrs)

      assert_receive {[:neonfs, :escalation, :raised], ^ref, %{count: 1}, metadata}
      assert metadata.category == "quorum"
      assert metadata.severity == :warning
    end

    test "emits :resolved event on resolve/2" do
      {:ok, created} = Escalation.create(@base_attrs)
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :escalation, :resolved]])

      {:ok, _} = Escalation.resolve(created.id, "retry")

      assert_receive {[:neonfs, :escalation, :resolved], ^ref, %{count: 1}, metadata}
      assert metadata.choice == "retry"
    end

    test "emits :pending_by_category aggregate on state changes" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :escalation, :pending_by_category]
        ])

      {:ok, _} = Escalation.create(@base_attrs)

      assert_receive {[:neonfs, :escalation, :pending_by_category], ^ref, %{count: 1},
                      %{category: "quorum"}}
    end
  end
end
