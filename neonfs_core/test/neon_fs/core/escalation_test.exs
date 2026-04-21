defmodule NeonFS.Core.EscalationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{Escalation, RaServer}

  @moduletag :tmp_dir

  # Ra persists across tests in the same module (see Codebase-Patterns
  # wiki), so each test namespaces its `:category` with a unique value
  # to stay robust against leftover state from earlier tests.
  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()

    # Ticker is started with tick disabled — tests that exercise expiry
    # call `Escalation.expire_overdue/0` directly.
    start_escalation_ticker()

    on_exit(fn -> cleanup_test_dirs() end)

    {:ok, category: "esc_test_#{System.unique_integer([:positive])}"}
  end

  defp base_attrs(category) do
    %{
      category: category,
      severity: :warning,
      description: "Quorum lost mid-write — retry or abort?",
      options: [
        %{value: "retry", label: "Retry the write"},
        %{value: "abort", label: "Abort the write"}
      ]
    }
  end

  describe "create/1" do
    test "generates a pending escalation with defaults", %{category: cat} do
      assert {:ok, escalation} = Escalation.create(base_attrs(cat))

      assert is_binary(escalation.id)
      assert String.starts_with?(escalation.id, "esc-")
      assert escalation.category == cat
      assert escalation.severity == :warning
      assert escalation.status == :pending
      assert escalation.choice == nil
      assert is_nil(escalation.resolved_at)
      assert %DateTime{} = escalation.created_at
    end

    test "accepts a caller-supplied id", %{category: cat} do
      id = "esc-custom-#{System.unique_integer([:positive])}"
      attrs = Map.put(base_attrs(cat), :id, id)

      assert {:ok, %{id: ^id}} = Escalation.create(attrs)
    end

    test "normalises plain-string options to {value, label} pairs", %{category: cat} do
      attrs = Map.put(base_attrs(cat), :options, ["retry", :abort])

      assert {:ok, escalation} = Escalation.create(attrs)

      assert escalation.options == [
               %{value: "retry", label: "retry"},
               %{value: "abort", label: "abort"}
             ]
    end

    test "rejects attrs missing a required field", %{category: cat} do
      bad = Map.delete(base_attrs(cat), :description)

      assert {:error, {:missing_field, :description}} = Escalation.create(bad)
    end
  end

  describe "get/1" do
    test "returns a pending escalation by id", %{category: cat} do
      {:ok, created} = Escalation.create(base_attrs(cat))

      assert {:ok, found} = Escalation.get(created.id)
      assert found.id == created.id
    end

    test "returns :not_found for an unknown id" do
      assert {:error, :not_found} =
               Escalation.get("esc-nope-#{System.unique_integer([:positive])}")
    end
  end

  describe "list/1" do
    test "returns escalations filtered by category, sorted by created_at", %{category: cat} do
      {:ok, first} = Escalation.create(base_attrs(cat))
      Process.sleep(5)
      {:ok, second} = Escalation.create(base_attrs(cat))

      assert [a, b] = Escalation.list(category: cat)
      assert a.id == first.id
      assert b.id == second.id
    end

    test "filters by status within a category", %{category: cat} do
      {:ok, raised} = Escalation.create(base_attrs(cat))
      {:ok, _still_pending} = Escalation.create(base_attrs(cat))
      {:ok, _} = Escalation.resolve(raised.id, "retry")

      pending = Escalation.list(status: :pending, category: cat)
      resolved = Escalation.list(status: :resolved, category: cat)

      assert length(pending) == 1
      assert length(resolved) == 1
    end

    test "filter by category scopes results", %{category: cat} do
      other_cat = "drive_#{System.unique_integer([:positive])}"

      {:ok, _} = Escalation.create(base_attrs(cat))
      {:ok, _} = Escalation.create(Map.put(base_attrs(cat), :category, other_cat))

      assert [one] = Escalation.list(category: cat)
      assert one.category == cat
    end
  end

  describe "resolve/2" do
    test "moves a pending escalation to :resolved with the chosen option", %{category: cat} do
      {:ok, created} = Escalation.create(base_attrs(cat))

      assert {:ok, resolved} = Escalation.resolve(created.id, "retry")
      assert resolved.status == :resolved
      assert resolved.choice == "retry"
      assert %DateTime{} = resolved.resolved_at
    end

    test "returns :not_found for an unknown id" do
      assert {:error, :not_found} =
               Escalation.resolve("esc-nope-#{System.unique_integer([:positive])}", "retry")
    end

    test "rejects resolution of an already-resolved escalation", %{category: cat} do
      {:ok, created} = Escalation.create(base_attrs(cat))
      {:ok, _} = Escalation.resolve(created.id, "retry")

      assert {:error, :already_resolved} = Escalation.resolve(created.id, "abort")
    end

    test "rejects a choice that isn't one of the offered options", %{category: cat} do
      {:ok, created} = Escalation.create(base_attrs(cat))

      assert {:error, {:invalid_choice, "wibble"}} = Escalation.resolve(created.id, "wibble")
    end
  end

  describe "delete/1" do
    test "removes an escalation", %{category: cat} do
      {:ok, created} = Escalation.create(base_attrs(cat))

      assert :ok = Escalation.delete(created.id)
      assert {:error, :not_found} = Escalation.get(created.id)
    end

    test "returns :not_found when deleting a missing escalation" do
      assert {:error, :not_found} =
               Escalation.delete("esc-nope-#{System.unique_integer([:positive])}")
    end
  end

  describe "expire_overdue/0" do
    test "moves overdue pending escalations to :expired and emits telemetry", %{category: cat} do
      past = DateTime.add(DateTime.utc_now(), -60, :second)

      {:ok, overdue} = Escalation.create(Map.put(base_attrs(cat), :expires_at, past))
      {:ok, fresh} = Escalation.create(base_attrs(cat))

      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :escalation, :expired]])

      :ok = Escalation.expire_overdue()

      assert {:ok, %{status: :expired}} = Escalation.get(overdue.id)
      assert {:ok, %{status: :pending}} = Escalation.get(fresh.id)

      assert_received {[:neonfs, :escalation, :expired], ^ref, %{count: 1}, %{id: id}}
      assert id == overdue.id
    end
  end

  describe "telemetry" do
    test "emits :raised event on create/1", %{category: cat} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :escalation, :raised]])

      {:ok, _} = Escalation.create(base_attrs(cat))

      assert_receive {[:neonfs, :escalation, :raised], ^ref, %{count: 1}, metadata}
      assert metadata.category == cat
      assert metadata.severity == :warning
    end

    test "emits :resolved event on resolve/2", %{category: cat} do
      {:ok, created} = Escalation.create(base_attrs(cat))
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :escalation, :resolved]])

      {:ok, _} = Escalation.resolve(created.id, "retry")

      assert_receive {[:neonfs, :escalation, :resolved], ^ref, %{count: 1}, metadata}
      assert metadata.choice == "retry"
    end

    test "emits :pending_by_category aggregate on state changes", %{category: cat} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :escalation, :pending_by_category]
        ])

      {:ok, _} = Escalation.create(base_attrs(cat))

      assert_receive {[:neonfs, :escalation, :pending_by_category], ^ref, %{count: count},
                      %{category: ^cat}}

      assert count >= 1
    end
  end

  describe "Ra unavailable" do
    setup do
      stop_ra()
      :ok
    end

    test "writes return {:error, :ra_not_available}", %{category: cat} do
      assert {:error, :ra_not_available} = Escalation.create(base_attrs(cat))
    end

    test "reads return :not_found / empty when the state is unreachable" do
      assert {:error, :not_found} = Escalation.get("esc-anything")
      assert [] = Escalation.list()
    end
  end
end
