defmodule NeonFS.Core.RaServerSanitiseTest do
  @moduledoc """
  Unit tests for `RaServer.sanitise_initial_state/2` (#688). The
  full force-reset path is exercised by the peer-cluster
  `force_reset_test.exs` integration test; this slice covers the
  pure-function purge in isolation so the regression is cheap to
  catch.
  """

  use ExUnit.Case, async: true

  alias NeonFS.Core.RaServer

  @survivor :survivor@host
  @departed :departed@host
  @other_departed :another@host

  describe "sanitise_initial_state/2 services" do
    test "drops service entries pointing at departed nodes" do
      state = %{
        services: %{
          {@survivor, :core} => %{node: @survivor, type: :core},
          {@departed, :core} => %{node: @departed, type: :core},
          {@departed, :nfs} => %{node: @departed, type: :nfs},
          {@other_departed, :core} => %{node: @other_departed, type: :core}
        }
      }

      sanitised = RaServer.sanitise_initial_state(state, @survivor)

      assert Map.keys(sanitised.services) == [{@survivor, :core}]
    end

    test "is a no-op when every service entry already points at the survivor" do
      state = %{
        services: %{
          {@survivor, :core} => %{node: @survivor, type: :core},
          {@survivor, :nfs} => %{node: @survivor, type: :nfs}
        }
      }

      assert RaServer.sanitise_initial_state(state, @survivor) == state
    end

    test "tolerates an empty services map" do
      assert %{services: %{}} = RaServer.sanitise_initial_state(%{services: %{}}, @survivor)
    end
  end

  describe "sanitise_initial_state/2 segment_assignments" do
    test "filters replica_set down to the survivor only" do
      state = %{
        segment_assignments: %{
          "seg-1" => %{replica_set: [@survivor, @departed], version: 5},
          "seg-2" => %{replica_set: [@departed, @other_departed], version: 3},
          "seg-3" => %{replica_set: [@survivor], version: 1}
        }
      }

      sanitised = RaServer.sanitise_initial_state(state, @survivor)

      assert sanitised.segment_assignments["seg-1"].replica_set == [@survivor]
      assert sanitised.segment_assignments["seg-2"].replica_set == []
      assert sanitised.segment_assignments["seg-3"].replica_set == [@survivor]
    end

    test "preserves the version field on each assignment" do
      state = %{
        segment_assignments: %{
          "seg" => %{replica_set: [@survivor, @departed], version: 42}
        }
      }

      sanitised = RaServer.sanitise_initial_state(state, @survivor)

      assert sanitised.segment_assignments["seg"].version == 42
    end
  end

  describe "sanitise_initial_state/2 misc" do
    test "passes through non-map states untouched" do
      assert RaServer.sanitise_initial_state(:not_a_map, @survivor) == :not_a_map
    end

    test "passes through state without :services or :segment_assignments keys" do
      state = %{some_other_field: 1}
      assert RaServer.sanitise_initial_state(state, @survivor) == state
    end

    test "preserves every other field on the state map" do
      state = %{
        services: %{{@departed, :core} => %{}},
        segment_assignments: %{},
        chunks: %{"hash" => :something},
        files: %{"path" => :else},
        version: 99
      }

      sanitised = RaServer.sanitise_initial_state(state, @survivor)

      assert sanitised.services == %{}
      assert sanitised.chunks == %{"hash" => :something}
      assert sanitised.files == %{"path" => :else}
      assert sanitised.version == 99
    end
  end
end
