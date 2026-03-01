defmodule NeonFS.Cluster.State.ValidatorTest do
  use ExUnit.Case, async: true

  alias NeonFS.Cluster.State
  alias NeonFS.Cluster.State.Validator

  defp valid_state do
    %State{
      cluster_id: "clust_abc12345",
      cluster_name: "test-cluster",
      created_at: ~U[2026-01-01 00:00:00Z],
      master_key: "test-master-key-abc123",
      this_node: %{
        id: "node-1",
        name: :neonfs_core@localhost,
        joined_at: ~U[2026-01-01 00:00:00Z]
      },
      drives: [
        %{
          "id" => "drive-1",
          "path" => "/var/lib/neonfs/data/hot",
          "tier" => "hot",
          "capacity" => "1000000000"
        }
      ],
      gc: %{"interval_ms" => 3_600_000, "pressure_threshold" => 0.85},
      worker: %{"max_concurrent" => 4, "max_per_minute" => 60, "drive_concurrency" => 2},
      ra_cluster_members: [:neonfs_core@localhost]
    }
  end

  defp find_error(errors, field) do
    Enum.find(errors, fn err -> err.field == field end)
  end

  describe "validate/1" do
    test "valid state passes validation" do
      assert :ok = Validator.validate(valid_state())
    end

    test "valid state with empty optional configs passes" do
      state = %{valid_state() | drives: [], gc: %{}, worker: %{}, ra_cluster_members: []}
      assert :ok = Validator.validate(state)
    end

    test "valid state with multiple drives passes" do
      state = %{
        valid_state()
        | drives: [
            %{"id" => "d1", "path" => "/data/hot", "tier" => "hot", "capacity" => "500"},
            %{"id" => "d2", "path" => "/data/warm", "tier" => "warm", "capacity" => "2000"},
            %{"id" => "d3", "path" => "/data/cold", "tier" => "cold", "capacity" => "10000"}
          ]
      }

      assert :ok = Validator.validate(state)
    end

    test "valid state with human-readable capacity formats passes" do
      state = %{
        valid_state()
        | drives: [
            %{"id" => "d1", "path" => "/data/hot", "tier" => "hot", "capacity" => "1T"},
            %{"id" => "d2", "path" => "/data/warm", "tier" => "warm", "capacity" => "500G"},
            %{"id" => "d3", "path" => "/data/cold", "tier" => "cold", "capacity" => "4T"}
          ]
      }

      assert :ok = Validator.validate(state)
    end
  end

  describe "cluster_id validation" do
    test "empty string returns error" do
      state = %{valid_state() | cluster_id: ""}
      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :cluster_id)
      assert error.message =~ "empty"
    end

    test "nil cluster_id returns error" do
      state = %{valid_state() | cluster_id: nil}
      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :cluster_id)
    end

    test "non-string cluster_id returns error" do
      state = %{valid_state() | cluster_id: 12_345}
      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :cluster_id)
    end
  end

  describe "cluster_name validation" do
    test "empty string returns error" do
      state = %{valid_state() | cluster_name: ""}
      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :cluster_name)
      assert error.message =~ "empty"
    end

    test "nil cluster_name returns error" do
      state = %{valid_state() | cluster_name: nil}
      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :cluster_name)
    end
  end

  describe "created_at validation" do
    test "non-DateTime value returns error" do
      state = %{valid_state() | created_at: "2026-01-01"}
      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :created_at)
      assert error.message =~ "datetime"
    end
  end

  describe "this_node validation" do
    test "missing required fields returns error" do
      state = %{valid_state() | this_node: %{id: "node-1"}}
      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :this_node)
      assert error.message =~ "missing required fields"
      assert error.message =~ "name"
      assert error.message =~ "joined_at"
    end

    test "node name with invalid characters returns error" do
      state = %{
        valid_state()
        | this_node: %{id: "n1", name: :"bad node!", joined_at: ~U[2026-01-01 00:00:00Z]}
      }

      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :"this_node.name")
      assert error.message =~ "invalid characters"
    end

    test "valid node name with @ passes" do
      state = %{
        valid_state()
        | this_node: %{
            id: "n1",
            name: :"core@host.local",
            joined_at: ~U[2026-01-01 00:00:00Z]
          }
      }

      assert :ok = Validator.validate(state)
    end
  end

  describe "drives validation" do
    test "drive with empty path returns error" do
      state = %{
        valid_state()
        | drives: [%{"id" => "d1", "path" => "", "tier" => "hot", "capacity" => "1000"}]
      }

      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :"drives[0].path")
      assert error.message =~ "empty"
    end

    test "drive with invalid tier returns error" do
      state = %{
        valid_state()
        | drives: [
            %{"id" => "d1", "path" => "/data", "tier" => "blazing", "capacity" => "1000"}
          ]
      }

      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :"drives[0].tier")
      assert error.message =~ "hot, warm, cold"
    end

    test "drive with negative capacity returns error" do
      state = %{
        valid_state()
        | drives: [%{"id" => "d1", "path" => "/data", "tier" => "hot", "capacity" => "-5"}]
      }

      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :"drives[0].capacity")
    end

    test "drive with zero capacity is valid (default/auto-detect)" do
      state = %{
        valid_state()
        | drives: [%{"id" => "d1", "path" => "/data", "tier" => "hot", "capacity" => "0"}]
      }

      assert :ok = Validator.validate(state)
    end

    test "drive with unparseable capacity returns error" do
      state = %{
        valid_state()
        | drives: [%{"id" => "d1", "path" => "/data", "tier" => "hot", "capacity" => "lots"}]
      }

      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :"drives[0].capacity")
    end

    test "errors reference correct drive index" do
      drives = [
        %{"id" => "d0", "path" => "/data/ok", "tier" => "hot", "capacity" => "1000"},
        %{"id" => "d1", "path" => "", "tier" => "hot", "capacity" => "1000"}
      ]

      state = %{valid_state() | drives: drives}
      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :"drives[1].path")
      refute find_error(errors, :"drives[0].path")
    end
  end

  describe "gc config validation" do
    test "negative interval_ms returns error" do
      state = %{valid_state() | gc: %{"interval_ms" => -1}}
      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :"gc.interval_ms")
      assert error.message =~ "positive integer"
    end

    test "zero interval_ms returns error" do
      state = %{valid_state() | gc: %{"interval_ms" => 0}}
      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :"gc.interval_ms")
    end

    test "pressure_threshold above 1.0 returns error" do
      state = %{valid_state() | gc: %{"pressure_threshold" => 1.5}}
      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :"gc.pressure_threshold")
      assert error.message =~ "0.0 and 1.0"
    end

    test "pressure_threshold below 0.0 returns error" do
      state = %{valid_state() | gc: %{"pressure_threshold" => -0.1}}
      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :"gc.pressure_threshold")
    end

    test "valid pressure_threshold boundary values pass" do
      state = %{valid_state() | gc: %{"pressure_threshold" => 0.0}}
      assert :ok = Validator.validate(state)

      state = %{valid_state() | gc: %{"pressure_threshold" => 1.0}}
      assert :ok = Validator.validate(state)
    end
  end

  describe "worker config validation" do
    test "negative max_concurrent returns error" do
      state = %{valid_state() | worker: %{"max_concurrent" => -1}}
      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :"worker.max_concurrent")
      assert error.message =~ "positive integer"
    end

    test "zero max_per_minute returns error" do
      state = %{valid_state() | worker: %{"max_per_minute" => 0}}
      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :"worker.max_per_minute")
    end

    test "non-integer drive_concurrency returns error" do
      state = %{valid_state() | worker: %{"drive_concurrency" => "fast"}}
      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :"worker.drive_concurrency")
    end
  end

  describe "metrics config validation" do
    test "non-boolean enabled returns error" do
      state = %{valid_state() | metrics: %{"enabled" => "yes"}}
      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :"metrics.enabled")
    end

    test "non-integer port returns error" do
      state = %{valid_state() | metrics: %{"port" => "9568"}}
      assert {:error, errors} = Validator.validate(state)
      assert find_error(errors, :"metrics.port")
    end

    test "valid metrics config passes" do
      state = %{
        valid_state()
        | metrics: %{"enabled" => true, "bind" => "127.0.0.1", "port" => 9568}
      }

      assert :ok = Validator.validate(state)
    end
  end

  describe "ra_cluster_members validation" do
    test "atom without @ returns error" do
      state = %{valid_state() | ra_cluster_members: [:not_a_node_name]}
      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :"ra_cluster_members[0]")
      assert error.message =~ "name@host"
    end

    test "non-atom member returns error" do
      state = %{valid_state() | ra_cluster_members: ["neonfs@host"]}
      assert {:error, errors} = Validator.validate(state)
      error = find_error(errors, :"ra_cluster_members[0]")
      assert error.message =~ "atom"
    end
  end

  describe "multiple error collection" do
    test "collects errors from multiple fields" do
      state = %{
        valid_state()
        | cluster_id: "",
          cluster_name: "",
          drives: [
            %{"id" => "d1", "path" => "", "tier" => "unknown", "capacity" => "-1"}
          ]
      }

      assert {:error, errors} = Validator.validate(state)
      assert length(errors) >= 4

      fields = Enum.map(errors, & &1.field)
      assert :cluster_id in fields
      assert :cluster_name in fields
      assert :"drives[0].path" in fields
      assert :"drives[0].tier" in fields
    end

    test "all errors have field and message keys" do
      state = %{valid_state() | cluster_id: "", cluster_name: ""}
      assert {:error, errors} = Validator.validate(state)

      for error <- errors do
        assert Map.has_key?(error, :field)
        assert Map.has_key?(error, :message)
        assert is_atom(error.field)
        assert is_binary(error.message)
      end
    end
  end
end
