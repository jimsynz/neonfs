defmodule NeonFS.Cluster.BootstrapProviderTest do
  @moduledoc """
  The provider stands between a ConfigMap and a node that boots into a cluster,
  so its failure modes matter as much as its success one: a misconfigured file
  must stop the boot rather than produce a node that starts, forms nothing, and
  passes its health check.
  """
  use ExUnit.Case, async: true

  alias NeonFS.Cluster.BootstrapProvider

  @moduletag :tmp_dir

  describe "load/2 with no file" do
    test "leaves the configuration untouched", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "absent.json")
      existing = [neonfs_core: [cluster_name: "from-env"]]

      assert BootstrapProvider.load(existing, path) == existing
    end
  end

  describe "load/2 with a valid file" do
    test "configures formation from the file", %{tmp_dir: tmp_dir} do
      path =
        write(tmp_dir, %{
          "cluster_name" => "prod",
          "bootstrap_expect" => 3,
          "bootstrap_timeout_ms" => 60_000,
          "peers" => [
            %{"node" => "neonfs@10.0.0.1", "dist_port" => 9100},
            %{"node" => "neonfs@10.0.0.2", "dist_port" => 9101}
          ]
        })

      core = BootstrapProvider.load([], path)[:neonfs_core]

      assert core[:auto_bootstrap] == true
      assert core[:cluster_name] == "prod"
      assert core[:bootstrap_expect] == 3
      assert core[:bootstrap_timeout] == 60_000
      assert core[:bootstrap_peers] == [:"neonfs@10.0.0.1", :"neonfs@10.0.0.2"]

      # The per-peer port is the thing `NEONFS_BOOTSTRAP_PEERS` could not carry,
      # and `NeonFS.Epmd` cannot resolve a peer without it.
      assert core[:bootstrap_peer_ports] == %{
               :"neonfs@10.0.0.1" => 9100,
               :"neonfs@10.0.0.2" => 9101
             }
    end

    test "overrides configuration the environment already set", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, %{"cluster_name" => "from-file"})

      existing = [neonfs_core: [cluster_name: "from-env", meta_dir: "/var/lib/neonfs/meta"]]
      core = BootstrapProvider.load(existing, path)[:neonfs_core]

      assert core[:cluster_name] == "from-file"
      # Merged, not replaced: unrelated keys survive.
      assert core[:meta_dir] == "/var/lib/neonfs/meta"
    end

    test "a file with no peers configures a single-node bootstrap", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, %{"cluster_name" => "solo"})
      core = BootstrapProvider.load([], path)[:neonfs_core]

      assert core[:bootstrap_peers] == []
      assert core[:bootstrap_expect] == 1
      assert core[:bootstrap_timeout] == 300_000
    end

    test "expect defaults to the number of peers", %{tmp_dir: tmp_dir} do
      path =
        write(tmp_dir, %{
          "cluster_name" => "prod",
          "peers" => [
            %{"node" => "neonfs@a", "dist_port" => 9100},
            %{"node" => "neonfs@b", "dist_port" => 9100}
          ]
        })

      assert BootstrapProvider.load([], path)[:neonfs_core][:bootstrap_expect] == 2
    end

    test "carries the drive list, capacity left as written", %{tmp_dir: tmp_dir} do
      path =
        write(tmp_dir, %{
          "cluster_name" => "prod",
          "drives" => [
            %{"id" => "drive1", "path" => "/mnt/d1", "tier" => "hot", "capacity" => "1T"},
            %{"id" => "drive2", "path" => "/mnt/d2"}
          ]
        })

      assert [drive1, drive2] = BootstrapProvider.load([], path)[:neonfs_core][:drives]
      assert drive1 == %{id: "drive1", path: "/mnt/d1", tier: "hot", capacity: "1T"}

      # Defaults rather than a refusal: a drive with no tier or declared
      # capacity is ordinary, and the registry detects capacity itself.
      assert drive2 == %{id: "drive2", path: "/mnt/d2", tier: "hot", capacity: "0"}
    end

    test "leaves drives alone when the file names none", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, %{"cluster_name" => "prod"})

      refute Keyword.has_key?(BootstrapProvider.load([], path)[:neonfs_core], :drives)
    end

    test "does not consume the file", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, %{"cluster_name" => "prod"})
      BootstrapProvider.load([], path)

      # A ConfigMap mount is read-only, and a re-bootstrap needs the file again.
      assert File.exists?(path)
    end
  end

  describe "load/2 refuses a file it cannot honour" do
    test "malformed JSON names the file", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "bootstrap.json")
      File.write!(path, "{not json")

      assert_raise ArgumentError, ~r/#{Regex.escape(path)}.*not valid JSON/s, fn ->
        BootstrapProvider.load([], path)
      end
    end

    test "a JSON array is not a bootstrap file", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "bootstrap.json")
      File.write!(path, "[]")

      assert_raise ArgumentError, ~r/must contain a JSON object/, fn ->
        BootstrapProvider.load([], path)
      end
    end

    test "a missing cluster name is refused", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, %{"bootstrap_expect" => 3})

      assert_raise ArgumentError, ~r/missing the required key "cluster_name"/, fn ->
        BootstrapProvider.load([], path)
      end
    end

    # The port is the reason the file exists. Defaulting it would reintroduce
    # the failure the env-var format has: an unresolvable peer, and a formation
    # timeout with nothing pointing back at the configuration.
    test "a peer without a distribution port is refused", %{tmp_dir: tmp_dir} do
      path =
        write(tmp_dir, %{"cluster_name" => "prod", "peers" => [%{"node" => "neonfs@10.0.0.1"}]})

      assert_raise ArgumentError, ~r/dist_port/, fn -> BootstrapProvider.load([], path) end
    end

    test "a nonsensical expect is refused rather than clamped", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, %{"cluster_name" => "prod", "bootstrap_expect" => 0})

      assert_raise ArgumentError, ~r/invalid "bootstrap_expect"/, fn ->
        BootstrapProvider.load([], path)
      end
    end

    test "a drive without a path is refused", %{tmp_dir: tmp_dir} do
      path = write(tmp_dir, %{"cluster_name" => "prod", "drives" => [%{"id" => "drive1"}]})

      assert_raise ArgumentError, ~r/without a string "id" and "path"/, fn ->
        BootstrapProvider.load([], path)
      end
    end
  end

  defp write(tmp_dir, document) do
    path = Path.join(tmp_dir, "bootstrap.json")
    File.write!(path, document |> :json.encode() |> IO.iodata_to_binary())
    path
  end
end
