defmodule NeonFS.EpmdTest do
  # Mutates process-wide environment variables — cannot run async.
  use ExUnit.Case, async: false

  @env_vars ["NEONFS_META_DIR", "NEONFS_DATA_DIR", "NEONFS_PEER_PORTS"]

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    saved = Map.new(@env_vars, fn var -> {var, System.get_env(var)} end)
    Enum.each(@env_vars, &System.delete_env/1)

    on_exit(fn ->
      Enum.each(saved, fn
        {var, nil} -> System.delete_env(var)
        {var, value} -> System.put_env(var, value)
      end)
    end)

    meta_dir = Path.join(tmp_dir, "meta")
    File.mkdir_p!(meta_dir)

    cluster_json = """
    {
      "this_node": {"name": "neonfs@localhost", "dist_port": 9001},
      "known_peers": [{"name": "peer1@localhost", "dist_port": 9123}]
    }
    """

    File.write!(Path.join(meta_dir, "cluster.json"), cluster_json)

    {:ok, meta_dir: meta_dir}
  end

  describe "address_please/3 cluster.json fallback" do
    test "resolves a peer port from $NEONFS_DATA_DIR/meta/cluster.json without NEONFS_PEER_PORTS",
         %{tmp_dir: tmp_dir} do
      System.put_env("NEONFS_DATA_DIR", tmp_dir)

      assert {:ok, _ip, 9123, 5} = NeonFS.Epmd.address_please(~c"peer1", ~c"localhost", :inet)
    end

    test "resolves this_node's own port from cluster.json", %{tmp_dir: tmp_dir} do
      System.put_env("NEONFS_DATA_DIR", tmp_dir)

      assert {:ok, _ip, 9001, 5} = NeonFS.Epmd.address_please(~c"neonfs", ~c"localhost", :inet)
    end

    test "NEONFS_META_DIR takes priority over $NEONFS_DATA_DIR/meta", %{
      tmp_dir: tmp_dir,
      meta_dir: meta_dir
    } do
      System.put_env("NEONFS_DATA_DIR", Path.join(tmp_dir, "nonexistent"))
      System.put_env("NEONFS_META_DIR", meta_dir)

      assert {:ok, _ip, 9123, 5} = NeonFS.Epmd.address_please(~c"peer1", ~c"localhost", :inet)
    end

    test "returns nxdomain when no cluster.json exists at the resolved path", %{tmp_dir: tmp_dir} do
      System.put_env("NEONFS_DATA_DIR", Path.join(tmp_dir, "nonexistent"))

      assert {:error, :nxdomain} = NeonFS.Epmd.address_please(~c"peer1", ~c"localhost", :inet)
    end

    test "NEONFS_PEER_PORTS takes priority over cluster.json", %{tmp_dir: tmp_dir} do
      System.put_env("NEONFS_DATA_DIR", tmp_dir)
      System.put_env("NEONFS_PEER_PORTS", "peer1@localhost:9555")

      assert {:ok, _ip, 9555, 5} = NeonFS.Epmd.address_please(~c"peer1", ~c"localhost", :inet)
    end

    # This is what an interface node dialling a sibling depends on:
    # `NeonFS.Client.Discovery` publishes the sibling's registered port into
    # `NEONFS_PEER_PORTS`, and nothing else in the system knows it — the
    # sibling is not in `known_peers`, which carries cluster peers.
    test "resolves a node that appears in neither cluster.json section", %{tmp_dir: tmp_dir} do
      System.put_env("NEONFS_DATA_DIR", tmp_dir)
      System.put_env("NEONFS_PEER_PORTS", "neonfs_csi_node@localhost:9102")

      assert {:ok, _ip, 9102, 5} =
               NeonFS.Epmd.address_please(~c"neonfs_csi_node", ~c"localhost", :inet)
    end
  end

  # `erl_epmd:names/1` answers for one host: `{Name, Port}` pairs whose names
  # are short. Discarding the host, as this used to, answered for peers on
  # every other machine as well and collided two nodes sharing a name part.
  describe "names/1" do
    test "answers only for the host asked about", %{tmp_dir: tmp_dir} do
      System.put_env("NEONFS_DATA_DIR", tmp_dir)
      System.put_env("NEONFS_PEER_PORTS", "a@h1:9001,b@h2:9002,c@h1:9003")

      assert {:ok, entries} = NeonFS.Epmd.names(~c"h1")

      assert Enum.sort(entries) == [{~c"a", 9001}, {~c"c", 9003}]
    end

    test "keeps two nodes that share a name part on different hosts apart", %{tmp_dir: tmp_dir} do
      System.put_env("NEONFS_DATA_DIR", tmp_dir)
      System.put_env("NEONFS_PEER_PORTS", "neonfs@h1:9001,neonfs@h2:9002")

      assert {:ok, [{~c"neonfs", 9001}]} = NeonFS.Epmd.names(~c"h1")
      assert {:ok, [{~c"neonfs", 9002}]} = NeonFS.Epmd.names(~c"h2")
    end

    test "includes cluster.json entries for that host", %{tmp_dir: tmp_dir} do
      System.put_env("NEONFS_DATA_DIR", tmp_dir)

      assert {:ok, entries} = NeonFS.Epmd.names(~c"localhost")

      assert Enum.sort(entries) == [{~c"neonfs", 9001}, {~c"peer1", 9123}]
    end

    test "answers with nothing for a host it knows no nodes on", %{tmp_dir: tmp_dir} do
      System.put_env("NEONFS_DATA_DIR", tmp_dir)

      assert {:ok, []} = NeonFS.Epmd.names(~c"elsewhere")
    end
  end
end
