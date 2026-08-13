defmodule NeonFS.CSI.NodeResolverTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.ServiceInfo
  alias NeonFS.CSI.{AttachHolder, NodeResolver}

  setup do
    on_exit(fn -> Application.delete_env(:neonfs_csi, :service_list_fn) end)
    :ok
  end

  defp stub_services(services) do
    Application.put_env(:neonfs_csi, :service_list_fn, fn :csi -> services end)
  end

  defp service(node, metadata) do
    %ServiceInfo{node: node, type: :csi, metadata: metadata}
  end

  describe "beam_node/1" do
    test "resolves the node whose plugin reports the id" do
      stub_services([
        service(:csi_node_a@host, %{mode: :node, node_id: "worker-a"}),
        service(:csi_node_b@host, %{mode: :node, node_id: "worker-b"})
      ])

      assert {:ok, :csi_node_b@host} = NodeResolver.beam_node("worker-b")
    end

    # A controller registers no node_id, and claiming against it would tie
    # the attachment's lifetime to the wrong process entirely.
    test "ignores controller-mode plugins" do
      stub_services([
        service(:csi_controller@host, %{mode: :controller, node_id: "worker-a"}),
        service(:csi_node_a@host, %{mode: :node, node_id: "worker-a"})
      ])

      assert {:ok, :csi_node_a@host} = NodeResolver.beam_node("worker-a")
    end

    # Guessing here would put the claim on a node that is not the one being
    # attached to, which is worse than refusing the attach.
    test "refuses an id no plugin reports" do
      stub_services([service(:csi_node_a@host, %{mode: :node, node_id: "worker-a"})])

      assert {:error, {:unknown_node_id, "gone"}} = NodeResolver.beam_node("gone")
    end

    test "refuses an empty id" do
      assert {:error, :node_id_required} = NodeResolver.beam_node("")
    end

    test "refuses when nothing has registered" do
      stub_services([])

      assert {:error, {:unknown_node_id, "worker-a"}} = NodeResolver.beam_node("worker-a")
    end
  end

  describe "attach_holder/1" do
    test "answers the holder pid running on the resolved node" do
      start_supervised!(AttachHolder)
      stub_services([service(node(), %{mode: :node, node_id: "this-one"})])

      assert {:ok, pid} = NodeResolver.attach_holder("this-one")
      assert pid == Process.whereis(AttachHolder)
    end

    # A node whose plugin is running but not in node mode has nothing to
    # monitor, so a claim taken against it would never be released.
    test "reports a node with no holder rather than claiming against it" do
      stub_services([service(node(), %{mode: :node, node_id: "holderless"})])

      assert {:error, {:no_attach_holder, _node}} = NodeResolver.attach_holder("holderless")
    end

    test "passes through an unresolvable node_id" do
      stub_services([])

      assert {:error, {:unknown_node_id, "nope"}} = NodeResolver.attach_holder("nope")
    end
  end
end
