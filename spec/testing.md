# Testing

This document describes the testing strategy for NeonFS, covering unit testing, property testing, fuzzing, linting, and integration testing with containerised clusters.

## Testing Philosophy

NeonFS prioritises **correctness over performance**. The testing strategy reflects this:

1. **Defence in depth**: Multiple testing techniques catch different classes of bugs
2. **Realistic failure testing**: Container-based clusters allow testing actual failure modes
3. **Property-based testing**: Verify invariants hold across input space, not just happy paths
4. **Isolation**: Rust and Elixir components tested independently, with integration tests verifying boundaries

## Testing Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                    Integration Tests                             │
│            (Containerised multi-node clusters)                   │
├─────────────────────────────────────────────────────────────────┤
│     NIF Boundary Tests          │      API Surface Tests         │
│   (Elixir calling Rust NIFs)    │   (S3, FUSE, CLI behaviour)    │
├─────────────────────────────────┴────────────────────────────────┤
│   Elixir Unit/Property Tests    │    Rust Unit/Property Tests    │
│   (ExUnit, StreamData)          │    (cargo test, proptest)      │
├─────────────────────────────────┴────────────────────────────────┤
│              Static Analysis (Dialyzer, Clippy, Credo)           │
└─────────────────────────────────────────────────────────────────┘
```

---

## Static Analysis and Linting

Static analysis runs on every commit and must pass before merge.

### Elixir

| Tool | Purpose | Configuration |
|------|---------|---------------|
| Dialyzer | Type checking via typespecs | `mix dialyzer` with PLT caching |
| Credo | Code style and complexity | `.credo.exs` with strict mode |
| mix format | Code formatting | `mix format --check-formatted` |

**Dialyzer configuration:**

Maintain typespecs on all public functions. Dialyzer catches:
- Type mismatches in function calls
- Unreachable code paths
- Pattern match failures that can't succeed

**Credo rules of note:**

```elixir
# .credo.exs
%{
  configs: [
    %{
      name: "default",
      strict: true,
      checks: [
        {Credo.Check.Refactor.CyclomaticComplexity, max_complexity: 10},
        {Credo.Check.Refactor.Nesting, max_nesting: 3},
        {Credo.Check.Refactor.FunctionArity, max_arity: 5}
      ]
    }
  ]
}
```

### Rust

| Tool | Purpose | Configuration |
|------|---------|---------------|
| Clippy | Linting and common mistakes | `cargo clippy --all-targets -- -D warnings` |
| rustfmt | Code formatting | `cargo fmt --check` |
| cargo deny | Dependency auditing | `deny.toml` for license and security |

**Clippy configuration:**

```toml
# clippy.toml
cognitive-complexity-threshold = 15
too-many-arguments-threshold = 6
```

Run with all targets to catch issues in tests and benchmarks:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### CI Integration

```yaml
# Example CI step
lint:
  steps:
    # Elixir
    - mix format --check-formatted
    - mix credo --strict
    - mix dialyzer

    # Rust (each crate)
    - cargo fmt --check
    - cargo clippy --all-targets -- -D warnings
    - cargo deny check
```

---

## Unit Testing

### Elixir Unit Tests

Use ExUnit for testing individual modules. Focus on:

- Pure functions with clear inputs/outputs
- GenServer callback behaviour
- Protocol implementations
- Error handling paths

**Test organisation:**

```
test/
├── neonfs/
│   ├── volume/
│   │   ├── metadata_server_test.exs
│   │   ├── policy_engine_test.exs
│   │   └── tiering_manager_test.exs
│   ├── metadata/
│   │   ├── quorum_test.exs
│   │   ├── hlc_test.exs
│   │   └── consistent_hash_test.exs
│   ├── cluster/
│   │   ├── node_state_test.exs
│   │   └── membership_test.exs
│   └── cli/
│       └── handler_test.exs
└── test_helper.exs
```

**Example: HLC unit test**

```elixir
defmodule NeonFS.Metadata.HLCTest do
  use ExUnit.Case, async: true

  alias NeonFS.Metadata.HLC

  describe "compare/2" do
    test "later wall time wins" do
      hlc1 = HLC.new(1000, 0, :node1)
      hlc2 = HLC.new(2000, 0, :node2)

      assert HLC.compare(hlc1, hlc2) == :lt
      assert HLC.compare(hlc2, hlc1) == :gt
    end

    test "same wall time, higher counter wins" do
      hlc1 = HLC.new(1000, 5, :node1)
      hlc2 = HLC.new(1000, 10, :node2)

      assert HLC.compare(hlc1, hlc2) == :lt
    end

    test "same wall time and counter, node_id breaks tie deterministically" do
      hlc1 = HLC.new(1000, 5, :node1)
      hlc2 = HLC.new(1000, 5, :node2)

      # Consistent ordering regardless of comparison direction
      assert HLC.compare(hlc1, hlc2) == HLC.compare(hlc1, hlc2)
    end
  end
end
```

### Rust Unit Tests

Each Rust crate has its own test suite run via `cargo test`.

**neonfs_blob crate tests:**

```
neonfs_blob/
└── src/
    ├── chunk/
    │   ├── mod.rs
    │   └── tests.rs        # Chunking algorithm tests
    ├── store/
    │   ├── mod.rs
    │   └── tests.rs        # Blob store operations
    ├── compression/
    │   └── tests.rs        # Compression roundtrips
    └── crypto/
        └── tests.rs        # Encryption roundtrips
```

**Example: Chunk store unit test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn write_then_read_returns_same_data() {
        let temp = TempDir::new().unwrap();
        let store = BlobStore::open(temp.path(), Default::default()).unwrap();

        let data = b"hello world";
        let hash = store.write_chunk(data, Tier::Hot, WriteOpts::default()).unwrap();

        let retrieved = store.read_chunk(&hash, ReadOpts::default()).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn read_nonexistent_chunk_returns_not_found() {
        let temp = TempDir::new().unwrap();
        let store = BlobStore::open(temp.path(), Default::default()).unwrap();

        let fake_hash = Hash::from_bytes(&[0u8; 32]);
        let result = store.read_chunk(&fake_hash, ReadOpts::default());

        assert!(matches!(result, Err(Error::ChunkNotFound(_))));
    }

    #[test]
    fn corrupt_chunk_detected_on_verified_read() {
        let temp = TempDir::new().unwrap();
        let store = BlobStore::open(temp.path(), Default::default()).unwrap();

        let data = b"original data";
        let hash = store.write_chunk(data, Tier::Hot, WriteOpts::default()).unwrap();

        // Corrupt the file directly
        let path = store.chunk_path(&hash);
        std::fs::write(&path, b"corrupted").unwrap();

        let result = store.read_chunk(&hash, ReadOpts { verify: true, ..Default::default() });
        assert!(matches!(result, Err(Error::CorruptChunk { .. })));
    }
}
```

**neonfs_fuse crate tests:**

Focus on request/response marshalling and state management, not actual FUSE kernel interaction (that requires integration tests).

**neonfs-cli crate tests:**

Test term conversion and output formatting:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_info_from_term_parses_correctly() {
        let term = // ... construct test term
        let info: VolumeInfo = VolumeInfo::from_term(term).unwrap();

        assert_eq!(info.name, "documents");
        assert_eq!(info.durability, DurabilityConfig::Replicate { factor: 3 });
    }

    #[test]
    fn error_response_provides_helpful_message() {
        let term = // {:error, :volume_not_found}
        let err = CliError::from_term(term).unwrap();

        assert!(err.user_message().contains("not found"));
    }
}
```

---

## Property Testing

Property testing verifies that invariants hold across a wide range of inputs, catching edge cases that example-based tests miss.

### Elixir Property Tests (StreamData)

```elixir
defmodule NeonFS.Metadata.HLCPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Metadata.HLC

  property "HLC ordering is transitive" do
    check all hlc1 <- hlc_generator(),
              hlc2 <- hlc_generator(),
              hlc3 <- hlc_generator() do
      if HLC.compare(hlc1, hlc2) == :lt and HLC.compare(hlc2, hlc3) == :lt do
        assert HLC.compare(hlc1, hlc3) == :lt
      end
    end
  end

  property "HLC ordering is antisymmetric" do
    check all hlc1 <- hlc_generator(),
              hlc2 <- hlc_generator() do
      cmp1 = HLC.compare(hlc1, hlc2)
      cmp2 = HLC.compare(hlc2, hlc1)

      case cmp1 do
        :lt -> assert cmp2 == :gt
        :gt -> assert cmp2 == :lt
        :eq -> assert cmp2 == :eq
      end
    end
  end

  property "tick always advances the clock" do
    check all hlc <- hlc_generator(),
              wall_time <- positive_integer() do
      ticked = HLC.tick(hlc, wall_time)
      assert HLC.compare(hlc, ticked) == :lt
    end
  end

  defp hlc_generator do
    gen all wall_time <- positive_integer(),
            counter <- integer(0..1000),
            node_id <- atom(:alphanumeric) do
      HLC.new(wall_time, counter, node_id)
    end
  end
end
```

**Key properties to verify:**

| Component | Property |
|-----------|----------|
| HLC | Transitivity, antisymmetry, tick advancement |
| Quorum | R + W > N guarantees overlap |
| Consistent hash | Minimal disruption on node add/remove |
| Chunk metadata | Location tracking consistency |
| Volume policy | Valid configurations only |

### Rust Property Tests (proptest)

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn chunking_then_reassembly_returns_original(data: Vec<u8>) {
        let chunks = chunk_data(&data, ChunkStrategy::default());
        let reassembled: Vec<u8> = chunks.iter()
            .flat_map(|c| c.data.iter().copied())
            .collect();

        prop_assert_eq!(data, reassembled);
    }

    #[test]
    fn compression_roundtrip_preserves_data(data: Vec<u8>) {
        let compressed = compress(&data, Compression::Zstd { level: 3 });
        let decompressed = decompress(&compressed, Compression::Zstd { level: 3 }).unwrap();

        prop_assert_eq!(data, decompressed);
    }

    #[test]
    fn encryption_roundtrip_preserves_data(
        data: Vec<u8>,
        key in prop::array::uniform32(any::<u8>())
    ) {
        let encrypted = encrypt(&data, &key).unwrap();
        let decrypted = decrypt(&encrypted, &key).unwrap();

        prop_assert_eq!(data, decrypted);
    }

    #[test]
    fn hash_is_deterministic(data: Vec<u8>) {
        let hash1 = sha256(&data);
        let hash2 = sha256(&data);

        prop_assert_eq!(hash1, hash2);
    }

    #[test]
    fn different_data_produces_different_hash(
        data1: Vec<u8>,
        data2: Vec<u8>
    ) {
        prop_assume!(data1 != data2);

        let hash1 = sha256(&data1);
        let hash2 = sha256(&data2);

        prop_assert_ne!(hash1, hash2);
    }
}
```

**Key properties to verify:**

| Component | Property |
|-----------|----------|
| Chunking | Reassembly returns original data |
| Compression | Roundtrip preserves data |
| Encryption | Roundtrip preserves data |
| Hashing | Deterministic, collision-resistant |
| Erasure coding | Can reconstruct from any K of N chunks |
| Path sharding | Even distribution across directories |

---

## Fuzzing

Fuzzing discovers crashes, panics, and undefined behaviour by generating random/malformed inputs.

### Rust Fuzzing (cargo-fuzz)

Focus fuzzing on components that parse external input:

```
neonfs_blob/
└── fuzz/
    ├── Cargo.toml
    └── fuzz_targets/
        ├── chunk_parsing.rs      # Malformed chunk data
        └── compression.rs        # Malformed compressed data

neonfs-cli/
└── fuzz/
    ├── Cargo.toml
    └── fuzz_targets/
        └── term_parsing.rs       # Malformed Erlang terms
```

**Example: Chunk parsing fuzzer**

```rust
// neonfs_blob/fuzz/fuzz_targets/chunk_parsing.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use neonfs_blob::chunk::parse_chunk_header;

fuzz_target!(|data: &[u8]| {
    // Should not panic regardless of input
    let _ = parse_chunk_header(data);
});
```

**Example: Compression fuzzer**

```rust
// neonfs_blob/fuzz/fuzz_targets/compression.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use neonfs_blob::compression::{decompress, Compression};

fuzz_target!(|data: &[u8]| {
    // Decompressing garbage should return an error, not panic
    let _ = decompress(data, Compression::Zstd { level: 3 });
});
```

**Running fuzzers:**

```bash
# Install cargo-fuzz
cargo install cargo-fuzz

# Run a fuzzer (runs indefinitely until stopped or crash found)
cd neonfs_blob
cargo +nightly fuzz run chunk_parsing

# Run with timeout for CI
cargo +nightly fuzz run chunk_parsing -- -max_total_time=300
```

**CI integration:**

Run fuzzers for a limited time (e.g., 5 minutes) on each PR to catch obvious issues. Longer fuzzing runs should happen periodically (nightly builds).

---

## NIF Boundary Testing

The Rustler NIF boundary is tested from the Elixir side, verifying that:

1. Data crosses the boundary correctly
2. Errors from Rust are properly translated
3. Resources are managed correctly (no leaks)
4. Concurrent NIF calls behave correctly

```elixir
defmodule NeonFS.Blob.NativeTest do
  use ExUnit.Case

  alias NeonFS.Blob.Native

  setup do
    # Create temp directory for each test
    temp_dir = System.tmp_dir!() |> Path.join("neonfs_test_#{:rand.uniform(100000)}")
    File.mkdir_p!(temp_dir)

    {:ok, store} = Native.open(temp_dir, %{})

    on_exit(fn -> File.rm_rf!(temp_dir) end)

    %{store: store, temp_dir: temp_dir}
  end

  describe "write_chunk/4" do
    test "returns hash of written data", %{store: store} do
      data = "hello world"
      {:ok, hash} = Native.write_chunk(store, data, :hot, %{})

      # Hash should be SHA-256 of original data
      expected = :crypto.hash(:sha256, data)
      assert hash == expected
    end

    test "handles binary data with null bytes", %{store: store} do
      data = <<0, 1, 2, 0, 3, 4, 0>>
      {:ok, hash} = Native.write_chunk(store, data, :hot, %{})
      {:ok, retrieved} = Native.read_chunk(store, hash, %{})

      assert retrieved == data
    end

    test "handles large data", %{store: store} do
      # 10 MB of random data
      data = :crypto.strong_rand_bytes(10 * 1024 * 1024)
      {:ok, hash} = Native.write_chunk(store, data, :hot, %{})
      {:ok, retrieved} = Native.read_chunk(store, hash, %{})

      assert retrieved == data
    end
  end

  describe "concurrent access" do
    test "multiple concurrent writes succeed", %{store: store} do
      tasks = for i <- 1..100 do
        Task.async(fn ->
          data = "data #{i}"
          {:ok, _hash} = Native.write_chunk(store, data, :hot, %{})
        end)
      end

      results = Task.await_many(tasks, 30_000)
      assert Enum.all?(results, &match?({:ok, _}, &1))
    end

    test "concurrent read and write", %{store: store} do
      # Pre-populate some data
      data = "test data"
      {:ok, hash} = Native.write_chunk(store, data, :hot, %{})

      # Concurrent reads while writing new data
      read_tasks = for _ <- 1..50 do
        Task.async(fn -> Native.read_chunk(store, hash, %{}) end)
      end

      write_tasks = for i <- 1..50 do
        Task.async(fn -> Native.write_chunk(store, "new data #{i}", :hot, %{}) end)
      end

      read_results = Task.await_many(read_tasks, 30_000)
      write_results = Task.await_many(write_tasks, 30_000)

      assert Enum.all?(read_results, &(&1 == {:ok, data}))
      assert Enum.all?(write_results, &match?({:ok, _}, &1))
    end
  end

  describe "error handling" do
    test "read of nonexistent chunk returns error", %{store: store} do
      fake_hash = :crypto.strong_rand_bytes(32)
      result = Native.read_chunk(store, fake_hash, %{})

      assert {:error, :not_found} = result
    end

    test "invalid tier returns error", %{store: store} do
      result = Native.write_chunk(store, "data", :invalid_tier, %{})

      assert {:error, _reason} = result
    end
  end
end
```

---

## Integration Testing with Containerised Clusters

Integration tests run against real multi-node NeonFS clusters in containers. This enables testing:

- Cluster formation and node membership
- Distributed metadata operations
- Replication and consistency
- Failure modes and recovery

### Container Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Test Host (CI runner or dev machine)          │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                   Test Controller                           │ │
│  │  (Elixir process managing cluster lifecycle)                │ │
│  └─────────────────────┬──────────────────────────────────────┘ │
│                        │ Container API (Docker/Podman)          │
│  ┌─────────────────────┼──────────────────────────────────────┐ │
│  │                     ▼                                       │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐        │ │
│  │  │ node1   │  │ node2   │  │ node3   │  │ node4   │        │ │
│  │  │ (BEAM)  │──│ (BEAM)  │──│ (BEAM)  │──│ (BEAM)  │        │ │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘        │ │
│  │       Container network (isolated per test)                 │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Test Controller

The test controller is an Elixir module that manages cluster lifecycle:

```elixir
defmodule NeonFS.TestCluster do
  @moduledoc """
  Manages containerised NeonFS clusters for integration testing.
  """

  defstruct [:id, :nodes, :network, :container_runtime]

  @type t :: %__MODULE__{
    id: String.t(),
    nodes: [node_info()],
    network: String.t(),
    container_runtime: :docker | :podman
  }

  @type node_info :: %{
    name: atom(),
    container_id: String.t(),
    ip_address: String.t(),
    rpc_port: pos_integer(),
    state: :running | :paused | :stopped
  }

  @doc """
  Start a new test cluster with the specified number of nodes.

  ## Options

  - `:nodes` - Number of nodes (default: 3)
  - `:runtime` - Container runtime, `:docker` or `:podman` (default: auto-detect)
  - `:image` - NeonFS container image (default: from config)
  - `:volumes` - Volume configurations to create after cluster init

  ## Example

      {:ok, cluster} = TestCluster.start(nodes: 3)

      # Run tests...

      TestCluster.stop(cluster)
  """
  @spec start(keyword()) :: {:ok, t()} | {:error, term()}
  def start(opts \\ []) do
    node_count = Keyword.get(opts, :nodes, 3)
    runtime = Keyword.get(opts, :runtime, detect_runtime())
    image = Keyword.get(opts, :image, default_image())

    cluster_id = generate_cluster_id()
    network = "neonfs-test-#{cluster_id}"

    with :ok <- create_network(runtime, network),
         {:ok, nodes} <- start_nodes(runtime, network, image, node_count),
         :ok <- wait_for_cluster_ready(nodes),
         :ok <- init_cluster(nodes) do
      cluster = %__MODULE__{
        id: cluster_id,
        nodes: nodes,
        network: network,
        container_runtime: runtime
      }

      {:ok, cluster}
    end
  end

  @doc """
  Stop and remove all cluster resources.
  """
  @spec stop(t()) :: :ok
  def stop(%__MODULE__{} = cluster) do
    for node <- cluster.nodes do
      stop_container(cluster.container_runtime, node.container_id)
    end

    remove_network(cluster.container_runtime, cluster.network)
    :ok
  end

  @doc """
  Pause a node (simulates network partition or freeze).
  """
  @spec pause_node(t(), atom()) :: {:ok, t()} | {:error, term()}
  def pause_node(%__MODULE__{} = cluster, node_name) do
    case find_node(cluster, node_name) do
      nil ->
        {:error, :node_not_found}

      node ->
        :ok = pause_container(cluster.container_runtime, node.container_id)
        updated_nodes = update_node_state(cluster.nodes, node_name, :paused)
        {:ok, %{cluster | nodes: updated_nodes}}
    end
  end

  @doc """
  Unpause a previously paused node.
  """
  @spec unpause_node(t(), atom()) :: {:ok, t()} | {:error, term()}
  def unpause_node(%__MODULE__{} = cluster, node_name) do
    case find_node(cluster, node_name) do
      nil ->
        {:error, :node_not_found}

      node ->
        :ok = unpause_container(cluster.container_runtime, node.container_id)
        updated_nodes = update_node_state(cluster.nodes, node_name, :running)
        {:ok, %{cluster | nodes: updated_nodes}}
    end
  end

  @doc """
  Kill a node abruptly (simulates crash).
  """
  @spec kill_node(t(), atom()) :: {:ok, t()} | {:error, term()}
  def kill_node(%__MODULE__{} = cluster, node_name) do
    case find_node(cluster, node_name) do
      nil ->
        {:error, :node_not_found}

      node ->
        :ok = kill_container(cluster.container_runtime, node.container_id)
        updated_nodes = update_node_state(cluster.nodes, node_name, :stopped)
        {:ok, %{cluster | nodes: updated_nodes}}
    end
  end

  @doc """
  Restart a stopped node.
  """
  @spec restart_node(t(), atom()) :: {:ok, t()} | {:error, term()}
  def restart_node(%__MODULE__{} = cluster, node_name) do
    case find_node(cluster, node_name) do
      nil ->
        {:error, :node_not_found}

      node ->
        :ok = start_container(cluster.container_runtime, node.container_id)
        :ok = wait_for_node_ready(node)
        updated_nodes = update_node_state(cluster.nodes, node_name, :running)
        {:ok, %{cluster | nodes: updated_nodes}}
    end
  end

  @doc """
  Execute an RPC call against a specific node.
  """
  @spec rpc(t(), atom(), module(), atom(), [term()]) :: term()
  def rpc(%__MODULE__{} = cluster, node_name, module, function, args) do
    node = find_node(cluster, node_name) || raise "Node #{node_name} not found"

    # Connect to container's BEAM node and execute RPC
    :rpc.call(node.beam_node, module, function, args)
  end

  @doc """
  Get cluster status from any running node.
  """
  @spec cluster_status(t()) :: {:ok, map()} | {:error, term()}
  def cluster_status(%__MODULE__{} = cluster) do
    running_node = Enum.find(cluster.nodes, &(&1.state == :running))

    if running_node do
      rpc(cluster, running_node.name, NeonFS.CLI.Handler, :cluster_status, [])
    else
      {:error, :no_running_nodes}
    end
  end

  @doc """
  Collect logs from all nodes.
  """
  @spec collect_logs(t()) :: %{atom() => String.t()}
  def collect_logs(%__MODULE__{} = cluster) do
    for node <- cluster.nodes, into: %{} do
      logs = get_container_logs(cluster.container_runtime, node.container_id)
      {node.name, logs}
    end
  end

  # Private implementation functions...

  defp detect_runtime do
    cond do
      System.find_executable("podman") -> :podman
      System.find_executable("docker") -> :docker
      true -> raise "No container runtime found (docker or podman)"
    end
  end

  defp generate_cluster_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end

  # ... container management implementation
end
```

### Integration Test Examples

```elixir
defmodule NeonFS.Integration.ClusterFormationTest do
  use ExUnit.Case, async: false

  alias NeonFS.TestCluster

  @moduletag :integration
  @moduletag timeout: 120_000

  setup do
    {:ok, cluster} = TestCluster.start(nodes: 3)
    on_exit(fn -> TestCluster.stop(cluster) end)
    %{cluster: cluster}
  end

  describe "cluster formation" do
    test "all nodes see each other", %{cluster: cluster} do
      {:ok, status} = TestCluster.cluster_status(cluster)

      assert length(status.nodes) == 3
      assert Enum.all?(status.nodes, &(&1.state == :online))
    end

    test "Ra consensus established", %{cluster: cluster} do
      {:ok, status} = TestCluster.cluster_status(cluster)

      assert status.ra_leader != nil
      assert status.ra_term > 0
    end
  end
end

defmodule NeonFS.Integration.ReplicationTest do
  use ExUnit.Case, async: false

  alias NeonFS.TestCluster

  @moduletag :integration
  @moduletag timeout: 180_000

  setup do
    {:ok, cluster} = TestCluster.start(nodes: 3)

    # Create a test volume
    TestCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
      "test-volume",
      %{durability: %{type: :replicate, factor: 3}, write_ack: :quorum}
    ])

    on_exit(fn -> TestCluster.stop(cluster) end)
    %{cluster: cluster}
  end

  describe "write replication" do
    test "data available on all nodes after quorum write", %{cluster: cluster} do
      # Write via node1
      {:ok, file_id} = TestCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
        "test-volume", "/test.txt", "hello world"
      ])

      # Read from each node
      for node <- [:node1, :node2, :node3] do
        {:ok, data} = TestCluster.rpc(cluster, node, NeonFS.TestHelpers, :read_file, [
          "test-volume", "/test.txt"
        ])

        assert data == "hello world", "Data mismatch on #{node}"
      end
    end
  end
end

defmodule NeonFS.Integration.FailureRecoveryTest do
  use ExUnit.Case, async: false

  alias NeonFS.TestCluster

  @moduletag :integration
  @moduletag timeout: 300_000

  setup do
    {:ok, cluster} = TestCluster.start(nodes: 3)

    TestCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
      "test-volume",
      %{durability: %{type: :replicate, factor: 3}, write_ack: :quorum}
    ])

    # Pre-populate some data
    TestCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
      "test-volume", "/important.txt", "critical data"
    ])

    on_exit(fn -> TestCluster.stop(cluster) end)
    %{cluster: cluster}
  end

  describe "single node failure" do
    test "cluster remains operational when one node crashes", %{cluster: cluster} do
      # Kill node3
      {:ok, cluster} = TestCluster.kill_node(cluster, :node3)

      # Give cluster time to detect the failure
      Process.sleep(5_000)

      # Should still be able to read (quorum of 2 available)
      {:ok, data} = TestCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
        "test-volume", "/important.txt"
      ])

      assert data == "critical data"

      # Should still be able to write (quorum of 2 available)
      {:ok, _} = TestCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
        "test-volume", "/new.txt", "new data"
      ])
    end

    test "data is re-replicated after node failure", %{cluster: cluster} do
      # Kill node3
      {:ok, cluster} = TestCluster.kill_node(cluster, :node3)

      # Wait for repair to complete
      :ok = wait_for_repair_complete(cluster, timeout: 60_000)

      # All chunks should be back to factor=3 across remaining nodes
      {:ok, status} = TestCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :volume_status, [
        "test-volume"
      ])

      assert status.chunks_under_replicated == 0
    end

    test "recovered node rejoins and receives data", %{cluster: cluster} do
      # Kill node3
      {:ok, cluster} = TestCluster.kill_node(cluster, :node3)
      Process.sleep(5_000)

      # Write new data while node3 is down
      {:ok, _} = TestCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
        "test-volume", "/written-during-outage.txt", "outage data"
      ])

      # Restart node3
      {:ok, cluster} = TestCluster.restart_node(cluster, :node3)

      # Wait for node to rejoin and sync
      :ok = wait_for_node_synced(cluster, :node3, timeout: 60_000)

      # Node3 should be able to serve the data written during outage
      {:ok, data} = TestCluster.rpc(cluster, :node3, NeonFS.TestHelpers, :read_file, [
        "test-volume", "/written-during-outage.txt"
      ])

      assert data == "outage data"
    end
  end

  describe "network partition (simulated via pause)" do
    test "minority partition becomes read-only", %{cluster: cluster} do
      # Pause node3 (simulates partition where node3 is isolated)
      {:ok, cluster} = TestCluster.pause_node(cluster, :node3)
      Process.sleep(2_000)

      # Majority (node1, node2) can still write
      {:ok, _} = TestCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
        "test-volume", "/majority-write.txt", "majority data"
      ])

      # Unpause to verify
      {:ok, _cluster} = TestCluster.unpause_node(cluster, :node3)
    end

    test "cluster heals after partition resolves", %{cluster: cluster} do
      # Pause node3
      {:ok, cluster} = TestCluster.pause_node(cluster, :node3)

      # Write during partition
      {:ok, _} = TestCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
        "test-volume", "/partition-write.txt", "partition data"
      ])

      # Resolve partition
      {:ok, cluster} = TestCluster.unpause_node(cluster, :node3)

      # Wait for reconciliation
      Process.sleep(10_000)

      # Node3 should have the data
      {:ok, data} = TestCluster.rpc(cluster, :node3, NeonFS.TestHelpers, :read_file, [
        "test-volume", "/partition-write.txt"
      ])

      assert data == "partition data"
    end
  end

  defp wait_for_repair_complete(cluster, opts) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    interval = 1_000

    wait_until(timeout, interval, fn ->
      {:ok, status} = TestCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, [])
      status.repair_in_progress == false
    end)
  end

  defp wait_for_node_synced(cluster, node_name, opts) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    interval = 1_000

    wait_until(timeout, interval, fn ->
      {:ok, status} = TestCluster.cluster_status(cluster)
      node_status = Enum.find(status.nodes, &(&1.name == node_name))
      node_status && node_status.state == :online && node_status.synced == true
    end)
  end

  defp wait_until(timeout, interval, condition) when timeout > 0 do
    if condition.() do
      :ok
    else
      Process.sleep(interval)
      wait_until(timeout - interval, interval, condition)
    end
  end

  defp wait_until(_timeout, _interval, _condition) do
    {:error, :timeout}
  end
end
```

### Test Data Management

Integration tests need reproducible test data. Provide helpers for common scenarios:

```elixir
defmodule NeonFS.TestHelpers do
  @moduledoc """
  Helper functions available on test cluster nodes for integration testing.
  """

  @doc """
  Write a file with the given content.
  """
  def write_file(volume, path, content) do
    # Implementation calls actual NeonFS write path
  end

  @doc """
  Read a file's content.
  """
  def read_file(volume, path) do
    # Implementation calls actual NeonFS read path
  end

  @doc """
  Generate random file content of specified size.
  """
  def random_content(size_bytes) do
    :crypto.strong_rand_bytes(size_bytes)
  end

  @doc """
  Generate a directory tree with specified structure.
  """
  def generate_tree(volume, base_path, spec) do
    # spec is like %{files: 100, dirs: 10, depth: 3, file_size: 1..1000}
  end
end
```

### Running Integration Tests

```bash
# Run all integration tests
mix test --only integration

# Run with specific cluster size
NEONFS_TEST_NODES=5 mix test --only integration

# Keep cluster running after test for debugging
NEONFS_TEST_KEEP_CLUSTER=true mix test test/integration/failure_test.exs:42

# Use specific container image
NEONFS_TEST_IMAGE=neonfs:dev mix test --only integration
```

---

## Manual Testing

For developer exploratory testing, provide a simple way to spin up local clusters.

### Quick Start

```bash
# Start a 3-node cluster for manual testing
./scripts/dev-cluster start

# Cluster is now running. Access via:
#   - CLI: neonfs --cluster dev cluster status
#   - Mount: neonfs --cluster dev mount /tmp/neonfs-test --volume test
#   - Logs: ./scripts/dev-cluster logs [node1|node2|node3]

# Open a shell on a specific node
./scripts/dev-cluster shell node1

# Simulate failures
./scripts/dev-cluster pause node2      # Network partition
./scripts/dev-cluster unpause node2
./scripts/dev-cluster kill node3       # Crash
./scripts/dev-cluster restart node3

# Stop and clean up
./scripts/dev-cluster stop
```

### Dev Cluster Script

```bash
#!/usr/bin/env bash
# scripts/dev-cluster

set -euo pipefail

CLUSTER_NAME="neonfs-dev"
NODE_COUNT="${NEONFS_DEV_NODES:-3}"
IMAGE="${NEONFS_DEV_IMAGE:-neonfs:dev}"
RUNTIME="${NEONFS_RUNTIME:-$(command -v podman > /dev/null && echo podman || echo docker)}"

case "${1:-}" in
  start)
    echo "Starting ${NODE_COUNT}-node dev cluster..."

    # Create network
    $RUNTIME network create "${CLUSTER_NAME}" 2>/dev/null || true

    # Start nodes
    for i in $(seq 1 $NODE_COUNT); do
      $RUNTIME run -d \
        --name "${CLUSTER_NAME}-node${i}" \
        --network "${CLUSTER_NAME}" \
        --hostname "node${i}" \
        -v "${CLUSTER_NAME}-node${i}-data:/var/lib/neonfs" \
        "${IMAGE}"
    done

    # Wait for cluster to form
    echo "Waiting for cluster to initialise..."
    sleep 10

    # Initialise cluster on node1
    $RUNTIME exec "${CLUSTER_NAME}-node1" neonfs cluster init --name dev

    # Join other nodes
    for i in $(seq 2 $NODE_COUNT); do
      TOKEN=$($RUNTIME exec "${CLUSTER_NAME}-node1" neonfs cluster create-invite --expires 5m | tail -1)
      $RUNTIME exec "${CLUSTER_NAME}-node${i}" neonfs cluster join --token "$TOKEN"
    done

    echo "Dev cluster ready!"
    echo "  Nodes: node1, node2, node3"
    echo "  Use: ./scripts/dev-cluster shell node1"
    ;;

  stop)
    echo "Stopping dev cluster..."
    for i in $(seq 1 $NODE_COUNT); do
      $RUNTIME rm -f "${CLUSTER_NAME}-node${i}" 2>/dev/null || true
      $RUNTIME volume rm "${CLUSTER_NAME}-node${i}-data" 2>/dev/null || true
    done
    $RUNTIME network rm "${CLUSTER_NAME}" 2>/dev/null || true
    echo "Done."
    ;;

  shell)
    node="${2:-node1}"
    $RUNTIME exec -it "${CLUSTER_NAME}-${node}" /bin/bash
    ;;

  logs)
    node="${2:-node1}"
    $RUNTIME logs -f "${CLUSTER_NAME}-${node}"
    ;;

  pause)
    node="${2:?Usage: dev-cluster pause <node>}"
    $RUNTIME pause "${CLUSTER_NAME}-${node}"
    echo "Paused ${node}"
    ;;

  unpause)
    node="${2:?Usage: dev-cluster unpause <node>}"
    $RUNTIME unpause "${CLUSTER_NAME}-${node}"
    echo "Unpaused ${node}"
    ;;

  kill)
    node="${2:?Usage: dev-cluster kill <node>}"
    $RUNTIME kill "${CLUSTER_NAME}-${node}"
    echo "Killed ${node}"
    ;;

  restart)
    node="${2:?Usage: dev-cluster restart <node>}"
    $RUNTIME start "${CLUSTER_NAME}-${node}"
    echo "Restarted ${node}"
    ;;

  status)
    $RUNTIME exec "${CLUSTER_NAME}-node1" neonfs cluster status
    ;;

  *)
    echo "Usage: dev-cluster {start|stop|shell|logs|pause|unpause|kill|restart|status} [node]"
    exit 1
    ;;
esac
```

---

## Test Organisation and CI

### Directory Structure

```
test/
├── neonfs/                          # Unit tests (Elixir)
│   ├── metadata/
│   ├── volume/
│   └── cluster/
├── neonfs_property/                 # Property tests (Elixir)
│   ├── hlc_property_test.exs
│   └── quorum_property_test.exs
├── neonfs_nif/                      # NIF boundary tests
│   ├── blob_native_test.exs
│   └── fuse_native_test.exs
├── integration/                     # Integration tests
│   ├── cluster_formation_test.exs
│   ├── replication_test.exs
│   ├── failure_recovery_test.exs
│   └── api_surface_test.exs
├── support/
│   ├── test_cluster.ex              # TestCluster module
│   ├── test_helpers.ex
│   └── fixtures/
└── test_helper.exs

native/neonfs_blob/
├── src/
└── tests/                           # Rust unit tests (in-crate)

native/neonfs_fuse/
├── src/
└── tests/

native/neonfs-cli/
├── src/
├── tests/
└── fuzz/                            # Fuzz targets
```

### CI Pipeline

```yaml
# .github/workflows/test.yml or equivalent

name: Test

on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Elixir
        uses: erlef/setup-beam@v1
        with:
          elixir-version: '1.16'
          otp-version: '26'

      - name: Setup Rust
        uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy

      - name: Elixir lint
        run: |
          mix deps.get
          mix format --check-formatted
          mix credo --strict

      - name: Rust lint
        run: |
          cargo fmt --check --manifest-path native/neonfs_blob/Cargo.toml
          cargo fmt --check --manifest-path native/neonfs_fuse/Cargo.toml
          cargo fmt --check --manifest-path native/neonfs-cli/Cargo.toml
          cargo clippy --all-targets --manifest-path native/neonfs_blob/Cargo.toml -- -D warnings
          cargo clippy --all-targets --manifest-path native/neonfs_fuse/Cargo.toml -- -D warnings
          cargo clippy --all-targets --manifest-path native/neonfs-cli/Cargo.toml -- -D warnings

  dialyzer:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: erlef/setup-beam@v1
        with:
          elixir-version: '1.16'
          otp-version: '26'

      - name: Restore PLT cache
        uses: actions/cache@v4
        with:
          path: priv/plts
          key: plt-${{ runner.os }}-${{ hashFiles('mix.lock') }}

      - name: Run Dialyzer
        run: |
          mix deps.get
          mix dialyzer

  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: erlef/setup-beam@v1
        with:
          elixir-version: '1.16'
          otp-version: '26'
      - uses: dtolnay/rust-toolchain@stable

      - name: Elixir unit tests
        run: |
          mix deps.get
          mix test --exclude integration

      - name: Rust unit tests
        run: |
          cargo test --manifest-path native/neonfs_blob/Cargo.toml
          cargo test --manifest-path native/neonfs_fuse/Cargo.toml
          cargo test --manifest-path native/neonfs-cli/Cargo.toml

  integration-tests:
    runs-on: ubuntu-latest
    needs: [lint, unit-tests]
    steps:
      - uses: actions/checkout@v4
      - uses: erlef/setup-beam@v1
        with:
          elixir-version: '1.16'
          otp-version: '26'
      - uses: dtolnay/rust-toolchain@stable

      - name: Build test image
        run: docker build -t neonfs:test .

      - name: Run integration tests
        run: |
          mix deps.get
          NEONFS_TEST_IMAGE=neonfs:test mix test --only integration
        timeout-minutes: 30

  fuzz:
    runs-on: ubuntu-latest
    needs: [lint, unit-tests]
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@nightly

      - name: Install cargo-fuzz
        run: cargo install cargo-fuzz

      - name: Fuzz chunk parsing (5 minutes)
        run: |
          cd native/neonfs_blob
          cargo +nightly fuzz run chunk_parsing -- -max_total_time=300

      - name: Fuzz compression (5 minutes)
        run: |
          cd native/neonfs_blob
          cargo +nightly fuzz run compression -- -max_total_time=300
```

---

## Test Coverage Goals

| Component | Target | Notes |
|-----------|--------|-------|
| Elixir modules | 80% line coverage | Focus on business logic, not boilerplate |
| Rust crates | 80% line coverage | Focus on data handling and error paths |
| NIF boundary | 100% of exported functions | Every NIF function has at least one test |
| Integration scenarios | All failure modes documented in spec | Cluster formation, replication, recovery |

Use coverage tools:
- Elixir: `mix test --cover` or `excoveralls`
- Rust: `cargo tarpaulin` or `cargo llvm-cov`

---

## Summary

| Testing Layer | Tools | Focus |
|---------------|-------|-------|
| Static analysis | Dialyzer, Clippy, Credo | Type safety, code quality |
| Unit tests | ExUnit, cargo test | Individual module correctness |
| Property tests | StreamData, proptest | Invariants across input space |
| Fuzzing | cargo-fuzz | Crash resistance, parsing robustness |
| NIF tests | ExUnit | Rust/Elixir boundary correctness |
| Integration tests | TestCluster + containers | Distributed behaviour, failure recovery |
| Manual testing | dev-cluster script | Developer exploration |
