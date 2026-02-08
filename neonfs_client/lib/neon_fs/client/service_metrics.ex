defmodule NeonFS.Client.ServiceMetrics do
  @moduledoc """
  Load metrics for a service node, used by cost-based routing.

  Metrics are published by each node and collected by the CostFunction
  GenServer to make routing decisions.
  """

  @type t :: %__MODULE__{
          node: node(),
          cpu_pressure: float(),
          io_pressure: float(),
          storage_pressure: float(),
          queue_depth: non_neg_integer(),
          updated_at: DateTime.t() | nil
        }

  @enforce_keys [:node]
  defstruct [
    :node,
    cpu_pressure: 0.0,
    io_pressure: 0.0,
    storage_pressure: 0.0,
    queue_depth: 0,
    updated_at: nil
  ]

  @doc """
  Creates a new ServiceMetrics struct.
  """
  @spec new(node(), keyword()) :: t()
  def new(node, opts \\ []) do
    %__MODULE__{
      node: node,
      cpu_pressure: Keyword.get(opts, :cpu_pressure, 0.0),
      io_pressure: Keyword.get(opts, :io_pressure, 0.0),
      storage_pressure: Keyword.get(opts, :storage_pressure, 0.0),
      queue_depth: Keyword.get(opts, :queue_depth, 0),
      updated_at: Keyword.get(opts, :updated_at, DateTime.utc_now())
    }
  end

  @doc """
  Converts metrics to a plain map for RPC transport.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = metrics) do
    %{
      node: metrics.node,
      cpu_pressure: metrics.cpu_pressure,
      io_pressure: metrics.io_pressure,
      storage_pressure: metrics.storage_pressure,
      queue_depth: metrics.queue_depth,
      updated_at: metrics.updated_at
    }
  end
end
