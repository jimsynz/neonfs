defmodule NeonFS.Api.DataLayer.Query do
  @moduledoc """
  Contains information about a query for the data layer.
  """

  defstruct aggregates: [],
            calculations: [],
            distict: nil,
            distinct_sort: nil,
            domain: nil,
            filter: nil,
            limit: :infinity,
            offset: 0,
            relationships: %{},
            resource: nil,
            sort: nil
end
