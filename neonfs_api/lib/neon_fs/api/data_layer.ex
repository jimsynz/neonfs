defmodule NeonFS.Api.DataLayer do
  @moduledoc """
  An Ash data-layer which wraps the NeonFS.Client.KV store.
  """

  alias Ash.Actions.Sort
  alias Ash.Changeset
  alias Ash.Error.Changes.InvalidAttribute
  alias Ash.Error.Query.NotFound
  alias Ash.Filter.Runtime
  alias Ash.Resource.Info
  alias Ecto.Schema.Metadata
  alias NeonFS.Api.DataLayer.Query
  alias NeonFS.Client.KV

  @behaviour Ash.DataLayer
  use Spark.Dsl.Extension

  @doc false
  @impl true
  def can?(_, :create), do: true
  def can?(_, :update), do: true
  def can?(_, :upsert), do: true
  def can?(_, :destroy), do: true
  def can?(_, :read), do: true
  def can?(_, :multitenancy), do: false
  def can?(_, :filter), do: true
  def can?(_, :limit), do: true
  def can?(_, :offset), do: true
  def can?(_, :distinct), do: true
  def can?(_, :distinct_sort), do: true
  def can?(_, {:filter_expr, _}), do: true
  def can?(_, :boolean_filter), do: true
  def can?(_, :sort), do: true
  def can?(_, {:sort, _}), do: true
  def can?(_, :nested_expressions), do: true
  def can?(_, _), do: false

  @doc false
  @impl true
  def create(resource, changeset) do
    with {:ok, record} <- Changeset.apply_attributes(changeset),
         {:ok, key} <- make_key(record),
         :ok <- KV.put(key, record) do
      {:ok, set_loaded(record)}
    else
      {:error, exists} when is_struct(exists, NeonFS.Error.KeyExists) ->
        errors =
          resource
          |> Info.primary_key()
          |> Enum.map(&InvalidAttribute.exception(field: &1, message: "has already been taken"))

        {:error, errors}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  @impl true
  def upsert(resource, changeset, keys) do
    with {:ok, record} <- Changeset.apply_attributes(changeset),
         {:ok, key} <- make_key(record, keys),
         :ok <- KV.put(key, record, overwrite?: true) do
      {:ok, set_loaded(record)}
    else
      {:error, exists} when is_struct(exists, NeonFS.Error.KeyNotFound) ->
        {:error, %NotFound{primary_key: Info.primary_key(resource), resource: resource}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  @impl true
  def update(_resource, changeset) do
    with {:ok, key} <- make_key(changeset.data),
         {:ok, record} <-
           KV.update(key, fn _record ->
             {:ok, record} = Changeset.apply_attributes(changeset)
             record
           end) do
      {:ok, set_loaded(record)}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @impl true
  def destroy(resource, changeset) do
    with {:ok, key} <- make_key(changeset.data),
         :ok <- KV.delete(key) do
      :ok
    else
      {:error, exists} when is_struct(exists, NeonFS.Error.KeyNotFound) ->
        {:error, %NotFound{primary_key: Info.primary_key(resource), resource: resource}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  @impl true
  def resource_to_query(resource, domain), do: %Query{resource: resource, domain: domain}

  @doc false
  @impl true
  def limit(query, limit, _), do: {:ok, %{query | limit: limit}}

  @doc false
  @impl true
  def offset(query, offset, _), do: {:ok, %{query | offset: offset}}

  @doc false
  @impl true
  def add_calculation(query, calculation, _, _),
    do: {:ok, %{query | calculations: [calculation | query.calculations]}}

  @doc false
  @impl true
  def add_aggregate(query, aggregate, _),
    do: {:ok, %{query | aggregates: [aggregate | query.aggregates]}}

  @doc false
  @impl true
  def filter(query, filter, _resource) when is_nil(query.filter),
    do: {:ok, %{query | filter: filter}}

  def filter(query, filter, _resource),
    do: {:ok, %{query | filter: Ash.Filter.add_to_filter(query.filter, filter)}}

  @doc false
  @impl true
  def sort(query, sort, _resource), do: {:ok, %{query | sort: sort}}

  @doc false
  @impl true
  def distinct(query, distinct, _resource) do
    {:ok, %{query | distinct: distinct}}
  end

  @impl true
  def distinct_sort(query, distinct_sort, _resource) do
    {:ok, %{query | distinct_sort: distinct_sort}}
  end

  @doc false
  @impl true
  def run_query(query, resource, parent \\ nil) do
    with {:ok, stream} <- get_records(resource),
         {:ok, records} <- filter_matches(stream, query, parent),
         {:ok, records} <- runtime_sort(records, query) do
      {:ok, records}
    else
      {:error, reason} -> {:error, Ash.Error.to_ash_error(reason)}
    end
  end

  defp get_records(resource) do
    KV.query(fn
      {{__MODULE__, ^resource, _}, _} -> true
      _ -> false
    end)
  end

  defp filter_matches(records, query, _parent) when is_nil(query.filter), do: {:ok, records}

  defp filter_matches(records, query, parent) do
    query.domain
    |> Runtime.filter_matches(records, query.filter, parent: parent)
  end

  defp runtime_sort(records, query) when is_list(records) do
    records =
      records
      |> Sort.runtime_sort(query.distinct_sort || query.sort, domain: query.domain)
      |> Sort.runtime_distinct(query.distinct, domain: query.domain)
      |> Sort.runtime_sort(query.sort, domain: query.domain)
      |> Stream.drop(query.offset || 0)
      |> do_limit(query.limit)
      |> Enum.map(&set_loaded/1)

    {:ok, records}
  end

  defp do_limit(records, :infinity), do: records
  defp do_limit(records, limit), do: Stream.take(records, limit)

  defp make_key(record, keys \\ nil) do
    pkey = Info.primary_key(record.__struct__)
    keys = keys || pkey

    pk = Enum.map(keys, &Map.get(record, &1))

    {__MODULE__, record.__struct__, pk}
  end

  defp set_loaded(record) do
    %{record | __meta__: %Metadata{state: :loaded, schema: record.__struct__}}
  end
end
