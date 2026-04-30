defmodule NeonFS.Containerd.Metadata do
  @moduledoc """
  Helpers for translating between NeonFS `FileMeta` and containerd
  `Containerd.Services.Content.V1.Info` shapes.

  Labels live in the `xattrs` map of the FileMeta — the design call
  in #547 (POSIX xattrs via the surface from #671 / #676). The
  containerd label namespace is unprefixed in the protobuf (`labels:
  map<string, string>`), so this module wraps and unwraps with a
  fixed `containerd.io/` xattr prefix to keep containerd's labels
  isolated from anything else that might write xattrs on the same
  blob.
  """

  alias Containerd.Services.Content.V1.Info
  alias NeonFS.Containerd.Digest

  @label_xattr_prefix "containerd.io/"

  @doc """
  Build an `Info` proto message from a NeonFS `FileMeta`.

  The FileMeta's path is reverse-mapped to a `sha256:<hex>` digest
  via `path_to_digest/1`. Returns `:error` if the path doesn't fit
  the expected `sha256/<ab>/<cd>/<rest>` shape — which means the
  blob exists but isn't a containerd-managed entry.
  """
  @spec info_from_file_meta(map()) :: {:ok, Info.t()} | :error
  def info_from_file_meta(%{path: path} = meta) do
    case path_to_digest(path) do
      {:ok, digest} ->
        {:ok,
         %Info{
           digest: digest,
           size: Map.get(meta, :size, 0),
           created_at: to_timestamp(Map.get(meta, :created_at)),
           updated_at: to_timestamp(Map.get(meta, :updated_at)),
           labels: extract_labels(Map.get(meta, :xattrs, %{}))
         }}

      :error ->
        :error
    end
  end

  @doc """
  Reverse of `NeonFS.Containerd.Digest.to_path/1`. Returns the
  `sha256:<hex>` digest for a sharded `sha256/<ab>/<cd>/<rest>`
  path. `:error` for paths that don't match the layout.

  ## Examples

      iex> NeonFS.Containerd.Metadata.path_to_digest("sha256/01/23/456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
      {:ok, "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}

      iex> NeonFS.Containerd.Metadata.path_to_digest("/sha256/01/23/456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
      {:ok, "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}

      iex> NeonFS.Containerd.Metadata.path_to_digest("_writes/x")
      :error

      iex> NeonFS.Containerd.Metadata.path_to_digest("notsha")
      :error
  """
  @spec path_to_digest(String.t()) :: {:ok, String.t()} | :error
  def path_to_digest(path) when is_binary(path) do
    trimmed = String.trim_leading(path, "/")

    case String.split(trimmed, "/", parts: 4) do
      ["sha256", a, b, rest]
      when byte_size(a) == 2 and byte_size(b) == 2 and byte_size(rest) == 60 ->
        hex = a <> b <> rest

        if Regex.match?(~r/\A[0-9a-f]{64}\z/, hex) do
          {:ok, "sha256:" <> hex}
        else
          :error
        end

      _ ->
        :error
    end
  end

  @doc """
  Returns the volume path for a containerd digest. Thin wrapper
  around `NeonFS.Containerd.Digest.to_path/1` so callers in this
  module can stay decoupled from `Digest`.
  """
  @spec digest_to_path(String.t()) ::
          {:ok, String.t()} | {:error, :invalid_digest | :unsupported_algorithm}
  def digest_to_path(digest), do: Digest.to_path(digest)

  @doc """
  Apply a containerd `UpdateRequest`'s field mask to the existing
  labels stored in the blob's xattrs. Returns the new label map
  (still flat — caller is responsible for re-prefixing into xattrs
  via `wrap_labels/1`).

  Containerd's mask semantics:

    * Empty / absent mask → replace all labels with the request's
      labels.
    * Mask path `"labels"` → replace all labels.
    * Mask path `"labels.<key>"` → set / clear that one label only.
      Empty value clears the label entry.

  Other mask paths are ignored — the only mutable surface
  containerd's content-store schema has is `labels`. (The rest are
  derived from the blob bytes.)
  """
  @spec apply_label_mask(
          %{optional(String.t()) => String.t()},
          %{optional(String.t()) => String.t()},
          [String.t()]
        ) :: %{optional(String.t()) => String.t()}
  def apply_label_mask(current_labels, request_labels, mask_paths) do
    cond do
      mask_paths in [nil, []] -> request_labels
      "labels" in mask_paths -> request_labels
      true -> apply_per_key_mask(current_labels, request_labels, mask_paths)
    end
  end

  defp apply_per_key_mask(current_labels, request_labels, mask_paths) do
    mask_paths
    |> Enum.filter(&String.starts_with?(&1, "labels."))
    |> Enum.reduce(current_labels, fn "labels." <> key, acc ->
      apply_label_op(acc, key, Map.get(request_labels, key, ""))
    end)
  end

  defp apply_label_op(labels, key, ""), do: Map.delete(labels, key)
  defp apply_label_op(labels, key, value), do: Map.put(labels, key, value)

  @doc """
  Pull the containerd labels out of an xattrs map (strips the
  `containerd.io/` prefix; ignores unprefixed entries).
  """
  @spec extract_labels(%{optional(String.t()) => String.t()}) :: %{
          optional(String.t()) => String.t()
        }
  def extract_labels(xattrs) when is_map(xattrs) do
    for {key, value} <- xattrs,
        String.starts_with?(key, @label_xattr_prefix),
        into: %{},
        do: {String.slice(key, byte_size(@label_xattr_prefix)..-1//1), value}
  end

  def extract_labels(_), do: %{}

  @doc """
  Wrap a flat label map back into the xattrs shape, prefixing each
  key with `containerd.io/`. Use when handing labels back to
  `update_file_meta/3`.
  """
  @spec wrap_labels(%{optional(String.t()) => String.t()}) :: %{
          optional(String.t()) => String.t()
        }
  def wrap_labels(labels) when is_map(labels) do
    Map.new(labels, fn {key, value} -> {@label_xattr_prefix <> key, value} end)
  end

  @doc """
  Merge the containerd labels into the FileMeta's full xattrs map
  without disturbing non-label entries.
  """
  @spec merge_labels_into_xattrs(
          %{optional(String.t()) => String.t()},
          %{optional(String.t()) => String.t()}
        ) :: %{optional(String.t()) => String.t()}
  def merge_labels_into_xattrs(existing_xattrs, new_labels) do
    non_label =
      for {key, value} <- existing_xattrs,
          not String.starts_with?(key, @label_xattr_prefix),
          into: %{},
          do: {key, value}

    Map.merge(non_label, wrap_labels(new_labels))
  end

  defp to_timestamp(%DateTime{} = dt) do
    seconds = DateTime.to_unix(dt, :second)
    nanos = (DateTime.to_unix(dt, :microsecond) - seconds * 1_000_000) * 1_000
    %Google.Protobuf.Timestamp{seconds: seconds, nanos: nanos}
  end

  defp to_timestamp(_), do: nil
end
