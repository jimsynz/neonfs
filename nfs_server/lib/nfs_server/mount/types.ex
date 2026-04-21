defmodule NFSServer.Mount.Types do
  @moduledoc """
  XDR types for the MOUNT v3 protocol — [RFC 1813 appendix I](https://www.rfc-editor.org/rfc/rfc1813#appendix-I).

  Constants:

      MNTPATHLEN = 1024  # max path bytes
      MNTNAMLEN  = 255   # max name bytes
      FHSIZE3    = 64    # max v3 file-handle bytes

  Linked-list types (`mountlist`, `exportnode`, `groupnode`) use
  XDR optional-data (§4.19) chains: each node carries its data plus
  an optional pointer to the next node, and the chain ends with a
  `false` flag. We model them as plain Elixir lists at the Elixir
  layer and serialise/deserialise the chain shape on encode/decode.
  """

  alias NFSServer.XDR

  @mntpathlen 1024
  @mntnamlen 255
  @fhsize3 64

  @typedoc "Opaque file-handle, ≤ 64 bytes."
  @type fhandle3 :: binary()

  @typedoc "Mount path string, ≤ 1024 bytes."
  @type dirpath :: binary()

  @typedoc "Hostname / group name string, ≤ 255 bytes."
  @type name :: binary()

  @typedoc "Mount status — `:ok` is success, the rest are RFC 1813 errno-style codes."
  @type mountstat3 ::
          :ok
          | :perm
          | :noent
          | :io
          | :acces
          | :notdir
          | :inval
          | :nametoolong
          | :notsupp
          | :serverfault

  defmodule MountList do
    @moduledoc "One entry in `MOUNTPROC3_DUMP`'s reply."
    defstruct [:hostname, :directory]

    @type t :: %__MODULE__{
            hostname: NFSServer.Mount.Types.name(),
            directory: NFSServer.Mount.Types.dirpath()
          }
  end

  defmodule ExportNode do
    @moduledoc "One entry in `MOUNTPROC3_EXPORT`'s reply: a directory and the groups allowed to mount it."
    defstruct dir: "", groups: []

    @type t :: %__MODULE__{
            dir: NFSServer.Mount.Types.dirpath(),
            groups: [NFSServer.Mount.Types.name()]
          }
  end

  # ——— Constants ————————————————————————————————————————————————

  @doc "RFC 1813 max path length."
  @spec mntpathlen() :: 1024
  def mntpathlen, do: @mntpathlen

  @doc "RFC 1813 max name length."
  @spec mntnamlen() :: 255
  def mntnamlen, do: @mntnamlen

  @doc "RFC 1813 max v3 file-handle size."
  @spec fhsize3() :: 64
  def fhsize3, do: @fhsize3

  # ——— mountstat3 ———————————————————————————————————————————————

  @stat_codes %{
    ok: 0,
    perm: 1,
    noent: 2,
    io: 5,
    acces: 13,
    notdir: 20,
    inval: 22,
    nametoolong: 63,
    notsupp: 10_004,
    serverfault: 10_006
  }

  @code_to_stat Map.new(@stat_codes, fn {atom, code} -> {code, atom} end)

  @doc "Encode a `mountstat3` atom as its 32-bit wire integer."
  @spec encode_stat(mountstat3()) :: binary()
  def encode_stat(stat) when is_atom(stat) do
    code = Map.fetch!(@stat_codes, stat)
    XDR.encode_uint(code)
  end

  @doc """
  Decode a 32-bit mountstat3 code back to its atom. Unknown codes
  surface as `{:unknown, n}` so handlers can report it without
  crashing.
  """
  @spec decode_stat(binary()) ::
          {:ok, mountstat3() | {:unknown, non_neg_integer()}, binary()} | {:error, term()}
  def decode_stat(binary) do
    with {:ok, code, rest} <- XDR.decode_uint(binary) do
      atom = Map.get(@code_to_stat, code, {:unknown, code})
      {:ok, atom, rest}
    end
  end

  # ——— dirpath / name ——————————————————————————————————————————

  @doc "Encode a bounded `dirpath` (path name)."
  @spec encode_dirpath(dirpath()) :: binary()
  def encode_dirpath(path) when is_binary(path) and byte_size(path) <= @mntpathlen,
    do: XDR.encode_string(path)

  @doc "Decode a bounded `dirpath`."
  @spec decode_dirpath(binary()) :: {:ok, dirpath(), binary()} | {:error, term()}
  def decode_dirpath(binary) do
    with {:ok, path, rest} <- XDR.decode_string(binary),
         :ok <- check_max_size(path, @mntpathlen, :path_too_long) do
      {:ok, path, rest}
    end
  end

  @doc "Encode a bounded `name`."
  @spec encode_name(name()) :: binary()
  def encode_name(name) when is_binary(name) and byte_size(name) <= @mntnamlen,
    do: XDR.encode_string(name)

  @doc "Decode a bounded `name`."
  @spec decode_name(binary()) :: {:ok, name(), binary()} | {:error, term()}
  def decode_name(binary) do
    with {:ok, name, rest} <- XDR.decode_string(binary),
         :ok <- check_max_size(name, @mntnamlen, :name_too_long) do
      {:ok, name, rest}
    end
  end

  # ——— fhandle3 (variable opaque ≤ 64) —————————————————————————

  @doc "Encode a `fhandle3` — variable-length opaque ≤ 64 bytes."
  @spec encode_fhandle3(fhandle3()) :: binary()
  def encode_fhandle3(fh) when is_binary(fh) and byte_size(fh) <= @fhsize3,
    do: XDR.encode_var_opaque(fh)

  @doc "Decode a `fhandle3`."
  @spec decode_fhandle3(binary()) :: {:ok, fhandle3(), binary()} | {:error, term()}
  def decode_fhandle3(binary) do
    with {:ok, fh, rest} <- XDR.decode_var_opaque(binary),
         :ok <- check_max_size(fh, @fhsize3, :fhandle_too_long) do
      {:ok, fh, rest}
    end
  end

  # ——— mountres3 (MNT reply) ——————————————————————————————————

  @doc """
  Encode a successful `mountres3` (MNT_OK with file-handle and the
  list of accepted auth flavours).
  """
  @spec encode_mountres3_ok(fhandle3(), [non_neg_integer()]) :: binary()
  def encode_mountres3_ok(fhandle, auth_flavors) when is_list(auth_flavors) do
    encode_stat(:ok) <>
      encode_fhandle3(fhandle) <>
      XDR.encode_var_array(auth_flavors, &XDR.encode_uint/1)
  end

  @doc "Encode a failure `mountres3` (status only, no body)."
  @spec encode_mountres3_err(mountstat3()) :: binary()
  def encode_mountres3_err(status) when status != :ok, do: encode_stat(status)

  # ——— Linked-list helpers (mountlist / exportnode / groupnode) ——

  @doc """
  Encode a list of items as an XDR optional-data linked list. Each
  item is encoded by `encoder`, then a TRUE flag points at the next
  item; the final item is followed by a FALSE flag (no next).
  """
  @spec encode_chain([term()], (term() -> binary())) :: binary()
  def encode_chain(items, encoder) when is_list(items) and is_function(encoder, 1) do
    do_encode_chain(items, encoder, <<>>)
  end

  defp do_encode_chain([], _encoder, acc), do: acc <> XDR.encode_bool(false)

  defp do_encode_chain([item | rest], encoder, acc) do
    acc =
      acc <>
        XDR.encode_bool(true) <>
        encoder.(item)

    do_encode_chain(rest, encoder, acc)
  end

  @doc """
  Decode an XDR optional-data linked list into a flat Elixir list.
  Each item is decoded by `decoder/1` which consumes one item from
  the binary and returns `{:ok, item, rest}`.
  """
  @spec decode_chain(binary(), (binary() -> {:ok, term(), binary()} | {:error, term()})) ::
          {:ok, [term()], binary()} | {:error, term()}
  def decode_chain(binary, decoder) when is_function(decoder, 1) do
    do_decode_chain(binary, decoder, [])
  end

  defp do_decode_chain(binary, decoder, acc) do
    case XDR.decode_bool(binary) do
      {:ok, false, rest} -> {:ok, Enum.reverse(acc), rest}
      {:ok, true, rest} -> consume_chain_item(rest, decoder, acc)
      {:error, _} = err -> err
    end
  end

  defp consume_chain_item(binary, decoder, acc) do
    case decoder.(binary) do
      {:ok, item, rest} -> do_decode_chain(rest, decoder, [item | acc])
      {:error, _} = err -> err
    end
  end

  # ——— mountlist node ——————————————————————————————————————————

  @doc "Encode one mountlist entry — hostname + directory."
  @spec encode_mountlist_entry(MountList.t()) :: binary()
  def encode_mountlist_entry(%MountList{hostname: h, directory: d}) do
    encode_name(h) <> encode_dirpath(d)
  end

  @doc "Decode one mountlist entry."
  @spec decode_mountlist_entry(binary()) :: {:ok, MountList.t(), binary()} | {:error, term()}
  def decode_mountlist_entry(binary) do
    with {:ok, hostname, rest} <- decode_name(binary),
         {:ok, directory, rest2} <- decode_dirpath(rest) do
      {:ok, %MountList{hostname: hostname, directory: directory}, rest2}
    end
  end

  # ——— exportnode ——————————————————————————————————————————————

  @doc "Encode one exportnode — directory + group list (chained)."
  @spec encode_exportnode(ExportNode.t()) :: binary()
  def encode_exportnode(%ExportNode{dir: dir, groups: groups}) do
    encode_dirpath(dir) <> encode_chain(groups, &encode_name/1)
  end

  @doc "Decode one exportnode."
  @spec decode_exportnode(binary()) :: {:ok, ExportNode.t(), binary()} | {:error, term()}
  def decode_exportnode(binary) do
    with {:ok, dir, rest} <- decode_dirpath(binary),
         {:ok, groups, rest2} <- decode_chain(rest, &decode_name/1) do
      {:ok, %ExportNode{dir: dir, groups: groups}, rest2}
    end
  end

  # ——— Internal ————————————————————————————————————————————————

  defp check_max_size(value, max, _err) when byte_size(value) <= max, do: :ok
  defp check_max_size(_value, _max, err), do: {:error, err}
end
