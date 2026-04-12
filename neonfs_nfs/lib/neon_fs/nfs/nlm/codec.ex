defmodule NeonFS.NFS.NLM.Codec do
  @moduledoc """
  XDR codec for NLM v4 (Network Lock Manager) types per RFC 1813 Appendix I.

  NLM v4 uses 64-bit offsets/lengths for NFSv3 compatibility.
  """

  alias NeonFS.NFS.RPC.XDR

  @type nlm4_stats ::
          :granted
          | :denied
          | :denied_nolocks
          | :blocked
          | :denied_grace_period
          | :deadlck
          | :rofs
          | :stale_fh
          | :fbig
          | :failed

  @type nlm4_holder :: %{
          exclusive: boolean(),
          svid: integer(),
          oh: binary(),
          offset: non_neg_integer(),
          length: non_neg_integer()
        }

  @type nlm4_lock :: %{
          caller_name: String.t(),
          fh: binary(),
          oh: binary(),
          svid: integer(),
          offset: non_neg_integer(),
          length: non_neg_integer()
        }

  @type nlm4_lockargs :: %{
          cookie: binary(),
          block: boolean(),
          exclusive: boolean(),
          lock: nlm4_lock(),
          reclaim: boolean(),
          state: integer()
        }

  @type nlm4_testargs :: %{
          cookie: binary(),
          exclusive: boolean(),
          lock: nlm4_lock()
        }

  @type nlm4_unlockargs :: %{
          cookie: binary(),
          lock: nlm4_lock()
        }

  @type nlm4_cancargs :: %{
          cookie: binary(),
          block: boolean(),
          exclusive: boolean(),
          lock: nlm4_lock()
        }

  @type nlm4_notify :: %{
          name: String.t(),
          state: integer()
        }

  ## Decoders

  @doc """
  Decodes NLM4_LOCK arguments.
  """
  @spec decode_lockargs(binary()) :: {:ok, nlm4_lockargs()} | {:error, atom()}
  def decode_lockargs(data) do
    with {:ok, cookie, rest} <- XDR.decode_opaque(data),
         {:ok, block, rest} <- XDR.decode_bool(rest),
         {:ok, exclusive, rest} <- XDR.decode_bool(rest),
         {:ok, lock, rest} <- decode_lock(rest),
         {:ok, reclaim, rest} <- XDR.decode_bool(rest),
         {:ok, state, _rest} <- XDR.decode_int(rest) do
      {:ok,
       %{
         cookie: cookie,
         block: block,
         exclusive: exclusive,
         lock: lock,
         reclaim: reclaim,
         state: state
       }}
    end
  end

  @doc """
  Decodes NLM4_TEST arguments.
  """
  @spec decode_testargs(binary()) :: {:ok, nlm4_testargs()} | {:error, atom()}
  def decode_testargs(data) do
    with {:ok, cookie, rest} <- XDR.decode_opaque(data),
         {:ok, exclusive, rest} <- XDR.decode_bool(rest),
         {:ok, lock, _rest} <- decode_lock(rest) do
      {:ok, %{cookie: cookie, exclusive: exclusive, lock: lock}}
    end
  end

  @doc """
  Decodes NLM4_UNLOCK arguments.
  """
  @spec decode_unlockargs(binary()) :: {:ok, nlm4_unlockargs()} | {:error, atom()}
  def decode_unlockargs(data) do
    with {:ok, cookie, rest} <- XDR.decode_opaque(data),
         {:ok, lock, _rest} <- decode_lock(rest) do
      {:ok, %{cookie: cookie, lock: lock}}
    end
  end

  @doc """
  Decodes NLM4_CANCEL arguments.
  """
  @spec decode_cancargs(binary()) :: {:ok, nlm4_cancargs()} | {:error, atom()}
  def decode_cancargs(data) do
    with {:ok, cookie, rest} <- XDR.decode_opaque(data),
         {:ok, block, rest} <- XDR.decode_bool(rest),
         {:ok, exclusive, rest} <- XDR.decode_bool(rest),
         {:ok, lock, _rest} <- decode_lock(rest) do
      {:ok, %{cookie: cookie, block: block, exclusive: exclusive, lock: lock}}
    end
  end

  @doc """
  Decodes NLM4_FREE_ALL (nlm4_notify) arguments.
  """
  @spec decode_notify(binary()) :: {:ok, nlm4_notify()} | {:error, atom()}
  def decode_notify(data) do
    with {:ok, name, rest} <- XDR.decode_string(data),
         {:ok, state, _rest} <- XDR.decode_int(rest) do
      {:ok, %{name: name, state: state}}
    end
  end

  ## Encoders

  @doc """
  Encodes an nlm4_res (simple result with cookie + stat).
  """
  @spec encode_res(binary(), nlm4_stats()) :: binary()
  def encode_res(cookie, stat) do
    XDR.encode_opaque(cookie) <> XDR.encode_int(stat_to_int(stat))
  end

  @doc """
  Encodes an nlm4_testres (test result with optional holder).
  """
  @spec encode_testres(binary(), :granted | {:denied, nlm4_holder()}) :: binary()
  def encode_testres(cookie, :granted) do
    XDR.encode_opaque(cookie) <> encode_test_reply(:granted)
  end

  def encode_testres(cookie, {:denied, holder}) do
    XDR.encode_opaque(cookie) <> encode_test_reply({:denied, holder})
  end

  ## Internal

  @doc false
  @spec decode_lock(binary()) :: {:ok, nlm4_lock(), binary()} | {:error, atom()}
  def decode_lock(data) do
    with {:ok, caller_name, rest} <- XDR.decode_string(data),
         {:ok, fh, rest} <- XDR.decode_opaque(rest),
         {:ok, oh, rest} <- XDR.decode_opaque(rest),
         {:ok, svid, rest} <- XDR.decode_int(rest),
         {:ok, offset, rest} <- XDR.decode_hyper_uint(rest),
         {:ok, length, rest} <- XDR.decode_hyper_uint(rest) do
      {:ok,
       %{
         caller_name: caller_name,
         fh: fh,
         oh: oh,
         svid: svid,
         offset: offset,
         length: length
       }, rest}
    end
  end

  defp encode_test_reply(:granted) do
    # Union discriminant: NLM4_GRANTED (0) + void arm
    XDR.encode_int(0)
  end

  defp encode_test_reply({:denied, holder}) do
    # Union discriminant: NLM4_DENIED (1) + holder
    XDR.encode_int(1) <> encode_holder(holder)
  end

  defp encode_holder(holder) do
    XDR.encode_bool(holder.exclusive) <>
      XDR.encode_int(holder.svid) <>
      XDR.encode_opaque(holder.oh) <>
      XDR.encode_hyper_uint(holder.offset) <>
      XDR.encode_hyper_uint(holder.length)
  end

  @doc false
  @spec stat_to_int(nlm4_stats()) :: integer()
  def stat_to_int(:granted), do: 0
  def stat_to_int(:denied), do: 1
  def stat_to_int(:denied_nolocks), do: 2
  def stat_to_int(:blocked), do: 3
  def stat_to_int(:denied_grace_period), do: 4
  def stat_to_int(:deadlck), do: 5
  def stat_to_int(:rofs), do: 6
  def stat_to_int(:stale_fh), do: 7
  def stat_to_int(:fbig), do: 8
  def stat_to_int(:failed), do: 9
end
