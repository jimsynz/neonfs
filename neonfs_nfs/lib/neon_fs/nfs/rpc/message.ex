defmodule NeonFS.NFS.RPC.Message do
  @moduledoc """
  ONC RPC v2 message parsing and building per RFC 5531.

  Handles RPC call/reply message framing, authentication extraction,
  and program/version/procedure dispatch routing.
  """

  alias NeonFS.NFS.RPC.XDR

  @rpc_version 2

  @type auth_flavor :: :auth_none | :auth_sys
  @type auth_sys :: %{
          stamp: non_neg_integer(),
          machine_name: String.t(),
          uid: non_neg_integer(),
          gid: non_neg_integer(),
          gids: [non_neg_integer()]
        }

  @type rpc_call :: %{
          xid: non_neg_integer(),
          program: non_neg_integer(),
          version: non_neg_integer(),
          procedure: non_neg_integer(),
          cred_flavor: auth_flavor(),
          cred: auth_sys() | nil,
          verf_flavor: auth_flavor(),
          body: binary()
        }

  @type accept_stat ::
          :success
          | :prog_unavail
          | :prog_mismatch
          | :proc_unavail
          | :garbage_args
          | :system_err

  @doc """
  Decodes an ONC RPC call message from binary data.
  """
  @spec decode_call(binary()) :: {:ok, rpc_call()} | {:error, atom()}
  def decode_call(data) do
    with {:ok, xid, rest} <- XDR.decode_uint(data),
         {:ok, msg_type, rest} <- XDR.decode_uint(rest),
         :ok <- validate_msg_type(msg_type),
         {:ok, rpc_vers, rest} <- XDR.decode_uint(rest),
         :ok <- validate_rpc_version(rpc_vers),
         {:ok, program, rest} <- XDR.decode_uint(rest),
         {:ok, version, rest} <- XDR.decode_uint(rest),
         {:ok, procedure, rest} <- XDR.decode_uint(rest),
         {:ok, cred_flavor, cred, rest} <- decode_auth(rest),
         {:ok, verf_flavor, _verf, body} <- decode_auth(rest: rest) do
      {:ok,
       %{
         xid: xid,
         program: program,
         version: version,
         procedure: procedure,
         cred_flavor: cred_flavor,
         cred: cred,
         verf_flavor: verf_flavor,
         body: body
       }}
    end
  end

  @doc """
  Builds an RPC accepted reply (success) with the given body.
  """
  @spec encode_accepted_reply(non_neg_integer(), accept_stat(), binary()) :: binary()
  def encode_accepted_reply(xid, stat, body \\ <<>>) do
    <<
      xid::big-unsigned-32,
      # reply
      1::big-unsigned-32,
      # accepted
      0::big-unsigned-32,
      # verf: AUTH_NONE
      0::big-unsigned-32,
      0::big-unsigned-32,
      accept_stat_value(stat)::big-unsigned-32,
      body::binary
    >>
  end

  @doc """
  Builds an RPC rejected reply (auth error).
  """
  @spec encode_rejected_reply(non_neg_integer(), non_neg_integer()) :: binary()
  def encode_rejected_reply(xid, auth_stat) do
    <<
      xid::big-unsigned-32,
      # reply
      1::big-unsigned-32,
      # rejected - auth error
      1::big-unsigned-32,
      # AUTH_ERROR
      1::big-unsigned-32,
      auth_stat::big-unsigned-32
    >>
  end

  @doc """
  Builds a PROG_MISMATCH reply with supported version range.
  """
  @spec encode_prog_mismatch(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: binary()
  def encode_prog_mismatch(xid, low, high) do
    <<
      xid::big-unsigned-32,
      # reply
      1::big-unsigned-32,
      # accepted
      0::big-unsigned-32,
      # verf: AUTH_NONE
      0::big-unsigned-32,
      0::big-unsigned-32,
      # PROG_MISMATCH
      2::big-unsigned-32,
      low::big-unsigned-32,
      high::big-unsigned-32
    >>
  end

  ## Private

  defp validate_msg_type(0), do: :ok
  defp validate_msg_type(_), do: {:error, :not_a_call}

  defp validate_rpc_version(@rpc_version), do: :ok
  defp validate_rpc_version(_), do: {:error, :rpc_mismatch}

  defp decode_auth(rest: data), do: decode_auth(data)

  defp decode_auth(data) do
    with {:ok, flavor, rest} <- XDR.decode_uint(data),
         {:ok, body, rest} <- XDR.decode_opaque(rest) do
      auth = decode_auth_body(flavor, body)
      {:ok, auth_flavor_name(flavor), auth, rest}
    end
  end

  defp auth_flavor_name(0), do: :auth_none
  defp auth_flavor_name(1), do: :auth_sys
  defp auth_flavor_name(n), do: {:unknown, n}

  defp decode_auth_body(1, body) do
    with {:ok, stamp, rest} <- XDR.decode_uint(body),
         {:ok, machine_name, rest} <- XDR.decode_string(rest),
         {:ok, uid, rest} <- XDR.decode_uint(rest),
         {:ok, gid, rest} <- XDR.decode_uint(rest),
         {:ok, gids, _rest} <- decode_gid_list(rest) do
      %{stamp: stamp, machine_name: machine_name, uid: uid, gid: gid, gids: gids}
    else
      _ -> nil
    end
  end

  defp decode_auth_body(_, _body), do: nil

  defp decode_gid_list(data) do
    with {:ok, count, rest} <- XDR.decode_uint(data) do
      decode_n_uints(rest, count, [])
    end
  end

  defp decode_n_uints(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_n_uints(data, n, acc) do
    with {:ok, val, rest} <- XDR.decode_uint(data) do
      decode_n_uints(rest, n - 1, [val | acc])
    end
  end

  defp accept_stat_value(:success), do: 0
  defp accept_stat_value(:prog_unavail), do: 1
  defp accept_stat_value(:prog_mismatch), do: 2
  defp accept_stat_value(:proc_unavail), do: 3
  defp accept_stat_value(:garbage_args), do: 4
  defp accept_stat_value(:system_err), do: 5
end
