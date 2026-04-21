defmodule NFSServer.RPC.Auth do
  @moduledoc """
  ONC RPC authentication flavors — [RFC 5531 §8](https://www.rfc-editor.org/rfc/rfc5531#section-8).

  This module implements the two flavors needed for NFSv3:

  * `AUTH_NONE` (flavor 0) — no credentials, no verifier.
  * `AUTH_SYS` (flavor 1) — Unix-style identity: stamp, hostname,
    uid, gid, supplementary gid list. Surfaced to handlers as the
    `t:sys/0` struct so downstream code can apply POSIX permission
    checks.

  RPCSEC_GSS / Kerberos is intentionally out of scope.

  Each `decode_*/1` consumes a `<<flavor::32, body_var_opaque>>` pair
  off the wire. `encode_*` flips the same shape back out. Errors bubble
  up as `{:error, reason}` tuples so the dispatcher can return
  `AUTH_BADCRED` to the client.
  """

  alias NFSServer.XDR

  @auth_none 0
  @auth_sys 1

  defmodule None do
    @moduledoc "Marker for AUTH_NONE — no credential body."
    defstruct []
    @type t :: %__MODULE__{}
  end

  defmodule Sys do
    @moduledoc """
    Decoded `AUTH_SYS` credentials. Field names mirror the kernel's
    `authsys_parms` struct so handlers reading the auth context map
    1:1 to POSIX semantics.
    """
    defstruct stamp: 0,
              machinename: "",
              uid: 0,
              gid: 0,
              gids: []

    @type t :: %__MODULE__{
            stamp: non_neg_integer(),
            machinename: String.t(),
            uid: non_neg_integer(),
            gid: non_neg_integer(),
            gids: [non_neg_integer()]
          }
  end

  @typedoc "Decoded credential, per the supported flavors."
  @type credential :: None.t() | Sys.t()

  @doc "Numeric flavor for `AUTH_NONE`."
  @spec auth_none() :: 0
  def auth_none, do: @auth_none

  @doc "Numeric flavor for `AUTH_SYS`."
  @spec auth_sys() :: 1
  def auth_sys, do: @auth_sys

  @doc """
  Decode an `opaque_auth` (flavor + variable opaque body) into a
  `credential` struct. Used for both the call's credential and
  verifier slots.
  """
  @spec decode_opaque_auth(binary()) :: {:ok, credential(), binary()} | {:error, term()}
  def decode_opaque_auth(binary) do
    with {:ok, flavor, rest} <- XDR.decode_uint(binary),
         {:ok, body, rest2} <- XDR.decode_var_opaque(rest) do
      decode_body(flavor, body, rest2)
    end
  end

  defp decode_body(@auth_none, <<>>, rest), do: {:ok, %None{}, rest}

  defp decode_body(@auth_none, _other, _rest),
    do: {:error, {:auth_badcred, :auth_none_with_body}}

  defp decode_body(@auth_sys, body, rest) do
    case decode_authsys_body(body) do
      {:ok, sys} -> {:ok, sys, rest}
      {:error, _} = err -> err
    end
  end

  defp decode_body(flavor, _body, _rest), do: {:error, {:auth_badcred, {:unsupported, flavor}}}

  defp decode_authsys_body(body) do
    with {:ok, stamp, r1} <- XDR.decode_uint(body),
         {:ok, machinename, r2} <- XDR.decode_string(r1),
         {:ok, uid, r3} <- XDR.decode_uint(r2),
         {:ok, gid, r4} <- XDR.decode_uint(r3),
         {:ok, gids, _} <- XDR.decode_var_array(r4, &XDR.decode_uint/1) do
      {:ok,
       %Sys{
         stamp: stamp,
         machinename: machinename,
         uid: uid,
         gid: gid,
         gids: gids
       }}
    end
  end

  @doc """
  Encode an `opaque_auth` slot. The verifier on a successful reply is
  almost always `AUTH_NONE`; only RPCSEC_GSS uses anything else.
  """
  @spec encode_opaque_auth(credential()) :: binary()
  def encode_opaque_auth(%None{}) do
    XDR.encode_uint(@auth_none) <> XDR.encode_var_opaque(<<>>)
  end

  def encode_opaque_auth(%Sys{} = sys) do
    body =
      XDR.encode_uint(sys.stamp) <>
        XDR.encode_string(sys.machinename) <>
        XDR.encode_uint(sys.uid) <>
        XDR.encode_uint(sys.gid) <>
        XDR.encode_var_array(sys.gids, &XDR.encode_uint/1)

    XDR.encode_uint(@auth_sys) <> XDR.encode_var_opaque(body)
  end
end
