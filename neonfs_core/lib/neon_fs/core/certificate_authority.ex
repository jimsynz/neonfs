defmodule NeonFS.Core.CertificateAuthority do
  @moduledoc """
  Manages the cluster CA lifecycle via the system volume.

  Bridges the pure crypto functions in `NeonFS.Transport.TLS` and the
  system volume where CA state is persisted. Handles CA initialisation,
  node certificate signing, serial number allocation, certificate
  revocation, and CA info queries.

  Serial number allocation is serialised by the caller — during cluster
  init there is only one caller, and during node joins the handler
  GenServer provides the single-caller guarantee.
  """

  alias NeonFS.Core.SystemVolume
  alias NeonFS.Transport.TLS
  alias X509.CRL.Entry, as: CRLEntry
  alias X509.CRL.Extension, as: CRLExtension

  @type revocation_reason ::
          :unspecified | :key_compromise | :superseded | :cessation_of_operation

  ## Public API

  @doc """
  Returns information about the cluster CA.

  Reads the CA certificate and current serial counter from the system volume.
  Returns a map with subject, algorithm, validity dates, current serial
  (last allocated), and approximate node count (derived from the serial counter).
  """
  @spec ca_info() :: {:ok, map()} | {:error, term()}
  def ca_info do
    with {:ok, ca_cert_pem} <- SystemVolume.read("/tls/ca.crt"),
         {:ok, serial_content} <- SystemVolume.read("/tls/serial") do
      cert_info = TLS.certificate_info(ca_cert_pem)
      next = serial_content |> String.trim() |> String.to_integer()

      {:ok,
       %{
         subject: cert_info.subject,
         algorithm: "ECDSA P-256",
         valid_from: cert_info.not_before,
         valid_to: cert_info.not_after,
         current_serial: next - 1,
         nodes_issued: next - 1
       }}
    end
  end

  @doc """
  Reads and returns the current CRL PEM from the system volume.
  """
  @spec get_crl() :: {:ok, TLS.pem()} | {:error, term()}
  def get_crl do
    SystemVolume.read("/tls/crl.pem")
  end

  @doc """
  Initialises the cluster CA and writes materials to the system volume.

  Generates a self-signed CA certificate and key via `Transport.TLS.generate_ca/1`,
  then writes them along with an initial serial counter (set to `1`) and an empty
  CRL to the system volume under `/tls/`.

  Returns `{:ok, ca_cert, ca_key}` on success.
  """
  @spec init_ca(String.t()) :: {:ok, TLS.cert(), TLS.key()} | {:error, term()}
  def init_ca(cluster_name) do
    {ca_cert, ca_key} = TLS.generate_ca(cluster_name)

    with :ok <- SystemVolume.write("/tls/ca.crt", TLS.encode_cert(ca_cert)),
         :ok <- SystemVolume.write("/tls/ca.key", TLS.encode_key(ca_key)),
         :ok <- SystemVolume.write("/tls/serial", "1"),
         :ok <- SystemVolume.write("/tls/crl.pem", TLS.create_empty_crl(ca_cert, ca_key)) do
      {:ok, ca_cert, ca_key}
    end
  end

  @doc """
  Checks whether a certificate serial number has been revoked.
  """
  @spec is_revoked?(non_neg_integer()) :: {:ok, boolean()} | {:error, term()}
  def is_revoked?(serial) when is_integer(serial) do
    with {:ok, crl_pem} <- SystemVolume.read("/tls/crl.pem") do
      revoked =
        crl_pem
        |> TLS.parse_crl_entries()
        |> Enum.any?(fn entry -> CRLEntry.serial(entry) == serial end)

      {:ok, revoked}
    end
  end

  @doc """
  Lists metadata for all certificates issued by the CA.

  Returns a list of maps with `:node_name`, `:hostname`, `:serial`,
  `:not_after`, and `:revoked` (boolean). Reads issued cert metadata
  from `/tls/issued/` in the system volume.
  """
  @spec list_issued() :: {:ok, [map()]} | {:error, term()}
  def list_issued do
    with {:ok, entries} <- SystemVolume.list("/tls/issued"),
         {:ok, crl_pem} <- SystemVolume.read("/tls/crl.pem") do
      revoked_serials = revoked_serial_set(crl_pem)

      certs =
        entries
        |> Enum.map(&read_cert_metadata/1)
        |> Enum.reject(&is_nil/1)
        |> Enum.map(fn meta ->
          Map.put(meta, :revoked, MapSet.member?(revoked_serials, meta.serial))
        end)
        |> Enum.sort_by(& &1.serial)

      {:ok, certs}
    else
      # /tls/issued/ doesn't exist yet (no certs issued)
      {:error, :file_not_found} -> {:ok, []}
      {:error, _} = error -> error
    end
  end

  @doc """
  Returns a list of all revoked certificates from the CRL.

  Each entry is a map with `:serial`, `:revoked_at`, and `:reason`.
  """
  @spec list_revoked() :: {:ok, [map()]} | {:error, term()}
  def list_revoked do
    with {:ok, crl_pem} <- SystemVolume.read("/tls/crl.pem") do
      entries =
        crl_pem
        |> TLS.parse_crl_entries()
        |> Enum.map(fn entry ->
          %{
            serial: CRLEntry.serial(entry),
            revoked_at: CRLEntry.revocation_date(entry),
            reason: extract_reason(entry)
          }
        end)

      {:ok, entries}
    end
  end

  @doc """
  Allocates the next serial number from the system volume.

  Reads `/tls/serial`, returns the current value, and writes back the
  incremented value. This operation is NOT concurrency-safe on its own —
  callers must ensure serialisation (e.g., via a GenServer or single-caller
  guarantee).
  """
  @spec next_serial() :: {:ok, non_neg_integer()} | {:error, term()}
  def next_serial do
    with {:ok, content} <- SystemVolume.read("/tls/serial") do
      serial = content |> String.trim() |> String.to_integer()
      :ok = SystemVolume.write("/tls/serial", Integer.to_string(serial + 1))
      {:ok, serial}
    end
  end

  @doc """
  Revokes a certificate by adding it to the CRL stored in the system volume.

  Accepts either a certificate struct or a serial number. The reason defaults
  to `:unspecified`. Idempotent — revoking an already-revoked certificate
  returns `:ok`.

  ## Supported reasons
  - `:unspecified` — no specific reason (no reason code extension added)
  - `:key_compromise` — the certificate's private key was compromised
  - `:superseded` — the certificate has been replaced
  - `:cessation_of_operation` — the node has been decommissioned
  """
  @spec revoke_certificate(TLS.cert() | non_neg_integer(), revocation_reason()) ::
          :ok | {:error, term()}
  def revoke_certificate(cert_or_serial, reason \\ :unspecified) do
    serial = resolve_serial(cert_or_serial)

    with {:ok, ca_cert_pem} <- SystemVolume.read("/tls/ca.crt"),
         {:ok, ca_key_pem} <- SystemVolume.read("/tls/ca.key"),
         {:ok, crl_pem} <- SystemVolume.read("/tls/crl.pem") do
      ca_cert = TLS.decode_cert!(ca_cert_pem)
      ca_key = TLS.decode_key!(ca_key_pem)
      existing_entries = TLS.parse_crl_entries(crl_pem)

      if already_revoked?(existing_entries, serial) do
        :ok
      else
        extensions = reason_extensions(reason)
        entry = CRLEntry.new(serial, DateTime.utc_now(), extensions)

        new_crl_pem =
          X509.CRL.new([entry | existing_entries], ca_cert, ca_key) |> X509.CRL.to_pem()

        SystemVolume.write("/tls/crl.pem", new_crl_pem)
      end
    end
  end

  @doc """
  Signs a node's CSR, producing a certificate signed by the cluster CA.

  Reads the CA certificate and key from the system volume, allocates the
  next serial number, and signs the CSR. Returns the signed node certificate
  and the CA certificate (for the node to cache locally).

  Also stores cert metadata under `/tls/issued/<serial>` for tracking.

  The CSR signature is validated before signing. Returns `{:error, :invalid_csr}`
  if the CSR signature is invalid.
  """
  @spec sign_node_csr(TLS.csr(), String.t()) :: {:ok, TLS.cert(), TLS.cert()} | {:error, term()}
  def sign_node_csr(csr, hostname) do
    if TLS.validate_csr(csr) do
      do_sign_node_csr(csr, hostname)
    else
      {:error, :invalid_csr}
    end
  end

  ## Private

  defp already_revoked?(entries, serial) do
    Enum.any?(entries, fn entry -> CRLEntry.serial(entry) == serial end)
  end

  defp do_sign_node_csr(csr, hostname) do
    with {:ok, ca_cert_pem} <- SystemVolume.read("/tls/ca.crt"),
         {:ok, ca_key_pem} <- SystemVolume.read("/tls/ca.key"),
         {:ok, serial} <- next_serial() do
      ca_cert = TLS.decode_cert!(ca_cert_pem)
      ca_key = TLS.decode_key!(ca_key_pem)
      node_cert = TLS.sign_csr(csr, hostname, ca_cert, ca_key, serial)

      node_name = csr |> X509.CSR.subject() |> X509.RDNSequence.to_string()
      cert_info = TLS.certificate_info(node_cert)
      store_cert_metadata(serial, node_name, hostname, cert_info.not_after)

      {:ok, node_cert, ca_cert}
    end
  end

  defp extract_reason(entry) do
    # NOTE: x509 type spec says :crl_reason but find/2 matches :reason_code
    case CRLEntry.extension(entry, :reason_code) do
      nil ->
        :unspecified

      {:Extension, _oid, _critical, der_value} ->
        case :public_key.der_decode(:CRLReason, der_value) do
          :keyCompromise -> :key_compromise
          :superseded -> :superseded
          :cessationOfOperation -> :cessation_of_operation
          other -> other
        end
    end
  end

  defp read_cert_metadata(entry_name) do
    path = "/tls/issued/#{entry_name}"

    case SystemVolume.read(path) do
      {:ok, data} ->
        :erlang.binary_to_term(data)

      {:error, _} ->
        nil
    end
  rescue
    _ -> nil
  end

  defp reason_extensions(:unspecified), do: []

  defp reason_extensions(:key_compromise),
    do: [CRLExtension.reason_code(:keyCompromise)]

  defp reason_extensions(:superseded),
    do: [CRLExtension.reason_code(:superseded)]

  defp reason_extensions(:cessation_of_operation),
    do: [CRLExtension.reason_code(:cessationOfOperation)]

  defp resolve_serial(serial) when is_integer(serial), do: serial
  defp resolve_serial(cert), do: X509.Certificate.serial(cert)

  defp revoked_serial_set(crl_pem) do
    crl_pem
    |> TLS.parse_crl_entries()
    |> Enum.map(fn entry -> CRLEntry.serial(entry) end)
    |> MapSet.new()
  end

  defp store_cert_metadata(serial, node_name, hostname, not_after) do
    metadata = %{
      node_name: node_name,
      hostname: hostname,
      serial: serial,
      not_after: not_after
    }

    SystemVolume.write("/tls/issued/#{serial}", :erlang.term_to_binary(metadata))
  end
end
