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

  # Active (canonical) CA storage paths.
  @active_cert_path "/tls/ca.crt"
  @active_key_path "/tls/ca.key"
  @active_serial_path "/tls/serial"
  @active_crl_path "/tls/crl.pem"
  @active_issued_dir "/tls/issued"

  # Incoming (staged) CA storage paths — populated during the rotation
  # grace window by `init_incoming_ca/1` and consumed by
  # `finalize_rotation/0` (or discarded by `abort_rotation/0`).
  @incoming_cert_path "/tls/incoming/ca.crt"
  @incoming_key_path "/tls/incoming/ca.key"
  @incoming_serial_path "/tls/incoming/serial"
  @incoming_crl_path "/tls/incoming/crl.pem"
  @incoming_issued_dir "/tls/incoming/issued"

  ## Public API

  @doc """
  Returns information about the cluster CA.

  Reads the CA certificate and current serial counter from the system volume.
  Returns a map with subject, algorithm, validity dates, current serial
  (last allocated), and approximate node count (derived from the serial counter).
  """
  @spec ca_info() :: {:ok, map()} | {:error, term()}
  def ca_info do
    read_ca_info(@active_cert_path, @active_serial_path)
  end

  @doc """
  Returns information about the staged incoming CA, if one exists.

  Same shape as `ca_info/0` but reads `_system/tls/incoming/`. Returns
  `{:error, :no_incoming_ca}` when no incoming CA is staged.
  """
  @spec incoming_ca_info() :: {:ok, map()} | {:error, :no_incoming_ca | term()}
  def incoming_ca_info do
    case read_ca_info(@incoming_cert_path, @incoming_serial_path) do
      {:ok, _info} = ok -> ok
      {:error, %{class: :not_found}} -> {:error, :no_incoming_ca}
      {:error, _} = error -> error
    end
  end

  @doc """
  Aborts a staged CA rotation by discarding the incoming CA.

  Removes every file under `_system/tls/incoming/`. Idempotent — returns
  `:ok` when nothing is staged.
  """
  @spec abort_rotation() :: :ok | {:error, term()}
  def abort_rotation do
    delete_incoming_tree()
  end

  @doc """
  Atomically promotes the staged incoming CA to active.

  Reads everything from `_system/tls/incoming/`, overwrites the active CA
  storage, then deletes the incoming tree. The previous active CA's
  cert/key/serial/CRL and issued metadata are discarded — every certificate
  signed by the old CA is implicitly invalidated by the rotation.

  Returns `{:error, :no_incoming_ca}` if no rotation is staged.
  """
  @spec finalize_rotation() :: :ok | {:error, :no_incoming_ca | term()}
  def finalize_rotation do
    with {:ok, ca_cert_pem} <- read_incoming(@incoming_cert_path),
         {:ok, ca_key_pem} <- SystemVolume.read(@incoming_key_path),
         {:ok, serial_pem} <- SystemVolume.read(@incoming_serial_path),
         {:ok, crl_pem} <- SystemVolume.read(@incoming_crl_path),
         {:ok, issued} <- read_incoming_issued(),
         :ok <- delete_active_issued(),
         :ok <- SystemVolume.write(@active_cert_path, ca_cert_pem),
         :ok <- SystemVolume.write(@active_key_path, ca_key_pem),
         :ok <- SystemVolume.write(@active_serial_path, serial_pem),
         :ok <- SystemVolume.write(@active_crl_path, crl_pem),
         :ok <- write_active_issued(issued) do
      delete_incoming_tree()
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
  Stages an incoming CA in preparation for a `cluster ca rotate`.

  Generates a fresh self-signed CA via `Transport.TLS.generate_ca/1` and
  writes it under `_system/tls/incoming/` along with an initial serial
  counter (`1`) and an empty CRL.

  Refuses if an incoming CA is already staged — operators must call
  `abort_rotation/0` first to discard the previous staging attempt.
  """
  @spec init_incoming_ca(String.t()) ::
          {:ok, TLS.cert(), TLS.key()} | {:error, :incoming_ca_already_staged | term()}
  def init_incoming_ca(cluster_name) do
    if SystemVolume.exists?(@incoming_cert_path) do
      {:error, :incoming_ca_already_staged}
    else
      {ca_cert, ca_key} = TLS.generate_ca(cluster_name)

      with :ok <- SystemVolume.write(@incoming_cert_path, TLS.encode_cert(ca_cert)),
           :ok <- SystemVolume.write(@incoming_key_path, TLS.encode_key(ca_key)),
           :ok <- SystemVolume.write(@incoming_serial_path, "1"),
           :ok <- SystemVolume.write(@incoming_crl_path, TLS.create_empty_crl(ca_cert, ca_key)) do
        {:ok, ca_cert, ca_key}
      end
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
      {:error, %{class: :not_found}} -> {:ok, []}
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
    allocate_serial(@active_serial_path)
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
      do_sign_node_csr(csr, hostname,
        cert_path: @active_cert_path,
        key_path: @active_key_path,
        serial_path: @active_serial_path,
        issued_dir: @active_issued_dir
      )
    else
      {:error, :invalid_csr}
    end
  end

  @doc """
  Signs a node's CSR against the staged incoming CA.

  Same shape as `sign_node_csr/2`, but uses the incoming CA's key for
  signing and allocates the serial from the incoming serial counter.
  Issued cert metadata is stored under `_system/tls/incoming/issued/<serial>`
  and is moved to the active issued directory by `finalize_rotation/0`.

  Returns `{:error, :no_incoming_ca}` if no rotation is staged.
  """
  @spec sign_node_csr_with_incoming(TLS.csr(), String.t()) ::
          {:ok, TLS.cert(), TLS.cert()} | {:error, :no_incoming_ca | :invalid_csr | term()}
  def sign_node_csr_with_incoming(csr, hostname) do
    cond do
      not SystemVolume.exists?(@incoming_cert_path) ->
        {:error, :no_incoming_ca}

      not TLS.validate_csr(csr) ->
        {:error, :invalid_csr}

      true ->
        do_sign_node_csr(csr, hostname,
          cert_path: @incoming_cert_path,
          key_path: @incoming_key_path,
          serial_path: @incoming_serial_path,
          issued_dir: @incoming_issued_dir
        )
    end
  end

  ## Private

  defp already_revoked?(entries, serial) do
    Enum.any?(entries, fn entry -> CRLEntry.serial(entry) == serial end)
  end

  defp allocate_serial(serial_path) do
    with {:ok, content} <- SystemVolume.read(serial_path) do
      serial = content |> String.trim() |> String.to_integer()
      :ok = SystemVolume.write(serial_path, Integer.to_string(serial + 1))
      {:ok, serial}
    end
  end

  defp delete_active_issued do
    case SystemVolume.list(@active_issued_dir) do
      {:ok, entries} ->
        Enum.each(entries, fn name ->
          SystemVolume.delete("#{@active_issued_dir}/#{name}")
        end)

        :ok

      {:error, _} = error ->
        error
    end
  end

  defp delete_incoming_tree do
    delete_dir_entries(@incoming_issued_dir)

    Enum.each(
      [@incoming_cert_path, @incoming_key_path, @incoming_serial_path, @incoming_crl_path],
      fn path ->
        if SystemVolume.exists?(path), do: SystemVolume.delete(path)
      end
    )

    :ok
  end

  defp delete_dir_entries(dir) do
    case SystemVolume.list(dir) do
      {:ok, entries} ->
        Enum.each(entries, fn name -> SystemVolume.delete("#{dir}/#{name}") end)

      {:error, _} ->
        :ok
    end
  end

  defp do_sign_node_csr(csr, hostname, opts) do
    cert_path = Keyword.fetch!(opts, :cert_path)
    key_path = Keyword.fetch!(opts, :key_path)
    serial_path = Keyword.fetch!(opts, :serial_path)
    issued_dir = Keyword.fetch!(opts, :issued_dir)

    with {:ok, ca_cert_pem} <- SystemVolume.read(cert_path),
         {:ok, ca_key_pem} <- SystemVolume.read(key_path),
         {:ok, serial} <- allocate_serial(serial_path) do
      ca_cert = TLS.decode_cert!(ca_cert_pem)
      ca_key = TLS.decode_key!(ca_key_pem)
      node_cert = TLS.sign_csr(csr, hostname, ca_cert, ca_key, serial)

      node_name = csr |> X509.CSR.subject() |> X509.RDNSequence.to_string()
      cert_info = TLS.certificate_info(node_cert)
      store_cert_metadata(issued_dir, serial, node_name, hostname, cert_info.not_after)

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

  defp read_ca_info(cert_path, serial_path) do
    with {:ok, ca_cert_pem} <- SystemVolume.read(cert_path),
         {:ok, serial_content} <- SystemVolume.read(serial_path) do
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

  defp read_cert_metadata(entry_name) do
    read_cert_metadata_from(@active_issued_dir, entry_name)
  end

  defp read_cert_metadata_from(dir, entry_name) do
    path = "#{dir}/#{entry_name}"

    case SystemVolume.read(path) do
      {:ok, data} ->
        :erlang.binary_to_term(data)

      {:error, _} ->
        nil
    end
  rescue
    _ -> nil
  end

  defp read_incoming(path) do
    case SystemVolume.read(path) do
      {:ok, _} = ok -> ok
      {:error, %{class: :not_found}} -> {:error, :no_incoming_ca}
      {:error, _} = error -> error
    end
  end

  defp read_incoming_issued do
    case SystemVolume.list(@incoming_issued_dir) do
      {:ok, entries} ->
        issued =
          entries
          |> Enum.map(fn name -> {name, read_cert_metadata_from(@incoming_issued_dir, name)} end)
          |> Enum.reject(fn {_, meta} -> is_nil(meta) end)

        {:ok, issued}

      {:error, _} = error ->
        error
    end
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

  defp store_cert_metadata(issued_dir, serial, node_name, hostname, not_after) do
    metadata = %{
      node_name: node_name,
      hostname: hostname,
      serial: serial,
      not_after: not_after
    }

    SystemVolume.write("#{issued_dir}/#{serial}", :erlang.term_to_binary(metadata))
  end

  defp write_active_issued(issued) do
    Enum.reduce_while(issued, :ok, fn {name, metadata}, _acc ->
      case SystemVolume.write("#{@active_issued_dir}/#{name}", :erlang.term_to_binary(metadata)) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end
