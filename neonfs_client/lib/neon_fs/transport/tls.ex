defmodule NeonFS.Transport.TLS do
  @moduledoc """
  Pure cryptographic functions for CA and certificate operations.

  All functions are pure — no SystemVolume dependency, no RPC. Higher-level
  orchestration that reads/writes the system volume is handled by
  `NeonFS.Core.CertificateAuthority`.
  """

  alias X509.Certificate.Extension
  alias X509.CRL.Entry, as: CRLEntry

  @type pem :: binary()
  @type cert :: X509.Certificate.t()
  @type key :: X509.PrivateKey.t()
  @type csr :: X509.CSR.t()

  @type cert_info :: %{
          subject: String.t(),
          serial: non_neg_integer(),
          not_before: DateTime.t(),
          not_after: DateTime.t(),
          issuer: String.t()
        }

  @doc """
  Generates a self-signed CA certificate and private key.

  Returns `{ca_cert, ca_key}` with an ECDSA P-256 key, `:root_ca` template,
  and subject `/O=NeonFS/CN=<cluster_name> CA`.
  """
  @spec generate_ca(String.t()) :: {cert(), key()}
  def generate_ca(cluster_name) do
    ca_key = X509.PrivateKey.new_ec(:secp256r1)

    ca_cert =
      X509.Certificate.self_signed(ca_key, "/O=NeonFS/CN=#{cluster_name} CA",
        template: :root_ca,
        validity: ca_validity_days()
      )

    {ca_cert, ca_key}
  end

  @doc """
  Generates a new ECDSA P-256 private key for a node.
  """
  @spec generate_node_key() :: key()
  def generate_node_key do
    X509.PrivateKey.new_ec(:secp256r1)
  end

  @doc """
  Creates a Certificate Signing Request for a node.

  Subject is `/O=NeonFS/CN=<node_name>`.
  """
  @spec create_csr(key(), String.t()) :: csr()
  def create_csr(private_key, node_name) do
    X509.CSR.new(private_key, "/O=NeonFS/CN=#{node_name}")
  end

  @doc """
  Signs a CSR, producing a node certificate.

  Uses the `:server` template with SAN for the hostname and
  ext_key_usage `[:serverAuth, :clientAuth]`.
  """
  @spec sign_csr(csr(), String.t(), cert(), key(), non_neg_integer()) :: cert()
  def sign_csr(csr, hostname, ca_cert, ca_key, serial) do
    X509.Certificate.new(
      X509.CSR.public_key(csr),
      X509.CSR.subject(csr),
      ca_cert,
      ca_key,
      template: :server,
      serial: serial,
      validity: node_validity_days(),
      extensions: [
        subject_alt_name: Extension.subject_alt_name([hostname]),
        ext_key_usage: Extension.ext_key_usage([:serverAuth, :clientAuth])
      ]
    )
  end

  @doc """
  Checks whether the given value is a valid CSR tuple (structural check only).
  """
  @spec valid_csr_format?(term()) :: boolean()
  def valid_csr_format?({:CertificationRequest, _, _, _}), do: true
  def valid_csr_format?(_), do: false

  @doc """
  Validates a CSR's signature.
  """
  @spec validate_csr(csr()) :: boolean()
  def validate_csr(csr) do
    X509.CSR.valid?(csr)
  end

  @doc """
  Creates an empty CRL signed by the CA.
  """
  @spec create_empty_crl(cert(), key()) :: pem()
  def create_empty_crl(ca_cert, ca_key) do
    X509.CRL.new([], ca_cert, ca_key)
    |> X509.CRL.to_pem()
  end

  @doc """
  Adds a revocation entry to the CRL.

  Takes the certificate to revoke, existing CRL entries, CA cert, CA key,
  and optional CRL entry extensions (e.g., reason code).
  Returns the updated CRL as PEM.
  """
  @spec add_crl_entry(cert(), [CRLEntry.t()], cert(), key(), [X509.CRL.Extension.t()]) :: pem()
  def add_crl_entry(certificate_to_revoke, existing_entries, ca_cert, ca_key, extensions \\ []) do
    new_entry = CRLEntry.new(certificate_to_revoke, DateTime.utc_now(), extensions)
    X509.CRL.new([new_entry | existing_entries], ca_cert, ca_key) |> X509.CRL.to_pem()
  end

  @doc """
  Parses a CRL PEM and returns the list of revocation entries.
  """
  @spec parse_crl_entries(pem()) :: [CRLEntry.t()]
  def parse_crl_entries(crl_pem) do
    crl_pem
    |> X509.CRL.from_pem!()
    |> X509.CRL.list()
  end

  @doc """
  Extracts information from a certificate.

  Accepts either a decoded certificate or PEM binary.
  Returns a map with `:subject`, `:serial`, `:not_before`, `:not_after`, `:issuer`.
  """
  @spec certificate_info(cert() | pem()) :: cert_info()
  def certificate_info(cert_or_pem) do
    cert = maybe_decode_cert(cert_or_pem)

    {not_before, not_after} = validity_dates(cert)

    %{
      subject: cert |> X509.Certificate.subject() |> X509.RDNSequence.to_string(),
      serial: X509.Certificate.serial(cert),
      not_before: not_before,
      not_after: not_after,
      issuer: cert |> X509.Certificate.issuer() |> X509.RDNSequence.to_string()
    }
  end

  @doc """
  Returns the number of days until the certificate expires.

  Accepts either a decoded certificate or PEM binary.
  """
  @spec days_until_expiry(cert() | pem()) :: integer()
  def days_until_expiry(cert_or_pem) do
    cert = maybe_decode_cert(cert_or_pem)
    {_not_before, not_after} = validity_dates(cert)
    DateTime.diff(not_after, DateTime.utc_now(), :day)
  end

  @spec encode_cert(cert()) :: pem()
  def encode_cert(cert), do: X509.Certificate.to_pem(cert)

  @spec decode_cert!(pem()) :: cert()
  def decode_cert!(pem), do: X509.Certificate.from_pem!(pem)

  @spec encode_key(key()) :: pem()
  def encode_key(key), do: X509.PrivateKey.to_pem(key)

  @spec decode_key!(pem()) :: key()
  def decode_key!(pem), do: X509.PrivateKey.from_pem!(pem)

  @spec encode_csr(csr()) :: pem()
  def encode_csr(csr), do: X509.CSR.to_pem(csr)

  @spec decode_csr!(pem()) :: csr()
  def decode_csr!(pem), do: X509.CSR.from_pem!(pem)

  @doc """
  Returns the local TLS directory path.
  """
  @spec tls_dir() :: String.t()
  def tls_dir do
    Application.get_env(:neonfs_client, :tls_dir, "/var/lib/neonfs/tls")
  end

  @doc """
  Writes CA cert, node cert, and node key to the local TLS directory.

  Creates the directory if it doesn't exist. Sets file permissions:
  key file 0600, cert files 0644.
  """
  @spec write_local_tls(cert(), cert(), key()) :: :ok
  def write_local_tls(ca_cert, node_cert, node_key) do
    dir = tls_dir()
    File.mkdir_p!(dir)

    ca_path = Path.join(dir, "ca.crt")
    cert_path = Path.join(dir, "node.crt")
    key_path = Path.join(dir, "node.key")

    File.write!(ca_path, encode_cert(ca_cert))
    File.chmod!(ca_path, 0o644)

    File.write!(cert_path, encode_cert(node_cert))
    File.chmod!(cert_path, 0o644)

    File.write!(key_path, encode_key(node_key))
    File.chmod!(key_path, 0o600)

    :ok
  end

  @doc """
  Reads and decodes the local node certificate.
  """
  @spec read_local_cert() :: {:ok, cert()} | {:error, :not_found}
  def read_local_cert do
    path = Path.join(tls_dir(), "node.crt")

    case File.read(path) do
      {:ok, pem} -> {:ok, decode_cert!(pem)}
      {:error, :enoent} -> {:error, :not_found}
    end
  end

  @doc """
  Reads and decodes the local cached CA certificate.
  """
  @spec read_local_ca_cert() :: {:ok, cert()} | {:error, :not_found}
  def read_local_ca_cert do
    path = Path.join(tls_dir(), "ca.crt")

    case File.read(path) do
      {:ok, pem} -> {:ok, decode_cert!(pem)}
      {:error, :enoent} -> {:error, :not_found}
    end
  end

  defp maybe_decode_cert(pem) when is_binary(pem), do: decode_cert!(pem)
  defp maybe_decode_cert(cert), do: cert

  defp validity_dates(cert) do
    import X509.ASN1
    validity(notBefore: not_before, notAfter: not_after) = X509.Certificate.validity(cert)
    {X509.DateTime.to_datetime(not_before), X509.DateTime.to_datetime(not_after)}
  end

  @doc """
  Returns the renewal threshold in days (begin renewal this many days before expiry).
  """
  @spec renewal_threshold_days() :: non_neg_integer()
  def renewal_threshold_days do
    Application.get_env(:neonfs_client, :renewal_threshold_days, 30)
  end

  defp ca_validity_days do
    Application.get_env(:neonfs_client, :ca_validity_days, 3650)
  end

  defp node_validity_days do
    Application.get_env(:neonfs_client, :node_validity_days, 365)
  end
end
