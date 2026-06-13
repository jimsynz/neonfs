defmodule NeonFS.CLI.Handler.CA do
  @moduledoc """
  CLI command handlers for the cluster certificate authority: CA info,
  issued-certificate listing, per-node revocation, and the multi-stage
  CA rotation (#926, #927) — staging an incoming CA, the rolling
  per-node cert reissue across the BEAM cluster, dual-CA bundle
  distribution, and finalize/abort.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `handle_ca_*` RPC entry points here, so the CLI wire
  contract is unchanged.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.{AuditLog, CertificateAuthority, SystemVolume}
  alias NeonFS.Transport.TLS

  alias NeonFS.Error.{Invalid, NotFound, Unavailable}

  @default_grace_window_seconds 86_400

  @doc """
  Returns cluster CA information.

  ## Returns
  - `{:ok, map}` - CA info with subject, algorithm, validity dates, serial counter
  - `{:error, :ca_not_initialized}` - CA hasn't been created yet
  """
  @spec handle_ca_info() :: {:ok, map()} | {:error, term()}
  def handle_ca_info do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case CertificateAuthority.ca_info() do
        {:ok, info} ->
          {:ok,
           %{
             subject: info.subject,
             algorithm: info.algorithm,
             valid_from: DateTime.to_iso8601(info.valid_from),
             valid_to: DateTime.to_iso8601(info.valid_to),
             current_serial: info.current_serial,
             nodes_issued: info.nodes_issued
           }}

        {:error, _} ->
          {:error, Unavailable.exception(message: "Certificate authority not initialised")}
      end
    end
  end

  @doc """
  Lists all issued node certificates with their status.

  ## Returns
  - `{:ok, [map]}` - List of certificate info maps
  - `{:error, :ca_not_initialized}` - CA hasn't been created yet
  """
  @spec handle_ca_list() :: {:ok, [map()]} | {:error, term()}
  def handle_ca_list do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case CertificateAuthority.list_issued() do
        {:ok, certs} ->
          {:ok,
           Enum.map(certs, fn cert ->
             %{
               node_name: cert.node_name,
               hostname: cert.hostname,
               serial: cert.serial,
               expires: DateTime.to_iso8601(cert.not_after),
               status: if(cert.revoked, do: "revoked", else: "valid")
             }
           end)}

        {:error, _} ->
          {:error, Unavailable.exception(message: "Certificate authority not initialised")}
      end
    end
  end

  @doc """
  Revokes a node's certificate by node name.

  Looks up the node in the issued certificates list and revokes its certificate.

  ## Parameters
  - `node_name` - The node name (as it appears in the certificate subject CN)

  ## Returns
  - `{:ok, map}` - Revocation result with serial number
  - `{:error, :node_not_found}` - No certificate found for the given node
  - `{:error, :ca_not_initialized}` - CA hasn't been created yet
  """
  @spec handle_ca_revoke(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_ca_revoke(node_name) when is_binary(node_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, certs} <- map_ca_error(CertificateAuthority.list_issued()),
         {:ok, cert} <- find_cert_by_node(certs, node_name),
         :ok <- CertificateAuthority.revoke_certificate(cert.serial, :cessation_of_operation) do
      {:ok, %{serial: cert.serial, node_name: cert.node_name, status: "revoked"}}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Rotates the cluster CA.

  CA rotation is a rare, disruptive operation that reissues all node
  certificates. It requires a dual-CA transition period and rolling
  reissuance across the cluster.
  """
  @spec handle_ca_rotate(map()) :: {:ok, map()} | {:error, Exception.t()}
  def handle_ca_rotate(opts \\ %{}) when is_map(opts) do
    set_cli_metadata()

    cond do
      Map.get(opts, "abort", false) -> handle_ca_rotate_abort()
      Map.get(opts, "stage", false) -> handle_ca_rotate_stage()
      Map.get(opts, "finalize", false) -> handle_ca_rotate_finalize()
      Map.get(opts, "status", false) -> handle_ca_rotate_status()
      is_binary(Map.get(opts, "node")) -> handle_ca_rotate_node(Map.fetch!(opts, "node"))
      true -> handle_ca_rotate_default(opts)
    end
  end

  # Private

  # #926 + #927 — orchestrator. Stages a fresh CA, walks the BEAM
  # cluster reissuing each node's cert, distributes the dual-CA
  # bundle, then either finalizes immediately (`no-wait: true`) or
  # stops with the rotation in `pending-finalize` state so the
  # operator can wait for the dual-CA grace window before running
  # `--finalize`.
  defp handle_ca_rotate_default(opts) do
    no_wait? = Map.get(opts, "no-wait", false)
    grace_seconds = Map.get(opts, "grace-window-seconds", @default_grace_window_seconds)

    with :ok <- require_cluster(),
         {:ok, ca_cert, _ca_key} <- stage_incoming_ca_for_orchestrator(),
         :ok <- log_ca_rotate_started_from_cert(ca_cert),
         :ok <- reissue_node_certs_across_cluster(),
         :ok <- distribute_dual_ca_bundle_across_cluster() do
      if no_wait? do
        finalize_rotation_with_audit()
      else
        {:ok,
         %{
           rotated: false,
           pending_finalize: true,
           grace_window_seconds: grace_seconds,
           message:
             "rotation staged + bundle distributed; run `cluster ca rotate --finalize` " <>
               "after waiting at least #{grace_seconds}s for the dual-CA grace window"
         }}
      end
    else
      {:error, reason} = err ->
        log_ca_rotate_failed(reason)
        err
    end
  end

  # #927 — per-node retry. After the rolling reissue from #926 fails
  # for one node, the operator runs `cluster ca rotate --node <name>`
  # to pick that one node back up. Reuses the staged incoming CA.
  defp handle_ca_rotate_node(node_name) do
    node_atom = String.to_atom(node_name)

    with :ok <- require_cluster(),
         :ok <- ensure_incoming_ca_staged(),
         :ok <- reissue_node_cert(node_atom),
         :ok <- distribute_bundle_to(node_atom) do
      {:ok, %{node: node_name, reissued: true}}
    else
      {:error, reason} = err ->
        log_ca_rotate_failed(reason)
        err
    end
  end

  defp ensure_incoming_ca_staged do
    case CertificateAuthority.incoming_ca_info() do
      {:ok, _info} ->
        :ok

      {:error, :no_incoming_ca} ->
        {:error,
         Invalid.exception(
           message: "no CA rotation in progress; run `cluster ca rotate` (without --node) first"
         )}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  defp stage_incoming_ca_for_orchestrator do
    case CertificateAuthority.incoming_ca_info() do
      {:ok, _info} ->
        {:error,
         Invalid.exception(
           message: "CA rotation already in progress; abort it first with --abort"
         )}

      {:error, :no_incoming_ca} ->
        do_stage_incoming_ca()

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  defp do_stage_incoming_ca do
    with {:ok, state} <- load_cluster_state() do
      case CertificateAuthority.init_incoming_ca(state.cluster_name) do
        {:ok, ca_cert, ca_key} -> {:ok, ca_cert, ca_key}
        {:error, reason} -> {:error, wrap_error(reason)}
      end
    end
  end

  defp log_ca_rotate_started_from_cert(ca_cert) do
    fingerprint = ca_fingerprint(ca_cert)
    info = TLS.certificate_info(ca_cert)
    log_ca_rotate_started(fingerprint, info)
    :ok
  end

  defp reissue_node_certs_across_cluster do
    nodes = [Node.self() | Node.list()]

    Enum.reduce_while(nodes, :ok, fn node, _acc ->
      case reissue_node_cert(node) do
        :ok -> {:cont, :ok}
        {:error, _reason} = err -> {:halt, err}
      end
    end)
  end

  defp reissue_node_cert(node) do
    with {:ok, key} <- {:ok, TLS.generate_node_key()},
         csr = TLS.create_csr(key, Atom.to_string(node)),
         {:ok, signed_cert, _ca_cert} <-
           CertificateAuthority.sign_node_csr_with_incoming(csr, Atom.to_string(node)),
         :ok <- rpc_install_node_cert(node, signed_cert, key) do
      log_ca_rotate_node_completed(node, signed_cert)
      :ok
    else
      {:error, reason} ->
        {:error,
         wrap_error(
           Unavailable.exception(
             message: "CA rotation failed for #{inspect(node)}: #{inspect(reason)}"
           )
         )}
    end
  end

  defp rpc_install_node_cert(node, cert, key) do
    cert_pem = TLS.encode_cert(cert)
    key_pem = TLS.encode_key(key)

    case rpc_call_for_ca_rotate(node, NeonFS.TLSDistConfig, :install_node_cert, [
           cert_pem,
           key_pem
         ]) do
      :ok -> :ok
      {:badrpc, reason} -> {:error, {:rpc_failed, node, reason}}
      other -> {:error, {:install_node_cert_failed, node, other}}
    end
  end

  defp distribute_dual_ca_bundle_across_cluster do
    nodes = [Node.self() | Node.list()]

    Enum.reduce_while(nodes, :ok, fn node, _acc ->
      case distribute_bundle_to(node) do
        :ok -> {:cont, :ok}
        {:error, _reason} = err -> {:halt, err}
      end
    end)
  end

  defp distribute_bundle_to(node) do
    with :ok <- rpc_call_or_error(node, NeonFS.TLSDistConfig, :regenerate_ca_bundle, []),
         :ok <- rpc_call_or_error(node, NeonFS.TLSDistConfig, :reload_listener, []) do
      :ok
    else
      err -> err
    end
  end

  defp rpc_call_or_error(node, mod, fun, args) do
    case rpc_call_for_ca_rotate(node, mod, fun, args) do
      :ok -> :ok
      {:badrpc, reason} -> {:error, {:rpc_failed, node, reason}}
      other -> {:error, {:rpc_unexpected, node, other}}
    end
  end

  # Indirection so handler-level tests can stub the RPC layer without a
  # real BEAM cluster. Production callers hit `:rpc.call/4` directly.
  defp rpc_call_for_ca_rotate(node, mod, fun, args) do
    rpc_mod = Application.get_env(:neonfs_core, :ca_rotate_rpc_mod, :rpc)
    rpc_mod.call(node, mod, fun, args)
  end

  defp finalize_rotation_with_audit do
    old_fingerprint = current_active_ca_fingerprint()

    case CertificateAuthority.finalize_rotation() do
      :ok ->
        new_fingerprint = current_active_ca_fingerprint()
        log_ca_rotate_finalized(old_fingerprint, new_fingerprint)

        {:ok,
         %{
           rotated: true,
           old_fingerprint: old_fingerprint,
           fingerprint: new_fingerprint
         }}

      {:error, reason} ->
        {:error,
         Unavailable.exception(message: "Failed to finalize CA rotation: #{inspect(reason)}")}
    end
  end

  defp handle_ca_rotate_status do
    with :ok <- require_cluster() do
      active =
        case CertificateAuthority.ca_info() do
          {:ok, info} ->
            %{
              subject: info.subject,
              valid_from: info.valid_from,
              valid_to: info.valid_to,
              fingerprint: current_active_ca_fingerprint()
            }

          {:error, _} ->
            nil
        end

      incoming =
        case CertificateAuthority.incoming_ca_info() do
          {:ok, info} ->
            %{
              subject: info.subject,
              valid_from: info.valid_from,
              valid_to: info.valid_to,
              fingerprint: current_incoming_ca_fingerprint()
            }

          {:error, _} ->
            nil
        end

      {:ok, %{rotation_in_progress: not is_nil(incoming), active: active, incoming: incoming}}
    end
  end

  defp handle_ca_rotate_abort do
    with :ok <- require_cluster() do
      case CertificateAuthority.incoming_ca_info() do
        {:error, :no_incoming_ca} ->
          {:error, Invalid.exception(message: "No CA rotation in progress to abort")}

        {:ok, _info} ->
          :ok = CertificateAuthority.abort_rotation()
          log_ca_rotate_aborted()
          {:ok, %{aborted: true}}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  defp handle_ca_rotate_finalize do
    with :ok <- require_cluster() do
      case CertificateAuthority.incoming_ca_info() do
        {:error, :no_incoming_ca} ->
          {:error,
           Invalid.exception(
             message: "No CA rotation in progress to finalize; stage one with --stage first"
           )}

        {:ok, _incoming_info} ->
          do_finalize_rotation()

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  defp do_finalize_rotation do
    old_fingerprint = current_active_ca_fingerprint()

    case CertificateAuthority.finalize_rotation() do
      :ok ->
        new_fingerprint = current_active_ca_fingerprint()
        log_ca_rotate_finalized(old_fingerprint, new_fingerprint)

        {:ok, %{finalized: true, old_fingerprint: old_fingerprint, fingerprint: new_fingerprint}}

      {:error, reason} ->
        {:error,
         Unavailable.exception(message: "Failed to finalize CA rotation: #{inspect(reason)}")}
    end
  end

  defp handle_ca_rotate_stage do
    with :ok <- require_cluster(),
         {:ok, state} <- load_cluster_state() do
      case CertificateAuthority.init_incoming_ca(state.cluster_name) do
        {:ok, ca_cert, _ca_key} ->
          info = TLS.certificate_info(ca_cert)
          fingerprint = ca_fingerprint(ca_cert)
          log_ca_rotate_started(fingerprint, info)

          {:ok,
           %{
             staged: true,
             subject: info.subject,
             not_before: info.not_before,
             not_after: info.not_after,
             fingerprint: fingerprint
           }}

        {:error, :incoming_ca_already_staged} ->
          {:error,
           Invalid.exception(
             message: "CA rotation already in progress; abort it first with --abort"
           )}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  defp ca_fingerprint(ca_cert) do
    der = ca_cert |> X509.Certificate.to_der()
    :crypto.hash(:sha256, der) |> Base.encode16(case: :lower)
  end

  defp current_active_ca_fingerprint do
    read_ca_fingerprint("/tls/ca.crt")
  end

  defp current_incoming_ca_fingerprint do
    read_ca_fingerprint("/tls/incoming/ca.crt")
  end

  defp read_ca_fingerprint(path) do
    case SystemVolume.read(path) do
      {:ok, ca_pem} ->
        ca_pem |> TLS.decode_cert!() |> ca_fingerprint()

      {:error, _} ->
        nil
    end
  end

  # Find a cert entry matching the given node name.
  # The node_name in cert metadata is the full X.500 subject (e.g. "/O=NeonFS/CN=node@host").
  # Match against the CN portion or the full subject.
  defp find_cert_by_node(certs, name) do
    case Enum.find(certs, fn cert ->
           cert.node_name == name or
             String.ends_with?(cert.node_name, "/CN=#{name}") or
             cert.hostname == name
         end) do
      nil ->
        {:error, NotFound.exception(message: "No certificate found for node '#{name}'")}

      cert ->
        {:ok, cert}
    end
  end

  defp map_ca_error({:ok, _} = ok), do: ok

  defp map_ca_error({:error, _}),
    do: {:error, Unavailable.exception(message: "Certificate authority not initialised")}

  defp log_ca_rotate_aborted do
    AuditLog.log_event(
      event_type: :cluster_ca_rotate_aborted,
      actor_uid: 0,
      resource: cluster_resource(),
      details: %{}
    )
  end

  defp log_ca_rotate_started(fingerprint, info) do
    AuditLog.log_event(
      event_type: :cluster_ca_rotate_started,
      actor_uid: 0,
      resource: cluster_resource(),
      details: %{
        incoming_ca_fingerprint: fingerprint,
        subject: info.subject,
        not_before: DateTime.to_iso8601(info.not_before),
        not_after: DateTime.to_iso8601(info.not_after)
      }
    )
  end

  defp log_ca_rotate_finalized(old_fingerprint, new_fingerprint) do
    AuditLog.log_event(
      event_type: :cluster_ca_rotate_finalized,
      actor_uid: 0,
      resource: cluster_resource(),
      details: %{
        old_ca_fingerprint: old_fingerprint,
        new_ca_fingerprint: new_fingerprint
      }
    )
  end

  defp log_ca_rotate_node_completed(node, cert) do
    AuditLog.log_event(
      event_type: :cluster_ca_rotate_node_completed,
      actor_uid: 0,
      resource: cluster_resource(),
      details: %{
        node: Atom.to_string(node),
        new_serial: X509.Certificate.serial(cert)
      }
    )
  end

  defp log_ca_rotate_failed(reason) do
    AuditLog.log_event(
      event_type: :cluster_ca_rotate_failed,
      actor_uid: 0,
      resource: cluster_resource(),
      details: %{reason: inspect(reason)}
    )
  end

  defp cluster_resource do
    case load_cluster_state() do
      {:ok, %{cluster_id: id}} -> "cluster:#{id}"
      _ -> "cluster:unknown"
    end
  end
end
