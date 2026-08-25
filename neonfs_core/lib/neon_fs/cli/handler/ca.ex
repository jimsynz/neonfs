defmodule NeonFS.CLI.Handler.CA do
  @moduledoc """
  CLI command handlers for the cluster certificate authority: CA info,
  issued-certificate listing, per-node revocation, and the multi-stage
  CA rotation — staging an incoming CA, the rolling
  per-node cert reissue across the BEAM cluster, dual-CA bundle
  distribution, and finalize/abort.

  Extracted from `NeonFS.CLI.Handler`. `NeonFS.CLI.Handler`
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

  # Orchestrator. Stages a fresh CA, adds it to every node's trust
  # bundle, walks the BEAM cluster reissuing each node's cert against
  # it, then either finalizes immediately (`no-wait: true`) or stops
  # with the rotation in `pending-finalize` state so the operator can
  # wait for the dual-CA grace window before running `--finalize`.
  #
  # The anchor goes out before the certificates that chain to it. A
  # node handed a certificate signed by a CA its peers do not yet
  # trust is a node its peers stop accepting connections from, and
  # the whole point of the dual-CA window is that neither anchor is
  # ever missing.
  defp handle_ca_rotate_default(opts) do
    no_wait? = Map.get(opts, "no-wait", false)
    grace_seconds = Map.get(opts, "grace-window-seconds", @default_grace_window_seconds)

    with :ok <- require_cluster(),
         {:ok, ca_cert, _ca_key} <- stage_incoming_ca_for_orchestrator(),
         :ok <- log_ca_rotate_started_from_cert(ca_cert),
         :ok <- distribute_dual_ca_bundle_across_cluster(TLS.encode_cert(ca_cert)),
         :ok <- reissue_node_certs_across_cluster() do
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

  # Per-node retry. After the rolling reissue fails
  # for one node, the operator runs `cluster ca rotate --node <name>`
  # to pick that one node back up. Reuses the staged incoming CA.
  defp handle_ca_rotate_node(node_name) do
    node_atom = String.to_atom(node_name)

    with :ok <- require_cluster(),
         {:ok, incoming_ca_pem} <- staged_incoming_ca_pem(),
         :ok <- distribute_bundle_to(node_atom, incoming_ca_pem),
         :ok <- reissue_node_cert(node_atom) do
      {:ok, %{node: node_name, reissued: true}}
    else
      {:error, reason} = err ->
        log_ca_rotate_failed(reason)
        err
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
    fingerprint = TLS.cert_fingerprint(ca_cert)
    info = TLS.certificate_info(ca_cert)
    log_ca_rotate_started(fingerprint, info)
    :ok
  end

  defp reissue_node_certs_across_cluster do
    each_cluster_node(&reissue_node_cert/1)
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

  defp distribute_dual_ca_bundle_across_cluster(incoming_ca_pem) do
    each_cluster_node(&distribute_bundle_to(&1, incoming_ca_pem))
  end

  defp distribute_bundle_to(node, incoming_ca_pem) do
    rpc_call_or_error(node, NeonFS.TLSDistConfig, :install_incoming_ca, [incoming_ca_pem])
  end

  # Promotion visits every node even after one of them fails, unlike the
  # staging walks: the nodes it can reach are better off holding the
  # promoted anchor, and the operator needs the whole list of the ones
  # that missed it rather than the first. That list is what a re-run of
  # `--finalize` reconciles.
  defp promote_active_ca_across_cluster(active_ca_pem) do
    case promote_active_ca_on(cluster_nodes(), active_ca_pem) do
      [] -> :ok
      failed -> {:error, {:promote_active_ca_failed, failed}}
    end
  end

  defp promote_active_ca_on(nodes, active_ca_pem) do
    nodes
    |> Enum.map(
      &{&1, rpc_call_or_error(&1, NeonFS.TLSDistConfig, :promote_active_ca, [active_ca_pem])}
    )
    |> Enum.reject(&match?({_node, :ok}, &1))
    |> Enum.map(&elem(&1, 0))
  end

  defp discard_incoming_ca_across_cluster do
    each_cluster_node(fn node ->
      rpc_call_or_error(node, NeonFS.TLSDistConfig, :discard_incoming_ca, [])
    end)
  end

  defp each_cluster_node(fun) do
    Enum.reduce_while(cluster_nodes(), :ok, fn node, _acc ->
      case fun.(node) do
        :ok -> {:cont, :ok}
        {:error, _reason} = err -> {:halt, err}
      end
    end)
  end

  defp cluster_nodes, do: [Node.self() | Node.list()]

  defp staged_incoming_ca_pem do
    case CertificateAuthority.incoming_ca_pem() do
      {:ok, pem} ->
        {:ok, pem}

      {:error, :no_incoming_ca} ->
        {:error,
         Invalid.exception(
           message: "no CA rotation in progress; run `cluster ca rotate` (without --node) first"
         )}

      {:error, reason} ->
        {:error, wrap_error(reason)}
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
    with {:ok, old_fingerprint, new_fingerprint} <- finalize_rotation() do
      {:ok,
       %{
         rotated: true,
         old_fingerprint: old_fingerprint,
         fingerprint: new_fingerprint
       }}
    end
  end

  # Promotion is two writes, not one: the system volume is where the CA
  # lives, and every node's `ca.crt` is what its distribution listener
  # actually verifies against. Promoting only the former leaves the
  # superseded CA a trust anchor on every node in the cluster, for as long
  # as that node lives.
  defp finalize_rotation do
    old_fingerprint = current_active_ca_fingerprint()

    with :ok <- map_finalize_error(CertificateAuthority.finalize_rotation()),
         {:ok, active_ca_pem} <- map_finalize_error(CertificateAuthority.active_ca_pem()),
         :ok <- map_finalize_error(promote_active_ca_across_cluster(active_ca_pem)) do
      new_fingerprint = current_active_ca_fingerprint()
      log_ca_rotate_finalized(old_fingerprint, new_fingerprint)
      {:ok, old_fingerprint, new_fingerprint}
    end
  end

  defp map_finalize_error(:ok), do: :ok
  defp map_finalize_error({:ok, _} = ok), do: ok

  defp map_finalize_error({:error, {:promote_active_ca_failed, nodes}}) do
    names = Enum.map_join(nodes, ", ", &Atom.to_string/1)

    {:error,
     Unavailable.exception(
       message:
         "CA promoted in the cluster, but #{length(nodes)} node(s) did not take the new " <>
           "anchor: #{names}. Re-run `cluster ca rotate --finalize` to reconcile them; " <>
           "`cluster ca rotate --status` reports each node's anchor."
     )}
  end

  defp map_finalize_error({:error, reason}),
    do:
      {:error,
       Unavailable.exception(message: "Failed to finalize CA rotation: #{inspect(reason)}")}

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

      nodes =
        cluster_node_ca_states(
          active && active.fingerprint,
          incoming && incoming.fingerprint
        )

      {:ok,
       %{
         rotation_in_progress: not is_nil(incoming),
         active: active,
         incoming: incoming,
         nodes: Enum.map(nodes, &Map.update!(&1, :node, fn node -> Atom.to_string(node) end))
       }}
    end
  end

  # What each node holds on disk, against what the cluster says it
  # should. Nothing else in the rotation reports this, and a node that
  # missed a step is not otherwise visible — the anchors it kept are
  # what let it go on working.
  defp cluster_node_ca_states(cluster_active, cluster_incoming) do
    Enum.map(cluster_nodes(), fn node ->
      case rpc_call_for_ca_rotate(node, NeonFS.TLSDistConfig, :local_ca_state, []) do
        {:ok, %{active_ca_fingerprint: active, incoming_ca_fingerprint: staged}} ->
          %{
            node: node,
            ca_fingerprint: active,
            incoming_ca_fingerprint: staged,
            state: classify_node_ca(active, staged, cluster_active, cluster_incoming)
          }

        _unreachable ->
          %{
            node: node,
            ca_fingerprint: nil,
            incoming_ca_fingerprint: nil,
            state: :unreachable
          }
      end
    end)
  end

  defp classify_node_ca(node_active, node_staged, cluster_active, cluster_incoming) do
    cond do
      not is_nil(cluster_incoming) and node_active == cluster_active and
          node_staged == cluster_incoming ->
        :dual_ca

      not is_nil(cluster_incoming) ->
        :rotation_incomplete

      node_active == cluster_active and is_nil(node_staged) ->
        :in_sync

      true ->
        :finalize_incomplete
    end
  end

  defp handle_ca_rotate_abort do
    with :ok <- require_cluster() do
      case CertificateAuthority.incoming_ca_info() do
        {:error, :no_incoming_ca} ->
          {:error, Invalid.exception(message: "No CA rotation in progress to abort")}

        {:ok, _info} ->
          do_abort_rotation()

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  # Clearing the staged anchor off every node is the abort. Leaving it
  # behind means the cluster keeps trusting a CA it discarded, and the next
  # `--stage` writes a second one alongside it.
  defp do_abort_rotation do
    with :ok <- discard_incoming_ca_across_cluster(),
         :ok <- CertificateAuthority.abort_rotation() do
      log_ca_rotate_aborted()
      {:ok, %{aborted: true}}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  defp handle_ca_rotate_finalize do
    with :ok <- require_cluster() do
      case CertificateAuthority.incoming_ca_info() do
        {:error, :no_incoming_ca} ->
          resume_finalize_rotation()

        {:ok, _incoming_info} ->
          do_finalize_rotation()

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  defp do_finalize_rotation do
    with {:ok, old_fingerprint, new_fingerprint} <- finalize_rotation() do
      {:ok, %{finalized: true, old_fingerprint: old_fingerprint, fingerprint: new_fingerprint}}
    end
  end

  # A finalize with no staged CA left to promote is either a no-op the
  # operator should be told about, or the second half of a finalize that
  # only got partway. The two are indistinguishable from the system
  # volume alone — the promotion there has already happened either way —
  # so the nodes are what decides it.
  #
  # A node the first attempt could not reach keeps its superseded
  # `ca.crt` and its now-meaningless staged `incoming-ca.crt`, and goes
  # on working, because the staged anchor still trusts the promoted CA.
  # That is what makes this quiet. Promoting again is safe on a node
  # that already ran it: `promote_active_ca/2` writes the same anchor,
  # removes an `incoming-ca.crt` that is already gone, and rebuilds the
  # same bundle.
  defp resume_finalize_rotation do
    with {:ok, active_ca_pem} <- map_finalize_error(CertificateAuthority.active_ca_pem()) do
      fingerprint = TLS.cert_fingerprint(active_ca_pem)

      case Enum.reject(cluster_node_ca_states(fingerprint, nil), &(&1.state == :in_sync)) do
        [] ->
          {:error,
           Invalid.exception(
             message: "No CA rotation in progress to finalize; stage one with --stage first"
           )}

        behind ->
          reconcile_finalize(Enum.map(behind, & &1.node), active_ca_pem, fingerprint)
      end
    end
  end

  defp reconcile_finalize(nodes, active_ca_pem, fingerprint) do
    case promote_active_ca_on(nodes, active_ca_pem) do
      [] ->
        log_ca_rotate_finalize_resumed(nodes, fingerprint)

        {:ok,
         %{
           finalized: true,
           resumed: true,
           fingerprint: fingerprint,
           reconciled_nodes: Enum.map(nodes, &Atom.to_string/1)
         }}

      failed ->
        map_finalize_error({:error, {:promote_active_ca_failed, failed}})
    end
  end

  defp handle_ca_rotate_stage do
    with :ok <- require_cluster(),
         {:ok, state} <- load_cluster_state() do
      case CertificateAuthority.init_incoming_ca(state.cluster_name) do
        {:ok, ca_cert, _ca_key} ->
          info = TLS.certificate_info(ca_cert)
          fingerprint = TLS.cert_fingerprint(ca_cert)
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

  defp current_active_ca_fingerprint do
    read_ca_fingerprint("/tls/ca.crt")
  end

  defp current_incoming_ca_fingerprint do
    read_ca_fingerprint("/tls/incoming/ca.crt")
  end

  defp read_ca_fingerprint(path) do
    case SystemVolume.read(path) do
      {:ok, ca_pem} ->
        TLS.cert_fingerprint(ca_pem)

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

  defp log_ca_rotate_finalize_resumed(nodes, fingerprint) do
    AuditLog.log_event(
      event_type: :cluster_ca_rotate_finalize_resumed,
      actor_uid: 0,
      resource: cluster_resource(),
      details: %{
        ca_fingerprint: fingerprint,
        reconciled_nodes: Enum.map(nodes, &Atom.to_string/1)
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
