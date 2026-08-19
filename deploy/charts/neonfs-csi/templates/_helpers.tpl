{{/*
Common label/name helpers for the neonfs-csi chart.
*/}}

{{- define "neonfs-csi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "neonfs-csi.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "neonfs-csi.controllerName" -}}
{{- printf "%s-controller" (include "neonfs-csi.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "neonfs-csi.nodeName" -}}
{{- printf "%s-node" (include "neonfs-csi.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "neonfs-csi.controllerServiceAccount" -}}
{{- if .Values.serviceAccount.controllerName -}}
{{- .Values.serviceAccount.controllerName -}}
{{- else -}}
{{- include "neonfs-csi.controllerName" . -}}
{{- end -}}
{{- end -}}

{{- define "neonfs-csi.nodeServiceAccount" -}}
{{- if .Values.serviceAccount.nodeName -}}
{{- .Values.serviceAccount.nodeName -}}
{{- else -}}
{{- include "neonfs-csi.nodeName" . -}}
{{- end -}}
{{- end -}}

{{- define "neonfs-csi.bootstrapSecretName" -}}
{{- if .Values.bootstrap.existingSecret -}}
{{- .Values.bootstrap.existingSecret -}}
{{- else -}}
{{- printf "%s-bootstrap" (include "neonfs-csi.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "neonfs-csi.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag -}}
{{- end -}}

{{- define "neonfs-csi.image" -}}
{{- printf "%s:%s" .Values.image.repository (include "neonfs-csi.imageTag" .) -}}
{{- end -}}

{{- define "neonfs-csi.commonLabels" -}}
app.kubernetes.io/name: {{ include "neonfs-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "neonfs-csi.controllerSelectorLabels" -}}
app.kubernetes.io/name: {{ include "neonfs-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: controller
{{- end -}}

{{- define "neonfs-csi.nodeSelectorLabels" -}}
app.kubernetes.io/name: {{ include "neonfs-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: node
{{- end -}}

{{/*
Required values, checked once so a misconfigured install fails at render rather
than as pods that come up and never reach the cluster. That failure mode is the
one this chart has already shipped twice.
*/}}
{{- define "neonfs-csi.validate" -}}
{{- if not .Values.coreNode -}}
{{- fail "neonfs-csi: coreNode is required — set it to core's Erlang node name, e.g. --set coreNode=neonfs_core@10.0.0.1" -}}
{{- end -}}
{{- if not (contains "@" .Values.coreNode) -}}
{{- fail (printf "neonfs-csi: coreNode must be an Erlang node name like neonfs_core@10.0.0.1, got %q. The host:port redemption endpoint is joinVia." .Values.coreNode) -}}
{{- end -}}
{{/*
`uses` is required with `value`, not with the install: a deployment whose hosts
were provisioned out of band needs no token, and demanding one would refuse to
render a configuration that works. What must not happen is a token supplied
without a budget sized for the node count, because that fails later as pods on
the unlucky nodes never obtaining identity.
*/}}
{{- if and .Values.bootstrap.value (not .Values.bootstrap.uses) -}}
{{- fail "neonfs-csi: bootstrap.uses is required with bootstrap.value — an invite is redeemed once per node, so size it against the nodes that will run NeonFS workloads (neonfs cluster create-invite --uses N)" -}}
{{- end -}}
{{- end -}}

{{/*
The init container that gives this host its NeonFS identity.

Identical in both workloads by design: the controller has to be self-sufficient
on any host it lands on, including a control-plane node the DaemonSet's
tolerations exclude. It redeems an invite into the host's state directory and
exits; nothing is left running, and the host does not become a cluster member.

Runs as uid 0 because the node key it writes is 0600, and the same reason the
plugin containers do.
*/}}
{{/*
The redemption endpoint. `coreNode` is a node name; this is the HTTP address an
invite is redeemed at. Derived from `coreNode`'s host on core's default API port
unless given, because those agree in every deployment where core is on its
defaults, and requiring both would make the common case say the same host twice.
*/}}
{{- define "neonfs-csi.joinVia" -}}
{{- if .Values.joinVia -}}
{{- .Values.joinVia -}}
{{- else -}}
{{- printf "%s:9568" (splitList "@" .Values.coreNode | last) -}}
{{- end -}}
{{- end -}}

{{- define "neonfs-csi.provisionInitContainer" -}}
- name: provision-identity
  image: {{ include "neonfs-csi.image" . }}
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  command:
    - /app/bin/neonfs_csi
    - eval
    - NeonFS.CSI.Provision.main()
  securityContext:
    runAsUser: 0
  env:
    # The redemption endpoint, not the node name: this container speaks HTTP to
    # a cluster member and never uses distribution.
    - name: NEONFS_JOIN_VIA
      value: {{ include "neonfs-csi.joinVia" . | quote }}
    # Optional so a host whose identity was provisioned out of band still
    # starts. The command checks for existing credentials before it needs a
    # token, and only complains when it actually has to redeem.
    - name: NEONFS_BOOTSTRAP_TOKEN
      valueFrom:
        secretKeyRef:
          name: {{ include "neonfs-csi.bootstrapSecretName" . }}
          key: {{ .Values.bootstrap.secretKey }}
          optional: true
  volumeMounts:
    - name: neonfs-state
      mountPath: /var/lib/neonfs
{{- end -}}

{{/*
The host state a provisioned pod reads. Read-only: a pod consumes the identity
the init container established and must not be able to change it.
*/}}
{{- define "neonfs-csi.stateVolumeMounts" -}}
- name: neonfs-tls
  mountPath: /var/lib/neonfs/tls
  readOnly: true
- name: neonfs-meta
  mountPath: /var/lib/neonfs/meta
  readOnly: true
{{- end -}}

{{- define "neonfs-csi.stateVolumes" -}}
- name: neonfs-state
  hostPath:
    path: {{ .Values.stateDir | quote }}
    type: DirectoryOrCreate
- name: neonfs-tls
  hostPath:
    path: {{ printf "%s/tls" .Values.stateDir | quote }}
    type: DirectoryOrCreate
- name: neonfs-meta
  hostPath:
    path: {{ printf "%s/meta" .Values.stateDir | quote }}
    type: DirectoryOrCreate
{{- end -}}
