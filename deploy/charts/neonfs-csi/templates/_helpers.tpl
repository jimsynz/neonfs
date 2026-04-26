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
