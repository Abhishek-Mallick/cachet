{{- define "cachet.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "cachet.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "cachet.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "cachet.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "cachet.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "cachet.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cachet.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "cachet.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "cachet.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Image reference, preferring a digest.

A digest is what the signature covers, and a tag can be moved after signing. If both are set the
digest wins, because the alternative is deploying something other than what was verified.
*/}}
{{- define "cachet.image" -}}
{{- $binary := .binary -}}
{{- $img := .root.Values.image -}}
{{- $base := printf "%s/%s/%s" $img.registry $img.repository $binary -}}
{{- if $img.digest -}}
{{- printf "%s@%s" $base $img.digest -}}
{{- else -}}
{{- printf "%s:%s" $base (default $img.tag $img.tag | default .root.Chart.AppVersion) -}}
{{- end -}}
{{- end -}}

{{/*
The sidecar container, for inclusion in YOUR pod spec.

This is the recommended topology and the one the benchmarks measure — the engine reached over a Unix
socket in the same pod, with no network hop on the read path. A chart cannot deploy it for you,
because it belongs to your Deployment rather than to this release. Use it like:

    spec:
      template:
        spec:
          containers:
            - name: my-app
              ...
            {{- include "cachet.sidecar" (dict "root" .) | nindent 12 }}
          volumes:
            - name: cachet-socket
              emptyDir: {}
*/}}
{{- define "cachet.sidecar" -}}
- name: cachet
  image: {{ include "cachet.image" (dict "root" .root "binary" "cachet") }}
  imagePullPolicy: {{ .root.Values.image.pullPolicy }}
  args:
    - --listen=unix://{{ .root.Values.engine.socketPath }}
    - --metrics-addr=:{{ .root.Values.engine.metricsPort }}
    - --config=/etc/cachet/config.yaml
  ports:
    - name: metrics
      containerPort: {{ .root.Values.engine.metricsPort }}
  volumeMounts:
    - name: cachet-socket
      mountPath: {{ dir .root.Values.engine.socketPath }}
    - name: cachet-config
      mountPath: /etc/cachet
      readOnly: true
  securityContext:
    {{- toYaml .root.Values.securityContext | nindent 4 }}
  resources:
    {{- toYaml .root.Values.engine.resources | nindent 4 }}
{{- end -}}
