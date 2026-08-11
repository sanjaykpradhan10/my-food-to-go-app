{{/*
Renders an initContainers block that blocks until each named TCP dependency
is accepting connections, replacing compose.yml's depends_on/condition
chains (Kubernetes has no native equivalent — see spec Decision/mapping
table row "depends_on: condition:").
Usage: {{ include "ftgo.waitFor" (list (dict "name" "mysql" "port" 3306) (dict "name" "kafka" "port" 29092)) }}
*/}}
{{- define "ftgo.waitFor" -}}
{{- range . }}
- name: wait-for-{{ .name }}
  image: busybox:1.36
  command: ["sh", "-c", "until nc -z {{ .name }} {{ .port }}; do echo waiting for {{ .name }}:{{ .port }}; sleep 2; done"]
{{- end }}
{{- end -}}
