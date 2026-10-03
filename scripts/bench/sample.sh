#!/usr/bin/env bash
# Samples CPU and memory every INTERVAL seconds until killed, appending TSV lines
# "<utc-ts>\t<name>\t<cpu>\t<mem>" to OUT.
#   sample.sh docker OUT INTERVAL CONTAINER...                 (docker stats)
#   sample.sh k8s OUT INTERVAL NAMESPACE [KUBECTL_ARGS...]     (kubectl top; task Job pods excluded)
set -euo pipefail
mode="${1:?usage: sample.sh docker|k8s OUT INTERVAL ARGS...}"
out="${2:?}"
interval="${3:?}"
shift 3
case "${mode}" in
  docker)
    [ "$#" -gt 0 ] || { echo "sample.sh docker: give container names or ids" >&2; exit 2; }
    while true; do
      ts="$(date -u +%FT%TZ)"
      docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' "$@" 2>/dev/null \
        | awk -F'\t' -v ts="${ts}" '{ split($3, mem, " / "); print ts "\t" $1 "\t" $2 "\t" mem[1] }' >> "${out}"
      sleep "${interval}"
    done
    ;;
  k8s)
    ns="${1:?sample.sh k8s: give the namespace}"
    shift
    while true; do
      ts="$(date -u +%FT%TZ)"
      kubectl "$@" -n "${ns}" top pods --no-headers -l '!job-name' 2>/dev/null \
        | awk -v ts="${ts}" '{ print ts "\t" $1 "\t" $2 "\t" $3 }' >> "${out}"
      kubectl "$@" top nodes --no-headers 2>/dev/null \
        | awk -v ts="${ts}" '{ print ts "\tNODE\t" $2 "\t" $4 }' >> "${out}"
      sleep "${interval}"
    done
    ;;
  *)
    echo "sample.sh: unknown mode ${mode}" >&2
    exit 2
    ;;
esac
