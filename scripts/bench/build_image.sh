#!/usr/bin/env bash
# Builds the benchmark images and makes them available to the cluster that runs
# the Jobs: `kind load` on compose, an amd64 build imported into k3s's containerd
# over ssh on k8s. Names follow execution-controller's rule <prefix>bench:<tag>,
# where the prefix is its DOCKERHUB_USERNAME plus "/" when set.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
if [ "$(bench_target)" = "k8s" ]; then
  user="$(bench_env_value execution-controller DOCKERHUB_USERNAME 2>/dev/null || true)"
else
  user="$(docker exec execution-controller printenv DOCKERHUB_USERNAME 2>/dev/null || true)"
fi
prefix=""
[ -n "${user}" ] && prefix="${user}/"
for spec in "v1 0" "slow30 30"; do
  read -r tag sleep_s <<< "${spec}"
  if [ "$(bench_target)" = "k8s" ]; then
    docker buildx build --platform linux/amd64 --load \
      --build-arg "SLEEP_SECONDS=${sleep_s}" -t "${prefix}bench:${tag}" "${here}/image"
  else
    docker build --build-arg "SLEEP_SECONDS=${sleep_s}" -t "${prefix}bench:${tag}" "${here}/image"
  fi
done
if [ "$(bench_target)" = "k8s" ]; then
  docker save "${prefix}bench:v1" "${prefix}bench:slow30" \
    | ssh "${BENCH_SSH_HOST:-continuo-server}" k3s ctr -n k8s.io images import -
else
  kind load docker-image "${prefix}bench:v1" "${prefix}bench:slow30" --name "${BENCH_KIND_CLUSTER:-continuo}"
fi
echo "build_image.sh: ${prefix}bench:v1 and ${prefix}bench:slow30 available to the $(bench_target) cluster" >&2
