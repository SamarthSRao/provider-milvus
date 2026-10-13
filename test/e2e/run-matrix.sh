#!/usr/bin/env bash
# Live end-to-end run against a real cluster with OpenEverest and this provider
# installed (operator running). For each instance in instances/: create it,
# wait for Ready, check status.components and pod placement, run the data-plane
# smoke test, then delete it and check nothing is left behind. Both instances
# are also upgraded with data written before and verified after, and a
# downgrade must be rejected.
# Usage: test/e2e/run-matrix.sh [namespace]
set -euo pipefail

NAMESPACE=${1:-milvus-e2e-run}
DIR=$(cd "$(dirname "$0")" && pwd)
READY_TIMEOUT=${READY_TIMEOUT:-900}
UPGRADE_VERSION=${UPGRADE_VERSION:-2.6.15}
DOWNGRADE_VERSION=${DOWNGRADE_VERSION:-2.6.10}

log() { echo "[$(date +%H:%M:%S)] $*"; }
fail() { log "FAIL: $*"; kubectl -n "$NAMESPACE" get instance,milvus,pods -o wide || true; exit 1; }

wait_ready() {
  local instance=$1 phase message
  for _ in $(seq $((READY_TIMEOUT / 10))); do
    phase=$(kubectl -n "$NAMESPACE" get instance "$instance" -o jsonpath='{.status.phase}')
    [[ "$phase" == "Ready" ]] && return 0
    [[ "$phase" == "Failed" ]] && fail "$instance is Failed"
    sleep 10
  done
  message=$(kubectl -n "$NAMESPACE" get instance "$instance" -o jsonpath='{.status.message}')
  fail "$instance not Ready after ${READY_TIMEOUT}s (phase=$phase, message=$message)"
}

check_components() {
  local instance=$1 not_ready
  not_ready=$(kubectl -n "$NAMESPACE" get instance "$instance" -o json |
    jq -r '[.status.components[]? | select(.replicas != .readyReplicas or .replicas == 0) | .name] | join(",")')
  [[ -z "$not_ready" ]] || fail "$instance components not ready: $not_ready"
  [[ $(kubectl -n "$NAMESPACE" get instance "$instance" -o json | jq '.status.components | length') -gt 0 ]] ||
    fail "$instance reports no status.components"
}

check_images() {
  local instance=$1 version=$2 images stale
  # Query nodes roll blue/green: the idle group is scaled to 0 and keeps the old image.
  images=$(kubectl -n "$NAMESPACE" get deploy -l "app.kubernetes.io/instance=$instance,app.kubernetes.io/name=milvus" \
    -o jsonpath='{range .items[*]}{.spec.replicas} {.metadata.name}={.spec.template.spec.containers[0].image}{"\n"}{end}' |
    awk '$1 > 0 {print $2}')
  [[ -n "$images" ]] || fail "$instance has no Milvus deployments"
  stale=$(grep -v "=.*:v$version\$" <<<"$images" || true)
  [[ -z "$stale" ]] || fail "$instance not on $version: $stale"
}

upgrade() {
  local instance=$1
  kubectl -n "$NAMESPACE" patch instance "$instance" --type merge -p "{\"spec\":{\"version\":\"$UPGRADE_VERSION\"}}"
  sleep 20
  wait_ready "$instance"
  check_images "$instance" "$UPGRADE_VERSION"
}

# Validation errors do not reach the Instance status on every core release, so
# check the deployments instead: they must stay on the upgraded version.
check_downgrade_rejected() {
  local instance=$1
  kubectl -n "$NAMESPACE" patch instance "$instance" --type merge -p "{\"spec\":{\"version\":\"$DOWNGRADE_VERSION\"}}"
  sleep 30
  check_images "$instance" "$UPGRADE_VERSION"
  kubectl -n "$NAMESPACE" patch instance "$instance" --type merge -p "{\"spec\":{\"version\":\"$UPGRADE_VERSION\"}}"
}

check_spread() {
  local instance=$1 component=$2 pods nodes
  pods=$(kubectl -n "$NAMESPACE" get pods -l "core.openeverest.io/instance=$instance,core.openeverest.io/component=$component" \
    -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}')
  nodes=$(sort -u <<<"$pods" | grep -c .)
  [[ "$nodes" == $(grep -c . <<<"$pods") ]] || fail "$instance $component pods share a node despite anti-affinity"
}

delete_and_check() {
  local instance=$1 leftovers
  kubectl -n "$NAMESPACE" delete instance "$instance" --wait=true --timeout=300s
  # The operator uninstalls the dependency releases after the Instance is gone.
  for _ in $(seq 60); do
    if leftovers=$(kubectl -n "$NAMESPACE" get milvus,pods,pvc,statefulsets,deployments -o name); then
      leftovers=$(grep -- "/$instance" <<<"$leftovers" || true)
      [[ -z "$leftovers" ]] && return 0
    fi
    sleep 10
  done
  fail "$instance left behind: $leftovers"
}

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

log "creating instances"
kubectl -n "$NAMESPACE" apply -f "$DIR/instances/"

log "standalone: ready, smoke, upgrade with data, downgrade rejected"
wait_ready e2e-standalone
check_components e2e-standalone
MODE=write "$DIR/run-smoke.sh" "$NAMESPACE" e2e-standalone
upgrade e2e-standalone
MODE=verify "$DIR/run-smoke.sh" "$NAMESPACE" e2e-standalone
check_downgrade_rejected e2e-standalone

log "cluster: ready, placement, smoke, rolling upgrade with data"
wait_ready e2e-cluster
check_components e2e-cluster
check_spread e2e-cluster queryNode
MODE=write "$DIR/run-smoke.sh" "$NAMESPACE" e2e-cluster
upgrade e2e-cluster
MODE=verify "$DIR/run-smoke.sh" "$NAMESPACE" e2e-cluster

log "deleting instances"
delete_and_check e2e-standalone
delete_and_check e2e-cluster

log "PASS"
