#!/usr/bin/env bash
# Live end-to-end run against a real cluster with OpenEverest and this provider
# installed (operator running). Standalone: create, smoke test, back up,
# upgrade with data, reject a downgrade, and seed a new instance from the
# backup. Cluster: create, check placement, rolling upgrade with data, scale
# streaming and query nodes, reject a scale-in below a loaded collection's
# replicas, and toggle autoscaling (when the metrics API is available). Every
# instance is deleted at the end and must leave nothing behind. The two
# topologies run one after the other to keep the footprint small.
# Usage: test/e2e/run-matrix.sh [namespace]
set -euo pipefail

NAMESPACE=${1:-milvus-e2e-run}
DIR=$(cd "$(dirname "$0")" && pwd)
READY_TIMEOUT=${READY_TIMEOUT:-900}
UPGRADE_VERSION=${UPGRADE_VERSION:-2.6.25}
DOWNGRADE_VERSION=${DOWNGRADE_VERSION:-2.6.15}

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

wait_backup() {
  local backup=$1 state
  for _ in $(seq 60); do
    state=$(kubectl -n "$NAMESPACE" get backup "$backup" -o jsonpath='{.status.state}')
    [[ "$state" == "Succeeded" ]] && return 0
    [[ "$state" == "Failed" ]] && fail "backup $backup failed"
    sleep 10
  done
  fail "backup $backup not done (state=$state)"
}

milvus_replicas() {
  kubectl -n "$NAMESPACE" get milvus "$1" -o jsonpath="{.spec.components.$2.replicas}"
}

set_replicas() {
  kubectl -n "$NAMESPACE" patch instance "$1" --type merge -p "{\"spec\":{\"components\":{\"$2\":{\"replicas\":$3}}}}"
}

scale() {
  local instance=$1 component=$2 replicas=$3
  set_replicas "$instance" "$component" "$replicas"
  sleep 20
  wait_ready "$instance"
  [[ $(milvus_replicas "$instance" "$component") == "$replicas" ]] || fail "$instance $component not scaled to $replicas"
}

# The rejected spec turns the Instance Failed; putting it back recovers it.
check_scale_in_rejected() {
  local instance=$1
  set_replicas "$instance" queryNode 1
  sleep 30
  [[ $(milvus_replicas "$instance" queryNode) == 2 ]] || fail "$instance query nodes went below a loaded collection's replicas"
  set_replicas "$instance" queryNode 2
  sleep 20
  wait_ready "$instance"
}

check_autoscaling() {
  local instance=$1 hpa="$1-milvus-querynode-hpa" bounds=""
  if ! kubectl get --raw /apis/metrics.k8s.io/v1beta1/nodes >/dev/null 2>&1; then
    log "skipping autoscaling: no metrics API"
    return 0
  fi
  kubectl -n "$NAMESPACE" patch instance "$instance" --type merge \
    -p '{"spec":{"components":{"queryNode":{"parameters":{"autoscaling":{"maxReplicas":3}}}}}}'
  for _ in $(seq 30); do
    bounds=$(kubectl -n "$NAMESPACE" get hpa "$hpa" -o jsonpath='{.spec.minReplicas}-{.spec.maxReplicas}' 2>/dev/null || true)
    [[ "$bounds" == "1-3" ]] && break
    sleep 10
  done
  [[ "$bounds" == "1-3" ]] || fail "$instance has no query node autoscaler (bounds=$bounds)"
  kubectl -n "$NAMESPACE" patch instance "$instance" --type json \
    -p '[{"op":"remove","path":"/spec/components/queryNode/parameters/autoscaling"}]'
  for _ in $(seq 30); do
    kubectl -n "$NAMESPACE" get hpa "$hpa" >/dev/null 2>&1 || { wait_ready "$instance"; return 0; }
    sleep 10
  done
  fail "$instance kept its query node autoscaler after autoscaling was turned off"
}

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

log "creating instances"
kubectl -n "$NAMESPACE" apply -f "$DIR/backup/storage.yaml"

log "standalone: ready, smoke, backup, upgrade with data, downgrade rejected"
kubectl -n "$NAMESPACE" apply -f "$DIR/instances/standalone.yaml"
wait_ready e2e-standalone
check_components e2e-standalone
MODE=write "$DIR/run-smoke.sh" "$NAMESPACE" e2e-standalone
kubectl -n "$NAMESPACE" apply -f "$DIR/backup/backup.yaml"
wait_backup e2e-standalone-backup
upgrade e2e-standalone
MODE=verify "$DIR/run-smoke.sh" "$NAMESPACE" e2e-standalone
check_downgrade_rejected e2e-standalone

log "restore: a new instance seeded from the backup"
kubectl -n "$NAMESPACE" apply -f "$DIR/backup/restored.yaml"
wait_ready e2e-restored
MODE=verify "$DIR/run-smoke.sh" "$NAMESPACE" e2e-restored
# Before its instance: a Backup whose instance is gone cannot be deleted.
kubectl -n "$NAMESPACE" delete backup e2e-standalone-backup --wait=true --timeout=300s
delete_and_check e2e-restored
delete_and_check e2e-standalone

log "cluster: ready, placement, smoke, rolling upgrade with data"
kubectl -n "$NAMESPACE" apply -f "$DIR/instances/cluster.yaml"
wait_ready e2e-cluster
check_components e2e-cluster
check_spread e2e-cluster queryNode
MODE=write "$DIR/run-smoke.sh" "$NAMESPACE" e2e-cluster
upgrade e2e-cluster
MODE=verify "$DIR/run-smoke.sh" "$NAMESPACE" e2e-cluster

log "cluster: scaling"
# Two replicas of a collection need two streaming nodes as well.
scale e2e-cluster streamingNode 2
MODE=write REPLICAS=2 "$DIR/run-smoke.sh" "$NAMESPACE" e2e-cluster
check_scale_in_rejected e2e-cluster
MODE=verify "$DIR/run-smoke.sh" "$NAMESPACE" e2e-cluster
scale e2e-cluster queryNode 1
scale e2e-cluster streamingNode 1
check_components e2e-cluster
check_autoscaling e2e-cluster
delete_and_check e2e-cluster

kubectl -n "$NAMESPACE" delete -f "$DIR/backup/storage.yaml"
log "PASS"
