#!/usr/bin/env bash
# Run the Milvus data-plane smoke test against an Instance from inside the cluster.
# Usage: [MODE=full|write|verify] [REPLICAS=n] test/e2e/run-smoke.sh <namespace> <instance>
set -euo pipefail

NAMESPACE=${1:?namespace}
INSTANCE=${2:?instance}
MODE=${MODE:-full}
REPLICAS=${REPLICAS:-0}
JOB="smoke-${INSTANCE}"
PYMILVUS_VERSION=${PYMILVUS_VERSION:-2.6.3}
DIR=$(cd "$(dirname "$0")" && pwd)

kubectl -n "$NAMESPACE" delete job "$JOB" --ignore-not-found --wait
kubectl -n "$NAMESPACE" create configmap "$JOB" --from-file=smoke.py="$DIR/smoke.py" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NAMESPACE" apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: smoke
          image: python:3.12-slim
          command: [sh, -c, "pip install -q --root-user-action=ignore --disable-pip-version-check pymilvus==${PYMILVUS_VERSION} && python /smoke/smoke.py"]
          env:
            - name: MODE
              value: ${MODE}
            - name: REPLICAS
              value: "${REPLICAS}"
            - name: MILVUS_URI
              valueFrom: {secretKeyRef: {name: ${INSTANCE}-conn, key: uri}}
            - name: MILVUS_TOKEN
              valueFrom: {secretKeyRef: {name: ${INSTANCE}-conn, key: token}}
          volumeMounts:
            - {name: smoke, mountPath: /smoke}
      volumes:
        - name: smoke
          configMap: {name: ${JOB}}
EOF

status=1
for _ in $(seq 120); do
  succeeded=$(kubectl -n "$NAMESPACE" get job "$JOB" -o jsonpath='{.status.succeeded}')
  failed=$(kubectl -n "$NAMESPACE" get job "$JOB" -o jsonpath='{.status.failed}')
  [[ "$succeeded" == "1" ]] && { status=0; break; }
  [[ -n "$failed" ]] && break
  sleep 5
done
kubectl -n "$NAMESPACE" logs "job/$JOB"
kubectl -n "$NAMESPACE" delete job "$JOB" --wait=false >/dev/null
kubectl -n "$NAMESPACE" delete configmap "$JOB" >/dev/null
exit $status
