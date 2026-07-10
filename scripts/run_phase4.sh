#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/sdk/go/bin:$HOME/go/bin:$HOME/.local/bin:$PATH"

mkdir -p bin bench/results

free_disk_mb() {
  df -Pm / | awk 'NR==2{print $4}'
}

wait_kafka_healthy() {
  for i in $(seq 1 40); do
    status=$(docker inspect --format='{{.State.Health.Status}}' sluice-kafka 2>/dev/null || echo "missing")
    if [ "$status" = "healthy" ]; then return 0; fi
    sleep 3
  done
  echo "kafka never became healthy" >&2
  return 1
}

wait_timescale_ready() {
  for i in $(seq 1 40); do
    if (cd deploy && docker compose exec -T timescaledb pg_isready -U sluice) >/dev/null 2>&1; then
      return 0
    fi
    sleep 3
  done
  echo "timescale never became ready" >&2
  return 1
}

start_port_forward() {
  pkill -f "port-forward svc/sluice-gateway" 2>/dev/null || true
  sleep 1
  kubectl port-forward svc/sluice-gateway 50051:50051 > /tmp/sluice_phase4_portforward.log 2>&1 &
  PF_PID=$!
  sleep 3
}

echo "=== phase4 step 1/5: build distroless images ==="
docker build -f deploy/docker/gateway.Dockerfile -t sluice-gateway:local .
docker build -f deploy/docker/consumer.Dockerfile -t sluice-consumer:local .
docker images | grep sluice

echo "=== phase4 step 2/5: helm lint + template (no cluster required) ==="
helm lint deploy/helm/sluice
helm template sluice deploy/helm/sluice > /tmp/sluice_helm_rendered.yaml
echo "rendered $(wc -l < /tmp/sluice_helm_rendered.yaml) lines to /tmp/sluice_helm_rendered.yaml"

echo "=== phase4 step 3/5: terraform validate (no cluster required) ==="
(cd deploy/terraform && terraform init -backend=false -input=false && terraform validate)

echo "=== phase4 step 4/5: ansible syntax check (no cluster required) ==="
if command -v ansible-playbook >/dev/null 2>&1; then
  ansible-playbook --syntax-check deploy/ansible/bootstrap.yml
else
  echo "ansible-playbook not on PATH, skipping syntax check (see RUN_STATUS.md)"
fi

echo "=== phase4 step 5/5: kind cluster + terraform apply + replica-count scaling sweep ==="
FREE_MB=$(free_disk_mb)
echo "free disk: ${FREE_MB}MB"

REQUIRED_MB=3000
if [ "${FREE_MB}" -lt "${REQUIRED_MB}" ]; then
  cat >&2 <<EOF

BLOCKED: only ${FREE_MB}MB free; a kind node image plus control-plane data typically needs
${REQUIRED_MB}MB+ of headroom, and this session already hit disk exhaustion twice in Phase 3 on a
machine that started with far more headroom than this. Deliberately not attempting kind cluster
creation here to avoid a repeat incident.

Commands to run this step yourself once more disk is available (e.g. after removing old kernels,
snap revisions, or other host data not managed by this repo):

  cd $(pwd)
  export PATH="\$HOME/sdk/go/bin:\$HOME/go/bin:\$HOME/.local/bin:\$PATH"
  cd deploy/terraform
  terraform apply -auto-approve
  kubectl --kubeconfig "\$(terraform output -raw kubeconfig_path)" rollout status deployment/sluice-gateway
  kubectl --kubeconfig "\$(terraform output -raw kubeconfig_path)" rollout status deployment/sluice-consumer
  cd ../..
  # then for each replica count in {1,2,4} (8 optional, expected to flatten on 8 threads):
  #   terraform -chdir=deploy/terraform apply -auto-approve -var consumer_replicas=<N>
  #   kubectl --kubeconfig <path> rollout status deployment/sluice-consumer
  #   ./bin/loadgen -target <gateway-service-external-ip>:50051 -duration 30s -warmup 5s \\
  #       -concurrency 128 -batch-size 100 -devices 10000 -results-prefix phase4_scaling_r<N>
  #   record throughput + max consumer-group lag per replica count
  # then: terraform destroy -auto-approve

Marking this step BLOCKED and continuing (per this project's autonomy rules) rather than stopping.
EOF
  echo '{"phase":"phase4_scaling","status":"BLOCKED","reason":"insufficient disk for kind node image","free_disk_mb":'"${FREE_MB}"',"required_disk_mb":'"${REQUIRED_MB}"',"machine_tag":"Intel MacBook Pro, quad-core i5-1038NG7 (8 threads), 16GB RAM, Ubuntu 20.04, native Docker"}' \
    > "bench/results/phase4_scaling_$(date -u +%Y%m%dT%H%M%SZ)_BLOCKED.json"
  exit 0
fi

echo "sufficient disk detected - proceeding with kind + terraform"
echo "note: KAFKA_ADVERTISED_LISTENERS must point at the kind bridge gateway IP (not localhost) so"
echo "pods can reach it after the initial bootstrap - see docker-compose.phase4.override.yml and"
echo "RUN_STATUS.md for why. Also note kind node containers do NOT share the host's docker image"
echo "store, so images must be explicitly loaded with 'kind load docker-image' after cluster create."

HOST_GATEWAY_IP=$(python3 -c "
import subprocess, json
out = subprocess.check_output(['docker','network','inspect','kind']).decode()
for cfg in json.loads(out)[0]['IPAM']['Config']:
    if 'Gateway' in cfg:
        print(cfg['Gateway'])
        break
" 2>/dev/null || echo "172.18.0.1")
echo "kind bridge gateway IP: ${HOST_GATEWAY_IP}"

(cd deploy && docker compose -f docker-compose.yml -f docker-compose.phase4.override.yml --profile phase2 up -d)
wait_kafka_healthy
wait_timescale_ready

(cd deploy/terraform && terraform apply -auto-approve -var "host_gateway_ip=${HOST_GATEWAY_IP}")
KUBECONFIG_PATH=$(cd deploy/terraform && terraform output -raw kubeconfig_path)
export KUBECONFIG="${KUBECONFIG_PATH}"

kind load docker-image sluice-gateway:local sluice-consumer:local --name sluice

kubectl rollout status deployment/sluice-gateway --timeout=120s
kubectl rollout status deployment/sluice-consumer --timeout=120s

./bin/topicinit -brokers "${HOST_GATEWAY_IP}:9092" -topics telemetry.events,telemetry.events.dlq -partitions 12 -replication 1

for REPLICAS in 1 2 4; do
  echo "--- consumer replicas=${REPLICAS} ---"
  (cd deploy/terraform && terraform apply -auto-approve -var "consumer_replicas=${REPLICAS}" -var "host_gateway_ip=${HOST_GATEWAY_IP}")
  kubectl rollout status deployment/sluice-consumer --timeout=120s
  kubectl rollout status deployment/sluice-gateway --timeout=120s

  start_port_forward

  ./bin/loadgen -target localhost:50051 -duration 30s -warmup 5s \
    -concurrency 128 -batch-size 100 -devices 10000 \
    -sink-label timescale -real-infra=true \
    -results-prefix "phase4_scaling_r${REPLICAS}"
done

pkill -f "port-forward svc/sluice-gateway" 2>/dev/null || true
(cd deploy/terraform && terraform destroy -auto-approve -var "host_gateway_ip=${HOST_GATEWAY_IP}")
(cd deploy && docker compose --profile phase2 down)
docker volume rm deploy_kafka-data deploy_timescale-data 2>/dev/null || true

echo "phase4 complete"
