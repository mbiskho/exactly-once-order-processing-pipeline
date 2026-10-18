#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
compose=(docker compose -p order-pipeline -f docker-compose.yml)
network=order-pipeline_default
run_prefix="chaos-$(date +%s)-$$"
recovery_sum=0
recovery_max=0
"${compose[@]}" up -d --build

for attempt in $(seq 1 60); do
  if curl --fail --silent http://localhost:8080/health >/dev/null; then
    break
  fi
  if [[ "$attempt" == 60 ]]; then
    echo "API did not become healthy; inspect compose logs." >&2
    exit 1
  fi
  sleep 2
done
started=$SECONDS

for run in $(seq 1 20); do
  prefix="$run_prefix-$run"
  fault_started=$SECONDS
  case $((run % 3)) in
    0)
      relay_id=$("${compose[@]}" ps -q relay)
      docker network disconnect "$network" "$relay_id"
      ;;
    1)
      "${compose[@]}" kill -s SIGKILL consumer
      ;;
    2)
      "${compose[@]}" kill -s SIGKILL redpanda
      ;;
  esac

  for order in $(seq 1 12); do
    key="$prefix-order-$order"
    partition="account-$((order % 3))"
    payload="{\"key\":\"$partition\",\"idempotencyKey\":\"$key\",\"amountCents\":$((100 + order))}"
    curl --fail --silent --show-error -H 'Content-Type: application/json' -d "$payload" http://localhost:8080/orders >/dev/null
  done

  case $((run % 3)) in
    0)
      docker network connect "$network" "$relay_id"
      ;;
    1)
      "${compose[@]}" up -d consumer
      ;;
    2)
      "${compose[@]}" up -d redpanda consumer
      ;;
  esac

  completed=false
  for attempt in $(seq 1 90); do
    count=$("${compose[@]}" exec -T postgres psql -U pipeline -d pipeline -Atqc \
      "SELECT count(*) FROM order_effects e JOIN orders o USING (order_id) WHERE o.idempotency_key LIKE '$prefix-order-%'")
    if [[ "$count" == 12 ]]; then
      completed=true
      break
    fi
    sleep 1
  done
  if [[ "$completed" != true ]]; then
    echo "Run $run stalled: expected 12 unique effects, found $count." >&2
    "${compose[@]}" logs --tail=80 api relay consumer postgres redpanda >&2
    exit 1
  fi

  missing=$("${compose[@]}" exec -T postgres psql -U pipeline -d pipeline -Atqc \
    "SELECT count(*) FROM orders o LEFT JOIN order_effects e USING (order_id) WHERE o.idempotency_key LIKE '$prefix-order-%' AND e.order_id IS NULL")
  duplicated=$("${compose[@]}" exec -T postgres psql -U pipeline -d pipeline -Atqc \
    "SELECT count(*) FROM (SELECT e.order_id FROM order_effects e JOIN orders o USING (order_id) WHERE o.idempotency_key LIKE '$prefix-order-%' GROUP BY e.order_id HAVING count(*) <> 1) violations")
  ordering=$("${compose[@]}" exec -T postgres psql -U pipeline -d pipeline -Atqc \
    "SELECT count(*) FROM (SELECT partition_key, sequence_number, row_number() OVER (PARTITION BY partition_key ORDER BY sequence_number) AS position, min(sequence_number) OVER (PARTITION BY partition_key) AS first_sequence FROM orders WHERE idempotency_key LIKE '$prefix-order-%') ordered WHERE sequence_number <> first_sequence + position - 1")
  if [[ "$missing" != 0 || "$duplicated" != 0 || "$ordering" != 0 ]]; then
    echo "Run $run invariant failure: missing=$missing duplicate-effects=$duplicated ordering=$ordering." >&2
    exit 1
  fi
  recovery=$((SECONDS - fault_started))
  recovery_sum=$((recovery_sum + recovery))
  if (( recovery > recovery_max )); then recovery_max=$recovery; fi
  printf 'Run %02d/20 recovered in %ss; 12 orders, zero lost, zero duplicate effects, zero ordering violations.\n' "$run" "$recovery"
done

elapsed=$((SECONDS - started))
average_recovery=$((recovery_sum / 20))
throughput=0
if (( elapsed > 0 )); then throughput=$((240 / elapsed)); fi
printf '{"runs":20,"orders":240,"lost":0,"duplicateEffects":0,"orderingViolations":0,"faultsInjected":20,"elapsedSeconds":%d,"ordersPerSecond":%d,"averageRecoverySeconds":%d,"maxRecoverySeconds":%d}\n' \
  "$elapsed" "$throughput" "$average_recovery" "$recovery_max" | tee reports/chaos-test-results-live.json
