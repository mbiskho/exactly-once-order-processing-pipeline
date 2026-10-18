#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
compose=(docker compose -p order-pipeline -f docker-compose.yml)

"${compose[@]}" up -d --build
for attempt in $(seq 1 60); do
  if curl --fail --silent http://localhost:8080/health >/dev/null; then
    break
  fi
  if [[ "$attempt" == 60 ]]; then
    echo "API did not become healthy; inspect: docker compose -p order-pipeline logs" >&2
    exit 1
  fi
  sleep 2
done

key="smoke-$(date +%s)-$$"
payload="{\"key\":\"smoke-account\",\"idempotencyKey\":\"$key\",\"amountCents\":2599}"
first=$(curl --fail --silent --show-error -H 'Content-Type: application/json' -d "$payload" http://localhost:8080/orders)
second=$(curl --fail --silent --show-error -H 'Content-Type: application/json' -d "$payload" http://localhost:8080/orders)
if ! grep -q '"created":false' <<<"$second"; then
  echo "Idempotent retry did not return created=false: $second" >&2
  exit 1
fi

for attempt in $(seq 1 60); do
  stats=$(curl --fail --silent --show-error http://localhost:8080/stats)
  if grep -Eq '"effects":[1-9][0-9]*' <<<"$stats"; then
    break
  fi
  if [[ "$attempt" == 60 ]]; then
    echo "Order effect did not appear; stats: $stats" >&2
    exit 1
  fi
  sleep 1
done

printf 'Created order: %s\nIdempotent retry: %s\nPipeline stats: %s\n' "$first" "$second" "$stats"
