.PHONY: test demo chaos serve stack-up stack-down smoke chaos-live logs

test:
	go test -race ./...

demo:
	go run ./cmd/pipeline demo

chaos:
	go run ./cmd/pipeline chaos -runs 20

serve:
	python3 -m http.server 8090

stack-up:
	docker compose -p order-pipeline -f docker-compose.yml up -d --build

stack-down:
	docker compose -p order-pipeline -f docker-compose.yml down

smoke:
	bash scripts/smoke-compose.sh

chaos-live:
	bash scripts/chaos-compose.sh

logs:
	docker compose -p order-pipeline -f docker-compose.yml logs -f --tail=100
