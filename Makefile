.PHONY: tidy fmt vet test test-integration test-e2e up down migrate-up migrate-down logs

DB_TEST_URL ?= postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable

tidy:            ## resolve/lock dependencies (creates go.sum) - run once after cloning
	go mod tidy

fmt:
	gofmt -l -w .

vet:
	go vet ./... && go vet -tags "integration e2e" ./...

test:            ## unit tests (no infrastructure needed), with the race detector
	go test -race -count=1 ./...

up:              ## whole stack: postgres, localstack(SQS), keycloak, 3 replicas
	docker compose up -d --build --wait

down:
	docker compose down -v

migrate-up:
	docker compose run --rm migrate up

migrate-down:    ## reverts ONE migration (destroys data, dev/test only)
	docker compose run --rm migrate down 1

test-integration: ## PostgreSQL-level tests (needs only the postgres container)
	docker compose up -d --wait postgres
	TEST_DATABASE_URL="$(DB_TEST_URL)" go test -tags integration -race -count=1 ./internal/...

test-e2e: up     ## full-stack tests against the compose environment
	E2E_COMPOSE=1 go test -tags e2e -count=1 -timeout 10m ./test/integration/...

logs:
	docker compose logs -f app1 app2 app3
