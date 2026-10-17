# Jungle Wallet - carteira de apostas (Go)

Serviço de carteira para operações de provedores de jogos (`BET`, `WIN`, `LOSS`,
`REFUND`, `ROLLBACK`), com saldo consistente sob concorrência, idempotência
persistida, ledger imutável, mensageria SQS (entrada e saída) e autenticação
OAuth2/OIDC via Keycloak.

Decisões, garantias e limitações em detalhe: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Stack

Go · Uber Fx · PostgreSQL 16 (pgx) · SQS FIFO (LocalStack) · Keycloak 26 ·
Prometheus · slog (JSON) · Docker Compose · golang-migrate.

## Pré-requisitos

Go 1.22+ · Docker com Compose v2 · (opcional) `make`, `jq`, `curl`. Portas livres: 5432, 4566, 8080–8083.

## Como rodar

```bash
go mod tidy            # opcional após clonar, para conferir/atualizar dependências (go.sum já acompanha o projeto)
docker compose up --build        # postgres, localstack(SQS), keycloak, migrations, filas e 3 réplicas
```

`make up` equivale a `docker compose up -d --build --wait`. Réplicas: `http://localhost:8080`, `:8082`, `:8083`.
Keycloak: `http://localhost:8081` (admin/admin). Métricas: `GET /metrics`. Probes: `/health/live`, `/health/ready`.

### Variáveis de ambiente

`.env.example` lista todas, com valores locais de exemplo (sem segredos reais). O compose já define as necessárias;
`DATABASE_URL`, `OIDC_ISSUER` e `OIDC_JWKS_URL` são obrigatórias (a aplicação não sobe sem elas).

| Variável | Padrão | Função |
|---|---|---|
| `DATABASE_URL`, `DB_MAX_CONNS` | - , 20 | PostgreSQL |
| `SQS_ENDPOINT`, `AWS_REGION`, `QUEUE_TRANSACTIONS`, `QUEUE_TRANSACTIONS_DLQ`, `QUEUE_EVENTS` | vazio (AWS), us-east-1, nomes `.fifo` | broker |
| `OIDC_ISSUER`, `OIDC_JWKS_URL`, `OIDC_AUDIENCE`, `ROLE_PROVIDER`, `ROLE_INTERNAL` | -, -, `wallet-api`, `provider`, `wallet-internal` | autenticação (issuer = claim `iss` do token; JWKS = como *este* container alcança o Keycloak) |
| `CONSUMER_*`, `VISIBILITY_TIMEOUT` | 2 pollers, 60s | consumidor SQS |
| `OUTBOX_*` | 500ms, lote 25, lease 30s | publicador do outbox |
| `PENDING_*` | 8 tentativas, TTL 15m, backoff 2s→5m | referências pendentes |
| `SHUTDOWN_TIMEOUT`, `LOG_LEVEL`, `INSTANCE_ID`, `HTTP_ADDR` | 25s, info, hostname, :8080 | operação |

### Filas

Criadas pelo serviço `queues-init` do compose (`deploy/localstack/init-queues.sh`, idempotente):
`wager-transactions.fifo` (redrive para a DLQ após 5 recebimentos), `wager-transactions-dlq.fifo` e
`wager-events.fifo`. Recriar: `docker compose run --rm queues-init`.

### Migrations

Versionadas em `migrations/` (golang-migrate). Aplicar: `make migrate-up`
(o `docker compose up` já aplica). Reverter uma versão: `make migrate-down`.
A reversão remove tabelas e dados - apenas desenvolvimento/teste.

### Obtendo um token e chamando a API

```bash
TOKEN=$(curl -s -d grant_type=client_credentials -d client_id=wallet-internal \
  -d client_secret=wallet-internal-secret \
  http://localhost:8081/realms/wallet/protocol/openid-connect/token | jq -r .access_token)

curl -s -X POST localhost:8080/wallets -H "Authorization: Bearer $TOKEN" \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
```

Clientes de desenvolvimento (segredos **apenas locais**): `provider-a`, `provider-b`
(papel `provider`) e `wallet-internal` (papel `wallet-internal`); secret = `<client>-secret`.

## Testes

```bash
go vet ./...            # análise estática
go test ./...           # unitários (sem infraestrutura)
go test -race ./...     # idem, com detector de corrida
```

Integração - PostgreSQL real, concorrência, constraints, outbox, inbox, HTTP, consumidor SQS, ciclo de vida do Fx
(build tag `integration`; cada execução usa um schema próprio e descartável):

```bash
docker compose up -d --wait postgres
TEST_DATABASE_URL='postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable' \
  go test -tags integration -race -count=1 ./...
```

Ponta a ponta - Keycloak e LocalStack reais, **3 réplicas independentes**, restart e parada graciosa
(build tag `e2e`):

```bash
docker compose up -d --build --wait
go test -tags e2e -count=1 -timeout 10m ./test/integration/...          # fluxos autenticados, paralelismo, HTTP+SQS, DLQ, eventos
E2E_COMPOSE=1 go test -tags e2e -count=1 -timeout 10m ./test/integration/...   # + simulações de falha (restart/stop dos containers)
```

Simulações manuais de falha: `docker compose restart app1 app2 app3` (estado e idempotência persistem),
`docker compose stop -t 30 app3` (SIGTERM: drena requisições e libera mensagens), `docker compose stop postgres`
(503 + `Retry-After`; readiness cai) e `docker compose kill app2` (outro nó assume pendências e outbox).

## API

| Método e rota | Quem | Resultado |
|---|---|---|
| `POST /wallets` | interno | 201 carteira (versão 1; saldo inicial > 0 cria `OPENING`, ledger e eventos) |
| `GET /wallets/{id}` | interno | saldo e versão |
| `GET /wallets/{id}/ledger?limit&cursor` | interno | extrato paginado (cursor opaco, ordem estável) |
| `POST /wallets/{id}/reconciliation` | interno | saldo armazenado × saldo reconstruído do ledger (nunca corrige) |
| `POST /wagering/transactions` | provedor | 200 `PROCESSED` (inclui replay) · 202 `PENDING_REFERENCE` · 422 `REJECTED` |
| `GET /wagering/transactions/{id}` | provedor (só as suas) / interno | consulta por id |
| `GET /providers/{p}/wagering/transactions/{ext}` | provedor (só as suas) / interno | consulta por id externo |

`POST /wagering/transactions` exige o header `Idempotency-Key`; o `providerId` do corpo
deve ser o `client` autenticado (senão 403). Valores monetários são **strings** com
exatamente duas casas (`"25.00"`); número JSON, sinal, expoente ou escala extra → 400.

Erros: `{"error":{"code","message","correlationId"}}` - `INVALID_REQUEST` 400,
`MISSING_IDEMPOTENCY_KEY` 400, `UNAUTHENTICATED` 401, `FORBIDDEN` 403,
`NOT_FOUND`/`WALLET_NOT_FOUND` 404, `WALLET_ALREADY_EXISTS` 409,
`IDEMPOTENCY_KEY_PAYLOAD_MISMATCH` 409, `DUPLICATE_EXTERNAL_TRANSACTION` 409,
`SERVICE_UNAVAILABLE` 503 + `Retry-After` (reenvie com a mesma chave).

Rejeições de negócio (`REJECTED`, 422) trazem `failureCode`: `INSUFFICIENT_FUNDS`,
`REVERSAL_INSUFFICIENT_FUNDS`, `PLAYER_MISMATCH`, `CURRENCY_MISMATCH`,
`REFERENCE_NOT_FOUND`, `REFERENCE_NOT_SUCCESSFUL`, `REFERENCE_PENDING_TIMEOUT`,
`REFERENCE_MISMATCH`, `REFERENCE_INVALID_KIND`, `REFERENCE_AMOUNT_MISMATCH`,
`REFERENCE_ALREADY_REVERSED`. São persistidas, auditáveis e idempotentes.

## SQS

| Fila | Papel |
|---|---|
| `wager-transactions.fifo` | entrada: mensagem `WagerTransactionRequested` (grupo = `walletId`) |
| `wager-transactions-dlq.fifo` | mensagens impossíveis de processar e esgotadas (redrive após 5 recebimentos) |
| `wager-events.fifo` | saída: eventos do outbox (grupo = `walletId`, dedup = `eventId`) |

Eventos: `WagerTransactionProcessed`, `WagerTransactionRejected`,
`WalletBalanceChanged`, `WagerTransactionPendingReference` (envelope com `eventId`,
`eventType`, `aggregateId`, `correlationId`, `causationId`, `occurredAt`, `version`, `data`).

## Validação técnica

Validações executadas em 17/10/2026, com evidências de execução em Windows/PowerShell e Docker:

| Verificação | Resultado |
|---|---|
| Testes unitários (`go test -count=1 ./...`) | **PASS** |
| Análise estática (`go vet ./...` e com tags) | **PASS** |
| Formatação Go (`gofmt`) | **PASS** |
| Race detector (Linux/Docker, CGO habilitado) | **PASS** |
| Integração com PostgreSQL (`-tags integration`, `TEST_DATABASE_URL` definido) | **PASS** |
| E2E completo (`-tags e2e`, `E2E_COMPOSE=1`) | **PASS** (77,280 s) |
| Reinicialização de réplicas e desligamento controlado | **PASS** |
| Saúde e métricas das três réplicas | **9 respostas HTTP 200** |
| Dependências (`go mod verify`) e configuração (`docker compose config --quiet`) | **PASS** |

No cenário de desligamento controlado, o teste registrou **539 operações confirmadas e 539 armazenadas**. A infraestrutura Docker foi inicializada com PostgreSQL, LocalStack, Keycloak e três réplicas saudáveis; migrations e inicialização das filas concluíram com código 0.

Consulte [`VALIDATION_REPORT.md`](VALIDATION_REPORT.md) para o detalhamento dos testes e [`SUBMISSION_STATUS.md`](SUBMISSION_STATUS.md) para um resumo da preparação do repositório.

## Limitações conhecidas

Listadas com justificativa em `ARCHITECTURE.md` (seção "Limitações"). As principais:
a autenticação do produtor SQS depende de IAM/política da fila (o LocalStack não aplica);
a ordem global de eventos não é garantida (apenas FIFO por carteira no destino);
somente moedas de 2 casas decimais; um mesmo provedor deve manter `externalTransactionId`
únicos por provedor.
