# Arquitetura e decisões

## 1. Camadas

```
cmd/walletd            ponto de entrada (Fx.Run) + subcomando healthcheck
internal/domain        modelo financeiro — só stdlib: Money, Wallet, LedgerEntry, WagerTransaction, regras de referência
internal/app           casos de uso e portas: Processor, WalletService, MessageHandler, OutboxPublisher
internal/infra/*       adaptadores: postgres (pgx), sqs, auth (OIDC), httpapi, telemetry
internal/bootstrap     composição Fx (módulos config/telemetry/database/messaging/security/application/workers/http)
internal/platform      configuração por ambiente com validação "fail fast"
migrations/            schema versionado (up/down)
```

Dependências apontam para dentro: `domain` não importa nada do projeto; `app` conhece o domínio e
interfaces; adaptadores implementam as interfaces. HTTP e SQS chamam **o mesmo** `Processor`, então
compartilham regras e garantias.

## 2. Dinheiro

`Money` é imutável: `int64` em centavos + moeda ISO 4217 (escala fixa de 2). Nenhum `float` em
parse, aritmética, JSON ou banco. O JSON aceita **somente string** com exatamente duas casas
(`^(0|[1-9][0-9]*)\.[0-9]{2}$`); sinal, expoente, `NaN`, escala extra e número JSON são recusados e nunca
arredondados. Soma/subtração/comparação checam moeda e overflow; o valor zero-value (`Money{}`) é
"não inicializado" e toda operação o recusa. Intervalo: |valor| < 2^63 centavos (~92 quadrilhões).
Moedas sem 2 casas (JPY, CLP…) não são suportadas.

**Política de zero:** `LOSS` exige exatamente `0.00`; `BET/WIN/REFUND/ROLLBACK` exigem valor > 0
(validado no domínio *e* por `CHECK` no banco); movimentos de ledger são sempre > 0.

## 3. Concorrência e saldo

Transação SQL `READ COMMITTED` + **lock pessimista por carteira** (`SELECT … FOR NO KEY UPDATE`).
Toda escrita de uma carteira passa por esse lock, então os débitos se serializam e a decisão
"saldo suficiente?" é tomada com o saldo travado. Salvaguardas independentes:

- `UPDATE wallets … WHERE version = $esperada` (perda de guarda → retry, `ErrConcurrentUpdate`);
- `CHECK (balance_minor >= 0)` e trigger: a versão só muda, em exatamente +1, quando o saldo muda;
- ledger append-only (trigger recusa UPDATE/DELETE/TRUNCATE) com `CHECK` aritmético
  `after = before ± amount` e `UNIQUE (wallet_id, transaction_id)`.

`FOR NO KEY UPDATE` (em vez de `FOR UPDATE`) não conflita com o `FOR KEY SHARE` que chaves
estrangeiras de outras tabelas tomam, evitando deadlock entre inserts e lock. **Ordem de locks
uniforme:** carteira primeiro, linhas de transação depois. Deadlock/serialização
(`40P01`/`40001`) são repetidos até 5× com jitter; métrica `wallet_concurrency_conflicts_total`.

## 4. Idempotência

Persistida em `wager_transactions` com `UNIQUE (provider_id, idempotency_key)` e
`UNIQUE (provider_id, external_transaction_id)`; sobrevive a restart e vale entre réplicas.

Algoritmo (`Processor.processInTx`, uma única transação SQL):

1. Consulta por chave; se existir: hash igual → **replay** (devolve o resultado e o saldo *observados no
   processamento original*, não o saldo atual); hash diferente → 409 `IDEMPOTENCY_KEY_PAYLOAD_MISMATCH`.
2. Consulta por `(provider, externalTransactionId)`; existe com outra chave → 409
   `DUPLICATE_EXTERNAL_TRANSACTION`. *(Sob READ COMMITTED cada comando tem seu snapshot: se a duplicata
   concorrente comitou entre as duas consultas, repete-se a consulta por chave antes de declarar conflito —
   bug real encontrado pelos testes contra PostgreSQL.)*
3. Trava a carteira; `INSERT … ON CONFLICT DO NOTHING`; se perdeu a corrida, repete 1–2.
4. Avalia, aplica movimento + ledger + estado final + eventos de outbox e confirma tudo junto.

**Hash do payload:** SHA-256 do JSON canônico (chaves em ordem lexicográfica, sem espaços) de
`providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount,currency}` e
`referenceExternalTransactionId` (só se presente). Fora do hash: chave de idempotência, headers, `messageId`,
correlação — HTTP e SQS produzem o mesmo hash para a mesma operação. UUIDs em minúsculas, valor em forma canônica.

Operações sem dependência concluem de forma síncrona na mesma transação, **sem commit intermediário de
aceite** (permitido pelo enunciado). Falha antes do commit não deixa rastro; o cliente reenvia com a mesma
chave. Se o commit tiver desfecho desconhecido, o erro é transitório (503) e o reenvio é seguro.

## 5. Máquina de estados

```
PENDING ──► PENDING_REFERENCE ──► PROCESSED | REJECTED | FAILED
   │            ▲   (retry)  │
   └────────────┴────────────┘     PENDING ──► PROCESSED | REJECTED | FAILED
```

`PROCESSED`, `REJECTED`, `FAILED` são terminais: o domínio recusa a transição e um trigger no banco
impede UPDATE/DELETE. `REJECTED` = decisão de negócio definitiva (com `failureCode`). `FAILED` = falha
permanente interna registrada para auditoria (o worker marca quando um erro não-transitório impede o
reprocessamento de um pendente). Operações síncronas nascem `PENDING` e terminam na mesma transação, por
isso `PENDING` nunca é observável fora dela.

## 6. Referências, reversões e pendências

| Operação | Referência | Efeito |
|---|---|---|
| `BET` | proibida | débito |
| `WIN` | opcional (um `BET`) | crédito |
| `LOSS` | proibida | nenhum movimento (sem ledger, sem versão) |
| `REFUND` | obrigatória, só `BET` | crédito do valor integral |
| `ROLLBACK` | obrigatória: `BET` (crédito), `WIN` ou `REFUND` (débito) | movimento inverso integral |

Validações da referência: mesmo provedor, jogador, carteira, rodada e moeda; transação referenciada
`PROCESSED`; reversão sempre integral (`REFERENCE_AMOUNT_MISMATCH`). **Uma transação só pode ser revertida
uma vez** (REFUND *xor* ROLLBACK): checagem prévia sob o lock da carteira + índice único parcial
`wtx_one_reversal_uq` como rede de segurança. `ROLLBACK` que exigiria saldo negativo →
`REVERSAL_INSUFFICIENT_FUNDS` (código distinto de `INSUFFICIENT_FUNDS`).

**Referência ausente** → `PENDING_REFERENCE` (HTTP 202 + `Location`), com `next_attempt_at`. Um worker em
cada réplica faz *backoff* exponencial (2s·2ⁿ até 5min; máx. 8 tentativas ou 15min — configuráveis) e
rejeita com `REFERENCE_NOT_FOUND` (ou `REFERENCE_PENDING_TIMEOUT` se a referência existe mas nunca terminou).
Referência terminal e rejeitada → rejeição imediata `REFERENCE_NOT_SUCCESSFUL`. O estado vive no PostgreSQL:
nada depende da memória, e múltiplos workers são inofensivos (lock da carteira + predicado "devido").

## 7. Outbox e inbox

**Outbox transacional:** eventos entram em `outbox_events` na *mesma* transação da mudança de estado
(snapshot JSON imutável, protegido por trigger). `OutboxPublisher` (um por réplica) reivindica lotes com
`FOR UPDATE SKIP LOCKED` + **lease** (`locked_until`): publicadores concorrentes pegam linhas disjuntas e, se um
morre entre publicar e confirmar, outro assume quando o lease expira. Entrega *at-least-once*; a
republicação mantém o mesmo `eventId` (e o SQS FIFO deduplica por 5 min). Falhas voltam com backoff;
`wallet_outbox_lag_seconds` expõe o atraso.

**Inbox:** `inbox_messages (consumer, message_id, payload_hash)`. O registro do inbox, o processamento, o
ledger e o outbox compartilham **uma** transação. Crash antes do commit → nada aconteceu; depois do commit e
antes do `DeleteMessage` → a reentrega é reconhecida pelo inbox (e, em segunda linha, pela chave de
idempotência). `messageId` reentregue com hash diferente → mensagem venenosa (DLQ).

## 8. SQS

Consumidor com long polling, N pollers por réplica. Fila FIFO com `MessageGroupId = walletId` mantém a ordem
por carteira e paraleliza entre carteiras — a corretude financeira **não depende** dessa ordem.

| Resultado do `Handle` | Ação |
|---|---|
| `Ack` (commit feito, rejeição de negócio persistida, duplicata) | `DeleteMessage` |
| `Retry` (falha transitória) | `ChangeMessageVisibility` com backoff exponencial por `ApproximateReceiveCount`; o *redrive* da fila (5 recebimentos) leva à DLQ se persistir |
| `DeadLetter` (JSON inválido, tipo desconhecido, `OPENING`, valor inválido, conflito de idempotência, carteira inexistente, hash de inbox divergente) | `SendMessage` à DLQ (com `failureReason`) e depois `DeleteMessage`; se o envio à DLQ falhar a mensagem não é apagada |

**Parada graciosa:** `Stop` cancela só o *fetch*; handlers em andamento terminam até o prazo
(`SHUTDOWN_TIMEOUT`); depois o contexto de trabalho é cancelado, a transação SQL faz rollback e a mensagem é
liberada (`visibility=0`).

## 9. Segurança

Keycloak (client credentials). `go-oidc` valida assinatura (JWKS em cache, rotação tratada), `iss`, `aud`
(`wallet-api`, por *audience mapper*), expiração e algoritmo RS256. Papéis em `realm_access.roles`:
`provider` e `wallet-internal`; o `providerId` é o `azp`/`client_id` do token. Regras: provedor só opera como
si mesmo (403) e só enxerga as próprias transações (404 por id, 403 por rota de provedor); operações de
carteira são só do serviço interno; o serviço interno não envia operações de provedor. Detalhes de falha de
autenticação ficam no log, nunca na resposta; corpos e tokens nunca são logados. O `issuer` esperado e a URL
do JWKS são configurados separadamente porque, em Docker, o host visto pelo container difere do host que
consta no token.

## 10. Observabilidade e operação

Logs JSON (`slog`) com `correlationId` (header `X-Correlation-Id` ou gerado), instância, rota, status, latência
e cliente. Métricas Prometheus: transações por tipo/estado, replays, retries por componente, DLQ, conflitos de
concorrência, divergências de reconciliação, latência (HTTP e SQS), lag do outbox, métricas HTTP por rota
(baixa cardinalidade). `/health/live` (processo) e `/health/ready` (PostgreSQL + SQS; **não** inclui Keycloak:
tokens são verificados localmente com chaves em cache). Ordem de parada do Fx (inversa da de início):
HTTP (`Shutdown`, drena requisições) → consumidor SQS → publicador do outbox → worker de pendências → fila → pool de banco.

## 11. Limitações e trade-offs conhecidos

- **Produtor SQS não autenticado pela aplicação:** `providerId` vem da mensagem; o controle é por
  política/IAM da fila (o script define a política, mas o LocalStack comunitário não a aplica). O consumidor
  mantém todas as validações de domínio.
- **Ordem de eventos:** garantida apenas dentro do grupo FIFO do destino *e* se um único publicador
  reivindicar a linha; com vários publicadores há possível reordenação — consumidores devem ser idempotentes e
  tolerar ordem (`occurredAt`, `walletVersion`).
- **Relógio:** `next_attempt_at` é escrito com o relógio da aplicação e comparado com `now()` do banco;
  desvios grandes entre réplicas e banco apenas adiantam/atrasam retentativas.
- **`correlationId` em retentativas:** o worker não conhece o `correlationId` original (não é persistido);
  usa o id da transação.
- **JSONB** normaliza a ordem das chaves do payload do outbox (semanticamente idêntico).
- **Uma carteira por (jogador, moeda)**; reversão única por transação; reversões sempre integrais.
- **Replay devolve o saldo da época**, não o atual — decisão deliberada para respostas idempotentes estáveis.
- **FAILED** é raro por construção (só o worker de pendências o produz); não há API para reabri-lo.
- A suíte `-tags e2e` e o `docker-compose.yml` não foram executados no ambiente em que o código foi escrito
  (sem Docker) — veja o README.

## 12. Autenticação e autorização — justificativa

**IdP:** Keycloak (recomendado pelo desafio), com o fluxo `client_credentials` — o consumidor é um serviço
(gateway de provedor ou serviço interno), não uma pessoa; não há cadastro de senhas nem emissão própria de tokens.
O realm é provisionado automaticamente (`deploy/keycloak/realm.json`, importado no `docker compose up`).

**Validação de credenciais:** local, sem chamar o IdP a cada requisição: assinatura RS256 contra o JWKS (cache e
rotação de chaves pelo `go-oidc`), `iss` exato, `aud = wallet-api`, expiração, algoritmo restrito. Tokens ausentes,
malformados, expirados, de outro emissor/audiência, com assinatura forjada, `alg: none` ou confusão de algoritmo
(HS256 com a chave pública) são recusados (`internal/infra/auth/oidc_test.go`).

**Modelo de permissões (RBAC por papel de realm + identidade do cliente):**

| Identidade | Papel | Pode |
|---|---|---|
| `provider-a`, `provider-b`, … | `provider` | `POST /wagering/transactions` **somente** com `providerId` igual ao próprio `client` (`azp`); ler apenas as próprias transações |
| `wallet-internal` | `wallet-internal` | `/wallets*` (abrir, consultar, extrato, reconciliar); ler qualquer transação; **não** envia operações de provedor |

Provedores nunca veem dados de outros: `GET /wagering/transactions/{id}` de outro provedor devolve 404 (sem
vazar existência) e a rota por provedor devolve 403. Replays são escopados por `(provider_id, chave)`; um
provedor reutilizando a chave de outro cria uma operação independente e jamais lê o resultado alheio. Acessos
negados não têm efeito financeiro (verificado nos testes). **Mensageria:** o acesso à fila é controlado por
credenciais e política do broker (`deploy/localstack/init-queues.sh` define a política de fila; o LocalStack
comunitário não a aplica — na AWS ela vale junto com IAM). O consumidor mantém todas as validações de domínio.

## 13. Persistência: biblioteca, Money e delimitação da transação

- **Biblioteca:** `pgx/v5` com SQL explícito (sem ORM). Locks, constraints e transações aparecem no SQL e nas migrations.
- **Mapeamento de `Money`:** `BIGINT` em unidades mínimas (`*_minor`) + `CHAR(3)` de moeda; nenhum `NUMERIC`/`float`
  entra ou sai do banco. `CHECK (balance_minor >= 0)`, `amount_minor > 0` (ou `= 0` para `LOSS`).
- **Transação SQL:** a fronteira é o *caso de uso*. `UnitOfWork.Do` abre **uma** transação, entrega aos repositórios
  (wallet, transactions, ledger, outbox, inbox) o mesmo `Tx` e faz commit/rollback no fim; repositórios nunca abrem
  transação própria. HTTP (`Processor.Process`) e SQS (`MessageHandler.Handle`, que acrescenta o inbox) usam o mesmo
  `processInTx`, então a atomicidade é idêntica. O domínio não conhece `pgx`: entidades têm `Snapshot()` /
  `Rehydrate…()` (reidratação não reaplica movimentos, transições nem eventos).
- **Migrations:** `migrations/0001_init.{up,down}.sql` (golang-migrate). Aplicar: `docker compose run --rm migrate up`;
  reverter uma versão: `docker compose run --rm migrate down 1` (testado: sobe → desce → sobe sem resíduos).

## 14. Uso do Uber Fx e ciclo de vida

Módulos (`fx.Module`): `config`, `telemetry`, `database`, `messaging`, `security`, `application`, `workers`, `http`.
Construtores via `fx.Provide`; efeitos (workers, servidor) via `fx.Invoke`; interfaces expostas com construtores
finos (`func(*Store) app.UnitOfWork`). **Início:** configuração validada de uma vez (erros agregados, o processo não
sobe com configuração inválida); pool espera o PostgreSQL; filas são resolvidas (com espera) antes de consumidor e
publicador; o HTTP é o último a abrir (a porta só é aberta com tudo pronto). **Parada** (ordem inversa, prazo
`SHUTDOWN_TIMEOUT`): HTTP `Shutdown` (para de aceitar, drena requisições) → consumidor SQS (para de buscar,
conclui ou devolve mensagens) → publicador do outbox → worker de pendências → pool de banco (por último).
Workers têm dois contextos: um para *agendar* (cancelado primeiro) e outro para o trabalho em andamento (cancelado
só se o prazo estourar), e a parada é observável (`worker stopped` no log; `OnStop` espera o término).
Verificado por `internal/bootstrap` (`-tags integration`): grafo válido, início/atendimento/parada, porta liberada,
nenhuma goroutine da aplicação restante (o teste falha se `pool.Close()` ou o `Shutdown` do HTTP forem removidos).

## 15. Falhas: transitórias × permanentes e códigos

| Classe | Exemplos | Tratamento |
|---|---|---|
| Entrada corrigível (nada é persistido) | JSON/valor inválido, `OPENING` externo, sem `Idempotency-Key`, carteira inexistente, conflito de chave/ID externo | HTTP 4xx; SQS → DLQ; o produtor deve corrigir |
| Resultado de negócio definitivo (persistido, terminal) | saldo insuficiente, referência inválida… | `REJECTED` + `failureCode` estável, HTTP 422, evento `WagerTransactionRejected`; idempotente em replays; SQS: ack |
| Transitória de infraestrutura | PostgreSQL/SQS indisponível, timeout, deadlock, perda de conexão | nada parcial fica gravado (transação única); HTTP 503 + `Retry-After` (reenviar com a mesma chave); SQS: retry com backoff; esgotado → DLQ |
| Permanente de infraestrutura | erro de dados/constraint não esperado ao reprocessar uma pendência | `FAILED` (auditável, terminal) |

Classificação: erros do pgx são mapeados por classe SQLSTATE (22/23/42/0A = permanente; o resto = transitório);
`app.Transient/Permanent` os marcam e `errors.Is/As` os separa dos erros de domínio.

`failureCode` (estáveis): `INSUFFICIENT_FUNDS` (BET sem saldo), `REVERSAL_INSUFFICIENT_FUNDS` (reversão que
deixaria saldo negativo — código distinto), `PLAYER_MISMATCH`, `CURRENCY_MISMATCH`, `REFERENCE_NOT_FOUND` (esgotou
tentativas/TTL), `REFERENCE_PENDING_TIMEOUT` (a referência existe mas não terminou no prazo),
`REFERENCE_NOT_SUCCESSFUL` (referência `REJECTED`/`FAILED`: rejeição imediata), `REFERENCE_MISMATCH`,
`REFERENCE_INVALID_KIND`, `REFERENCE_AMOUNT_MISMATCH`, `REFERENCE_ALREADY_REVERSED`, e `INTERNAL_ERROR` (só em `FAILED`).

## 16. Contrato HTTP (status e corpos)

| Situação | Status | Corpo |
|---|---|---|
| Processada (inclui replay) | 200 | `{transactionId,status:"PROCESSED",balance,idempotentReplay}` |
| Aguardando referência | 202 + `Location` | `{transactionId,status:"PENDING_REFERENCE",nextAttemptAt,…}` |
| Rejeição de negócio | 422 | `{transactionId,status:"REJECTED",failureCode,balance,idempotentReplay}` |
| Entrada inválida | 400 | `{error:{code:"INVALID_REQUEST"\|"MISSING_IDEMPOTENCY_KEY",message,correlationId}}` |
| Conflito | 409 | `IDEMPOTENCY_KEY_PAYLOAD_MISMATCH`, `DUPLICATE_EXTERNAL_TRANSACTION`, `WALLET_ALREADY_EXISTS` |
| Não autenticado / proibido | 401 / 403 | `UNAUTHENTICATED` / `FORBIDDEN` |
| Não encontrado | 404 | `NOT_FOUND`, `WALLET_NOT_FOUND` |
| Indisponibilidade transitória | 503 + `Retry-After` | `SERVICE_UNAVAILABLE` |

Reconciliação: `{walletId,storedBalance,calculatedBalance,difference,consistent,checkedEntries}`;
`difference = stored − calculated`; leitura em uma única transação `REPEATABLE READ READ ONLY`.

## 17. Contratos de mensageria

**Entrada** `wager-transactions.fifo` (+ `wager-transactions-dlq.fifo`, redrive `maxReceiveCount=5`):
`MessageGroupId = walletId` (ordem por carteira, paralelismo entre carteiras); `MessageDeduplicationId = messageId`
(o produtor deve enviá-lo; a aplicação **não** depende da deduplicação do FIFO — a garantia vem de inbox +
chave de idempotência + constraints). Identidade durável: `messageId` do envelope + hash (chave de idempotência +
hash do payload); reentrega com hash diferente → DLQ. Visibility timeout por recebimento: `VISIBILITY_TIMEOUT`
(padrão 60s; 20s no compose). Falha transitória: `ChangeMessageVisibility = 5s·2^(recebimentos−1)` (teto 5min); após 5
recebimentos o redrive manda à DLQ. Mensagens inválidas (JSON, tipo, `OPENING`, valores, carteira inexistente,
conflito) vão direto à DLQ com o atributo `failureReason` e só então são apagadas. A mensagem só é apagada depois do commit.

**Saída** `wager-events.fifo`: `MessageGroupId = aggregateId` (carteira), `MessageDeduplicationId = eventId`,
atributos `eventType`/`eventId`, corpo = envelope JSON do outbox. Consumidores devem ser idempotentes por `eventId`
(entrega *at-least-once*, republicação preserva o `eventId`).

## 18. Interpretações adotadas e trabalho não concluído

- Carteira inexistente é **erro de entrada** (nada persistido), pois não há carteira à qual vincular a auditoria;
  `PLAYER_MISMATCH` e `CURRENCY_MISMATCH` são rejeições persistidas.
- Operações sem dependência concluem de forma síncrona (sem commit de aceite); logo só `PENDING_REFERENCE` precisa de retomada.
- Não implementados (opcionais no enunciado): partidas dobradas, tracing OpenTelemetry, dashboards, testes de carga.
- **Não executado onde o código foi escrito (sem Docker):** `docker compose up`, Keycloak/LocalStack reais e a suíte
  `-tags e2e`. Foram executados com `-race`: testes unitários, PostgreSQL 16 real, HTTP, consumidor SQS (broker
  simulado) e ciclo de vida do Fx. `go.sum` deve ser gerado com `go mod tidy` (sem acesso ao proxy de módulos).
