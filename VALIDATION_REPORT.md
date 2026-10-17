# Relatório de validação técnica — 17/10/2026

## Escopo

Resultados baseados nas saídas de terminal apresentadas durante o desenvolvimento, em Windows/PowerShell e Docker. Esta atualização documenta execuções já realizadas; não representa uma nova execução independente dos testes.

## Resultados

| Verificação | Execução | Resultado |
|---|---|---|
| Testes Go padrão | `go test -count=1 ./...` | **PASS** |
| Análise estática | `go vet ./...` | **PASS** |
| Análise estática com tags | `go vet -tags "integration e2e" ./...` | **PASS** |
| Formatação | `gofmt -l` nos arquivos Go | **PASS**, sem arquivos listados |
| Detector de condições de corrida | `go test -race -count=1 ./...` em `golang:1.25-bookworm` com `CGO_ENABLED=1` | **PASS** |
| Integração PostgreSQL | `go test -tags integration -count=1 -v ./internal/...`, com `TEST_DATABASE_URL` definido | **PASS**, todos os pacotes executados |
| E2E padrão | `go test -tags e2e -count=1 -timeout 10m ./test/integration/...` | **PASS** (27,840 s) |
| E2E completo | `E2E_COMPOSE=1` e `go test -tags e2e -count=1 -v -timeout 10m ./test/integration/...` | **PASS** (77,280 s) |
| Integridade de dependências | `go mod verify` | **PASS** (`all modules verified`) |
| Validação Compose | `docker compose config --quiet` | **PASS** |
| Endpoints de saúde e métricas | `/health/live`, `/health/ready`, `/metrics` nas portas 8080, 8082 e 8083 | **9 × HTTP 200** |
| Inicialização da infraestrutura | `docker compose up -d --build --wait` e `docker compose ps -a` | **PASS** |

## Evidências de resiliência e integração

- Os testes de integração com PostgreSQL executaram cenários de concorrência entre instâncias, isolamento, consistência, deduplicação, recuperação de publicação e integridade de registros, todos com resultado **PASS**.
- Os testes E2E completos validaram autenticação com Keycloak, integração HTTP/SQS, processamento assíncrono, reinicialização de todas as réplicas e encerramento controlado, todos com resultado **PASS**.
- O cenário de desligamento controlado registrou `acknowledged=539 stored=539 connectionErrorsAfterStop=36`. As 539 operações confirmadas estavam armazenadas; os erros de conexão ocorreram após a parada no cenário testado.
- PostgreSQL e LocalStack foram observados como `healthy`; as três réplicas também. Os serviços de migração e inicialização de filas finalizaram com código 0.
- O health check do LocalStack foi ajustado para reconhecer os estados `running` e `available` retornados pelo serviço SQS.

## Ambiente e observações

- Módulo Go: versão declarada `go 1.22`.
- O race detector foi executado com sucesso em Linux/Docker porque a instalação Windows utilizada estava sem CGO/GCC habilitados.
- As credenciais presentes no ambiente de exemplo são destinadas ao desenvolvimento local. Uma revisão de segredos reais e uma avaliação de segurança para produção são atividades distintas dos testes automatizados apresentados.
- Os resultados demonstram os cenários efetivamente executados, sem afirmar cobertura absoluta de todos os comportamentos possíveis.
