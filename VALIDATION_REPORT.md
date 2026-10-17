# Technical validation report - 2026-10-17

## Escopo e proveniência

Relatório atualizado a partir das saídas de terminal fornecidas pelo responsável pelo projeto. Os resultados foram observados no Windows/PowerShell e, para o race detector, em um contêiner Linux. Não foi realizada nova execução independente durante a edição deste documento. O código de negócio não foi alterado nesta atualização.

## Resultados confirmados

| Verificação | Comando / evidência | Resultado |
|---|---|---|
| Testes Go padrão | `go test -count=1 ./...` | PASS |
| Análise estática | `go vet ./...` | PASS |
| Análise com tags | `go vet -tags "integration e2e" ./...` | PASS |
| Formatação | `gofmt -l` nos arquivos `.go` | PASS (sem saída) |
| Race detector | `docker run --rm -v "${PWD}:/workspace" -w /workspace -e CGO_ENABLED=1 golang:1.25-bookworm sh -c "go test -race -count=1 ./..."` | PASS |
| E2E padrão | `go test -tags e2e -count=1 -timeout 10m ./test/integration/...` | PASS (27,840 s) |
| Saúde das réplicas | `/health/live`, `/health/ready`, `/metrics` em 8080/8082/8083 | 9 × HTTP 200 |
| Infraestrutura | `docker compose ps -a` e logs observados | PostgreSQL e LocalStack saudáveis; três apps saudáveis; migrations e filas inicializadas; Keycloak iniciado |
| Git | `git rev-parse --show-toplevel` | `C:/Users/pedro/Desktop/jungle-wallet` |

O arquivo `go.sum` existe no projeto. O Docker Compose teve o healthcheck do LocalStack ajustado para aceitar o estado real do serviço SQS.

## Verificações ainda pendentes

- **Integração PostgreSQL separada:** nenhum resultado apresentado de `go test -tags integration ...` com `TEST_DATABASE_URL` definido. `testsupport.Setup()` cria um schema `t_...` e `DB.Close()` tenta removê-lo; o isolamento foi inspecionado, mas a execução ainda não foi comprovada.
- **E2E com alterações de contêineres:** `E2E_COMPOSE=1` não foi usado na execução E2E apresentada. Portanto, os testes que reiniciam/paralisam réplicas permanecem sem confirmação.
- **Integridade e empacotamento:** resultados finais de `go mod verify`, revisão de credenciais, commit inicial e aderência integral ao enunciado oficial não foram fornecidos.
- **Produção:** os testes aprovados não constituem auditoria de segurança ou certificação operacional.

## Ambiente e limitações

- Go do módulo: `go 1.22`; race detector executado na imagem `golang:1.25-bookworm`.
- No host Windows, `CGO_ENABLED=0` e `gcc` ausente impediram o race detector nativo; a alternativa via Docker passou.
- A suíte E2E padrão utiliza PostgreSQL, LocalStack/SQS, Keycloak e três réplicas do serviço.

## Alterações desta revisão

Somente documentação: `SUBMISSION_STATUS.md`, `VALIDATION_REPORT.md` e a seção de validação do `README.md`. Não foram modificados código Go, migrations, Docker Compose ou regras de negócio.
