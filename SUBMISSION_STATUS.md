# Submission status - 2026-10-17

**VALIDAÇÃO PRINCIPAL APROVADA; ENTREGA FINAL AINDA NÃO CERTIFICADA.** Os resultados abaixo foram fornecidos pelo responsável pelo projeto em uma máquina Windows com Docker. A revisão dos requisitos oficiais e as verificações adicionais indicadas permanecem pendentes.

## Verificações confirmadas (saídas do terminal)

- `go test -count=1 ./...`: **PASS** (pacotes com testes aprovados).
- `go vet ./...`: **PASS**.
- `go vet -tags "integration e2e" ./...`: **PASS**.
- `gofmt -l` sobre os arquivos Go: **PASS** (nenhum arquivo listado).
- `go test -race -count=1 ./...` em `golang:1.25-bookworm`, com `CGO_ENABLED=1`: **PASS**.
- `go test -tags e2e -count=1 -timeout 10m ./test/integration/...`: **PASS**, 27,840 s (modo E2E padrão).
- Docker Compose: PostgreSQL e LocalStack saudáveis; migrations e inicialização das filas concluídas; três réplicas saudáveis. Keycloak iniciado.
- `/health/live`, `/health/ready` e `/metrics` nas portas 8080, 8082 e 8083: **HTTP 200**, nove verificações.
- `go.sum`: **presente** no diretório do projeto.
- Git: repositório independente inicializado em `C:/Users/pedro/Desktop/jungle-wallet`.

## Pendente ou não comprovado

1. Execução separada dos testes com tag `integration` e `TEST_DATABASE_URL` definido; sem a variável, alguns testes são ignorados.
2. Execução E2E com `E2E_COMPOSE=1`, que habilita testes de reinicialização/parada de contêineres.
3. Resultado registrado de `go mod verify`, `docker compose config --quiet` após a última revisão e auditoria final dos requisitos oficiais.
4. Revisão dos arquivos para evitar inclusão de segredos e criação do primeiro commit Git.
5. Não há certificação independente de todos os cenários operacionais e regras de negócio.

## Observações

- No Windows, `go test -race` direto foi bloqueado por `CGO_ENABLED=0` e ausência de GCC; a mesma suíte passou dentro de um contêiner Linux com CGO habilitado.
- O `internal/testsupport/db.go` cria um schema temporário aleatório para testes PostgreSQL e tenta removê-lo em `DB.Close()`. Isso foi inspecionado no código; **não substitui** uma execução comprovada da suíte de integração.
- Os documentos registram resultados observados, não uma garantia de prontidão para produção.
