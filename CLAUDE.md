# NOC Pipeline — CLAUDE.md

## Project Overview

Sistema Python que sincroniza dados de uma API interna de NOC (Network Operations Center)
para um banco de dados MySQL. A API é baseada em MongoDB; o sistema faz a ponte para MySQL
permitindo consultas e relatórios na infraestrutura existente.

---

## Architecture

```
noc-pipelineget/
├── sync.py          # Entry point — orquestração do fluxo de sincronização bidirecional
├── api_client.py    # Comunicação com a API (GET use-cases + POST monitoring paginado)
├── db.py            # Camada de banco de dados (MySQL: tabelas, upsert batch, estado)
├── config.py        # Carrega variáveis de ambiente do .env
├── requirements.txt # Dependências Python
├── .env.example     # Template de configuração
└── .env             # Configurações reais (gitignored)
```

---

## Features

### 1. Scan Histórico (INITIAL_DATE → now, cronológico)
Cada execução avança um janela de `BACKWARD_WINDOW_DAYS` dias a partir de `forward_cursor`:
- Ordena ASC (mais antigo primeiro) — varredura cronológica de 2025 para 2026
- Sem parada antecipada: todos os registros da janela são upsertados
- `forward_cursor` persiste em `sync_config` — interrupções não perdem o ponto de parada
- Loga `% concluído` e `dias restantes` para visibilidade do progresso
- Torna-se no-op quando `forward_cursor >= now` (histórico completo)

### 2. Sync Incremental (last_sync_date → now)
Após o scan histórico, busca registros criados desde a última execução:
- Ordena DESC (mais recente primeiro) para processar dados novos imediatamente
- Para automaticamente quando uma página atinge `DUPLICATE_THRESHOLD`% de registros já no banco
- Atualiza `last_sync_date` em `sync_state` somente após sucesso completo

### 3. Detecção de Duplicatas (sync incremental)
- `db.count_existing_ids(conn, ids)`: consulta a PK de `history_io` para os IDs de cada página antes de upsertá-los
- Log por página: `N new + M already in DB (X% duplicate)`
- Se `existing/total >= DUPLICATE_THRESHOLD`, para de paginar (economia de chamadas à API)

### 3. Autenticação Bearer Manual
- Token OAuth2 Bearer copiado do browser (F12 → aba Rede → requisição `token` → Resposta)
- Configurado via variável `API_TOKEN` no `.env`
- Token expira em ~1 hora

### 4. Mapeamento de Tecnologia (use_cases)
- Busca todos os useCases via `GET /core/commons/use-cases` antes de cada sync
- Mapeia `useCase value → { network_value (tecnologia), vendor }`
- Preenche colunas `technology` e `vendor` na tabela `history_io` automaticamente

### 5. Inserção em Batch (performance)
- Acumula registros em lotes de `BATCH_SIZE` (padrão: 500)
- Usa `executemany()` + um único `commit()` por lote
- Reduz de 760k transações individuais para ~1520 transações

### 6. Paginação automática da API
- `api_client.fetch_pages(data_from, data_to)` — generator que yield `(page_num, records)`
- Itera páginas (page=0, 1, 2...) com `PAGE_SIZE` registros por página (padrão: 500)
- Para automaticamente quando a página retorna vazio ou menos que PAGE_SIZE registros
- `fetch_all_monitoring` mantida como wrapper para compatibilidade

---

## Database Tables

### `history_io` — registros sincronizados da API
| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | VARCHAR(36) PK | UUID do registro na API |
| `insert_date` | DATETIME | Data de inserção original |
| `cod_response` | INT | Código de resposta |
| `result` | TEXT | Resultado da operação |
| `msg_id` | VARCHAR(36) | UUID da mensagem |
| `ticket_id` | VARCHAR(100) | ID do ticket (indexado) |
| `use_cases` | JSON | Array de useCases |
| `type_event` | VARCHAR(50) | Tipo do evento (CREATION, RESOLUTION...) |
| `system_origin` | VARCHAR(100) | Sistema de origem (TEMS...) |
| `micro_service` | JSON | Array de microserviços |
| `technology` | VARCHAR(100) | Tecnologia de rede (lookup em use_cases) |
| `vendor` | VARCHAR(100) | Fornecedor (HUAWEI, NOKIA...) |
| `synced_at` | DATETIME | Data da última sincronização |

### `use_cases` — mapeamento useCase → tecnologia
| Coluna | Tipo | Descrição |
|---|---|---|
| `use_case` | VARCHAR(150) PK | Identificador do useCase |
| `label` | VARCHAR(255) | Nome legível |
| `network_value` | VARCHAR(100) | Tecnologia de rede |
| `network_label` | VARCHAR(100) | Nome legível da tecnologia |
| `vendor` | VARCHAR(100) | Fornecedor |

### `sync_state` — controle de sincronização incremental
| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | INT PK AUTO | Identificador |
| `last_sync_date` | DATETIME | Última data sincronizada com sucesso (forward) |

### `sync_config` — estado genérico chave-valor
| Coluna | Tipo | Descrição |
|---|---|---|
| `key_name` | VARCHAR(50) PK | Chave |
| `value` | VARCHAR(100) | Valor persistido |

Chaves em uso:
- `forward_cursor`: posição atual do scan histórico; avança `BACKWARD_WINDOW_DAYS` a cada execução bem-sucedida até atingir `now`

---

## Sync Flow (per execution)

```
run()
├── init_tables()              — cria DDLs se não existirem
├── get_db_stats()             — loga: count, insert_date range, cursors
├── get_use_cases()            — atualiza mapeamento tecnologia/vendor
├── run_historical_scan()      — varredura cronológica (INITIAL_DATE → now)
│   ├── cursor = get_forward_cursor() OR INITIAL_DATE
│   ├── if cursor >= now → skip (histórico completo)
│   ├── data_from = cursor
│   ├── data_to   = cursor + BACKWARD_WINDOW_DAYS (capped at now)
│   ├── _save_pages(..., sort_dir=None, stop_on_duplicates=False)
│   │   └── ASC order, upserta tudo na janela sem parar
│   └── set_forward_cursor(data_to)
└── run_incremental_sync()     — captura dados novos desde last_sync_date
    ├── data_from = get_last_sync_date() OR INITIAL_DATE
    ├── data_to   = now
    ├── _save_pages(..., sort_dir="DESC", stop_on_duplicates=True)
    │   └── per page: count_existing_ids → loga new/existing → se ratio >= threshold → break
    └── set_last_sync_date(now)
```

---

## API Endpoints

| Método | URL | Descrição |
|---|---|---|
| `GET` | `http://10.215.39.31:22205/core/commons/use-cases` | Lista todos os useCases com tecnologia e vendor |
| `POST` | `http://10.215.39.31:22205/core/history-io/monitoring` | Exporta eventos paginados por período |

**Auth:** Bearer token via header `Authorization: Bearer <token>`
**Token endpoint:** `POST http://10.215.39.31:22206/oauth2/token`

---

## Configuration (.env)

```env
# API
API_BASE_URL=http://10.215.39.31:22205
API_TOKEN=eyJra...    # Bearer token copiado do browser

# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DB=noc
MYSQL_USER=user
MYSQL_PASSWORD=senha

# Sync
PAGE_SIZE=500                  # registros por página na API
BATCH_SIZE=500                 # registros por lote de INSERT no MySQL
INITIAL_DATE=2025-01-01T00:00  # limite inferior do backward sync
LOG_LEVEL=INFO

# Sincronização bidirecional
BACKWARD_WINDOW_DAYS=7         # dias de histórico por execução (backward)
DUPLICATE_THRESHOLD=0.9        # fração de duplicatas para parar o forward sync (0.0–1.0)
```

---

## How to Run

```bash
# 1. Instalar dependências
python -m pip install -r requirements.txt

# 2. Configurar credenciais
cp .env.example .env
# Editar .env com token Bearer e credenciais MySQL

# 3. Executar (repita para avançar o backward sync)
python sync.py
```

---

## Token Renewal (Bearer)

1. Abrir browser → fazer login no sistema (`http://10.215.39.31:22207`)
2. F12 → aba **Rede** → clicar na requisição `token`
3. Aba **Resposta** → copiar o valor completo de `access_token` (começa com `eyJra...`, ~1000 caracteres)
4. Colar no `.env` como `API_TOKEN=<valor>`
5. Executar `python sync.py`

---

## Performance Notes

- 760k registros totais na API
- Com batch de 500: ~1520 transações MySQL (vs 760k individuais antes da otimização)
- PAGE_SIZE=500: ~1520 chamadas à API (vs 7600 com PAGE_SIZE=100)
- Forward sync com early-stop: evita repaginar dados já conhecidos
- Backward sync: `BACKWARD_WINDOW_DAYS=7` → ~28 páginas por execução (estimando 2k eventos/dia)
- Para carga retroativa agressiva sem sobrecarregar a API: aumentar `BACKWARD_WINDOW_DAYS` para 14–30
