# NOC Pipeline — CLAUDE.md

## Project Overview

Sistema Python que sincroniza dados de uma API interna de NOC (Network Operations Center)
para um banco de dados MySQL. A API é baseada em MongoDB; o sistema faz a ponte para MySQL
permitindo consultas e relatórios na infraestrutura existente.

---

## Architecture

```
noc-pipelineget/
├── sync.py          # Entry point — orquestração do fluxo de sincronização
├── api_client.py    # Comunicação com a API (GET use-cases + POST monitoring paginado)
├── db.py            # Camada de banco de dados (MySQL: tabelas, upsert batch, sync_state)
├── config.py        # Carrega variáveis de ambiente do .env
├── requirements.txt # Dependências Python
├── .env.example     # Template de configuração
└── .env             # Configurações reais (gitignored)
```

---

## Features

### 1. Sincronização Incremental
- Controla a última data sincronizada na tabela `sync_state`
- Cada execução busca apenas dados novos (`dataFrom = last_sync_date`, `dataTo = agora`)
- Em caso de erro, `sync_state` não é atualizada — próxima execução reprocessa o mesmo período

### 2. Autenticação Bearer Manual
- Token OAuth2 Bearer copiado do browser (F12 → aba Rede → requisição `token` → Resposta)
- Configurado via variável `API_TOKEN` no `.env`
- Token expira em ~1 hora

### 3. Mapeamento de Tecnologia (use_cases)
- Busca todos os useCases via `GET /core/commons/use-cases` antes de cada sync
- Mapeia `useCase value → { network_value (tecnologia), vendor }`
- Preenche colunas `technology` e `vendor` na tabela `history_io` automaticamente

### 4. Inserção em Batch (performance)
- Acumula registros em lotes de `BATCH_SIZE` (padrão: 500)
- Usa `executemany()` + um único `commit()` por lote
- Reduz de 760k transações individuais para ~1520 transações

### 5. Paginação automática da API
- Itera páginas (page=0, 1, 2...) com `PAGE_SIZE` registros por página (padrão: 500)
- Para automaticamente quando a página retorna vazio ou menos que PAGE_SIZE registros

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
| `last_sync_date` | DATETIME | Última data sincronizada com sucesso |

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
PAGE_SIZE=500          # registros por página na API
BATCH_SIZE=500         # registros por lote de INSERT no MySQL
INITIAL_DATE=2025-01-01T00:00  # usado só na 1ª execução
LOG_LEVEL=INFO
```

---

## How to Run

```bash
# 1. Instalar dependências
python -m pip install -r requirements.txt

# 2. Configurar credenciais
cp .env.example .env
# Editar .env com token Bearer e credenciais MySQL

# 3. Executar
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
- Tempo estimado após otimização: ~10-20 min para carga completa
