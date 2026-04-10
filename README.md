# NOC Pipeline

Sincroniza eventos da API interna do NOC (MongoDB) para MySQL, permitindo consultas e relatórios na infraestrutura existente.

---

## Pré-requisitos

- Python 3.10+
- MySQL acessível com banco criado (ex: `noc`)
- Token Bearer copiado do browser (validade ~1 hora)

---

## Instalação

```bash
python -m pip install -r requirements.txt
cp .env.example .env
```

Edite `.env` com o token Bearer e as credenciais MySQL.

---

## Configuração (.env)

| Variável | Padrão | Descrição |
|---|---|---|
| `API_BASE_URL` | `http://10.215.39.31:22205` | URL base da API NOC |
| `API_TOKEN` | — | **Obrigatório.** Bearer token copiado do browser |
| `MYSQL_HOST` | `localhost` | Host do MySQL |
| `MYSQL_PORT` | `3306` | Porta do MySQL |
| `MYSQL_DB` | `noc` | Nome do banco |
| `MYSQL_USER` | — | Usuário MySQL |
| `MYSQL_PASSWORD` | — | Senha MySQL |
| `PAGE_SIZE` | `500` | Registros por página na API |
| `BATCH_SIZE` | `500` | Registros por lote de INSERT |
| `INITIAL_DATE` | `2025-01-01T00:00` | Data limite para retroalimentação |
| `BACKWARD_WINDOW_DAYS` | `7` | Dias de histórico buscados por execução (sync retroativo) |
| `DUPLICATE_THRESHOLD` | `0.9` | Fração de duplicatas numa página para parar o sync avançado (0.0–1.0) |
| `LOG_LEVEL` | `INFO` | Nível de log (`DEBUG`, `INFO`, `WARNING`) |

---

## Token Bearer

### Obter o token pela primeira vez (antes de rodar)

1. Abrir `http://10.215.39.31:22207` no browser e fazer login
2. F12 → aba **Rede** → localizar requisição `token`
3. Aba **Resposta** → copiar o valor de `access_token` (começa com `eyJra...`, ~1000 caracteres)
4. Colar em `.env`:
   ```
   API_TOKEN=eyJra...
   ```

### Renovação automática durante a execução

O token expira em ~1 hora. Quando isso acontece **o sync não cai** — ele pausa e exibe:

```
==============================================================
  TOKEN EXPIRADO — 401 Unauthorized
==============================================================
  Como obter um novo token:
    1. Abra o browser e faça login no sistema
    2. F12  →  aba Rede  →  localizar requisição 'token'
    3. Aba Resposta  →  copiar valor de 'access_token'
       (começa com eyJra..., ~1000 caracteres)
==============================================================

Cole o novo token Bearer (entrada oculta):
```

Cole o novo token e pressione Enter. O sync **retoma exatamente de onde parou** e o novo token é salvo automaticamente no `.env` para a próxima execução.

---

## Execução

```bash
python sync.py
```

Cada execução faz **duas etapas automaticamente**:

### 1. Sync avançado (novos dados)
Busca registros de `last_sync_date` até agora. Para automaticamente quando uma página atinge `DUPLICATE_THRESHOLD`% de registros já conhecidos no banco, evitando requisições desnecessárias à API.

### 2. Sync retroativo (dados históricos)
Após o sync avançado, busca uma janela de `BACKWARD_WINDOW_DAYS` dias de dados históricos ainda não importados, caminhando de volta até `INITIAL_DATE`. Uma janela por execução — repita `python sync.py` para avançar o histórico.

**Progresso do sync retroativo** fica salvo na tabela `sync_config` — interrupções não perdem o ponto de parada.

---

## Tabelas criadas automaticamente

| Tabela | Descrição |
|---|---|
| `history_io` | Eventos sincronizados da API |
| `use_cases` | Mapeamento useCase → tecnologia/vendor |
| `sync_state` | Registro de `last_sync_date` (data mais recente sincronizada) |
| `sync_config` | Estado interno do sync retroativo (`oldest_sync_date`) |

---

## Resolução de problemas

| Sintoma | Causa provável | Solução |
|---|---|---|
| Prompt de token aparece durante execução | Token expirado (normal, ~1h) | Colar novo token no prompt — sync continua automaticamente |
| `RuntimeError: Token não fornecido` | Enter pressionado sem colar token | Reiniciar o sync; o estado foi preservado |
| `Can't connect to MySQL` | Credenciais ou host errado | Verificar `MYSQL_*` no `.env` |
| Sync retroativo parado em mesma data | `oldest_sync_date` já em `INITIAL_DATE` | Ajustar `INITIAL_DATE` para data mais antiga desejada |
| Muitas requisições à API | `BACKWARD_WINDOW_DAYS` alto | Reduzir para 1–3 dias por execução |
