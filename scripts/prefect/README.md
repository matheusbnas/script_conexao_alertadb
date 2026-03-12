# 🔄 Workflows Prefect - Sincronização BigQuery

Orquestração da sincronização incremental de dados **pluviométricos** e **meteorológicos** do NIMBUS para o BigQuery.

---

## 🐳 Rodar via Docker (recomendado)

Execução contínua **a cada 5 minutos** para atualizar os dados no BigQuery. Não usa Prefect Cloud; roda em modo local dentro do container.

### Pré-requisitos

- Docker e Docker Compose
- Arquivo `.env` na raiz com variáveis de banco e GCP
- Pasta `credentials/` com `credentials.json` (GCP)

### Comandos

```bash
# Subir o serviço (roda a cada 5 minutos)
docker-compose up -d

# Ver logs em tempo real
docker-compose logs -f prefect-service

# Parar
docker-compose down
```

### O que roda

- **Workflow padrão**: `combinado` (pluviométricos + meteorológicos em uma única execução).
- **Intervalo padrão**: 5 minutos (ajuste automático se uma execução passar de 5 min).
- **Estado**: persistido em `scripts/prefect/.prefect_service_state.json`.

### Variáveis de ambiente (Docker)

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `PREFECT_WORKFLOW` | `combinado` | `combinado`, `pluviometricos` ou `meteorologicos` |
| `PREFECT_INTERVALO` | `5` | Intervalo entre execuções (minutos) |

Exemplo com intervalo de 10 minutos:

```bash
PREFECT_INTERVALO=10 docker-compose up -d
```

Documentação detalhada do serviço: **`INSTALACAO_SERVICO.md`**.

**Guia passo a passo para rodar no Docker (instalar Docker, subir container):** **`COMO_RODAR_DOCKER.md`** (na raiz do projeto). Scripts de atalho: `subir-docker.bat` e `subir-docker.ps1`.

---

## 💻 Rodar localmente (teste ou sem Docker)

Para executar **uma vez** (sem agendamento, sem Prefect Cloud):

```bash
# Na raiz do projeto, com o venv ativado

# Sincronização combinada (pluviométricos + meteorológicos)
python scripts/prefect/prefect_workflow_combinado.py --run-once

# Apenas pluviométricos
python scripts/prefect/prefect_workflow_pluviometricos.py --run-once

# Apenas meteorológicos
python scripts/prefect/prefect_workflow_meteorologicos.py --run-once
```

**Windows (PowerShell)** – para evitar erro de encoding no console:

```powershell
$env:PYTHONIOENCODING='utf-8'
python scripts/prefect/prefect_workflow_combinado.py --run-once
```

**Execução contínua local** (loop a cada 5 min, sem Docker):

```bash
python scripts/prefect/prefect_service.py --workflow combinado --intervalo 5
```

---

## 📁 Estrutura de arquivos

```
scripts/prefect/
├── prefect_common_tasks.py              # Tasks compartilhadas (conexões)
├── prefect_helpers.py                   # Helpers (executar script, status BQ)
├── prefect_interval_manager.py         # Cálculo de intervalo dinâmico
├── prefect_service.py                  # Serviço contínuo (Docker/systemd)
├── prefect_workflow_pluviometricos.py   # Flow pluviométricos
├── prefect_workflow_meteorologicos.py   # Flow meteorológicos
├── prefect_workflow_combinado.py        # Flow combinado (1 deployment)
├── docker-entrypoint.sh                # Entrypoint Docker
├── prefect.service                     # Exemplo systemd (Linux)
├── INSTALACAO_SERVICO.md               # Guia de instalação do serviço
└── README.md                           # Este arquivo
```

---

## 📊 Tabelas BigQuery

- **Pluviométricos**: `alertadb_cor_raw.pluviometricos`
- **Meteorológicos**: `alertadb_cor_raw.meteorologicos`

Scripts de sincronização:

- `scripts/bigquery/sincronizar_pluviometricos_nimbus_bigquery.py`
- `scripts/bigquery/sincronizar_meteorologicos_nimbus_bigquery.py`

---

## ⚙️ Configuração (.env)

Variáveis usadas pelos workflows:

- **Banco origem**: `DB_ORIGEM_HOST`, `DB_ORIGEM_PORT`, `DB_ORIGEM_NAME`, `DB_ORIGEM_USER`, `DB_ORIGEM_PASSWORD`, `DB_ORIGEM_SSLMODE`
- **BigQuery**: `BIGQUERY_PROJECT_ID`, `BIGQUERY_DATASET_ID_NIMBUS` (padrão: `alertadb_cor_raw`)
- **Credenciais GCP**: arquivo `credentials/credentials.json`

---

## ⚠️ Troubleshooting

### Erro 401 Unauthorized

Se aparecer `PrefectHTTPStatusError: Client error '401 Unauthorized'`:

1. **Para rodar sem Prefect Cloud**: use `--run-once` ou Docker (o serviço já usa modo local).
2. **Para usar Prefect Cloud**: faça `prefect cloud login` e depois rode o script sem `--run-once`.

### Limite de deployments no Prefect Cloud

O plano gratuito do Prefect Cloud tem limite de **5 deployments**. Se atingir:

- Use o **flow combinado** (1 deployment para os dois tipos), ou
- Rode via **Docker** ou **`--run-once`** (não consomem deployments).

### Usar Prefect Local (servidor na máquina)

Se quiser UI e agendamento local:

1. No início do script do workflow, descomente:  
   `os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"`
2. Em um terminal: `prefect server start`
3. No outro: execute o script (ou use `prefect deploy`).

---

## 🔄 Opcional: Deploy no Prefect Cloud

Só é necessário se quiser agendamento e UI no Prefect Cloud:

```bash
prefect cloud login

# Flow combinado (recomendado – 1 deployment)
prefect deploy scripts/prefect/prefect_workflow_combinado.py:sincronizacao_combinada_flow --pool seu-work-pool

# Ou separados
prefect deploy scripts/prefect/prefect_workflow_pluviometricos.py:sincronizacao_pluviometricos_flow --pool seu-work-pool
prefect deploy scripts/prefect/prefect_workflow_meteorologicos.py:sincronizacao_meteorologicos_flow --pool seu-work-pool
```

---

## 📖 Mais informações

- **Instalação do serviço (Docker e systemd)**: `INSTALACAO_SERVICO.md`
- **Análise dos arquivos**: `ANALISE_ARQUIVOS.md`
