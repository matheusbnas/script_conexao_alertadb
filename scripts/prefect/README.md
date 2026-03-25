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

**Guia passo a passo para rodar no Docker (instalar Docker, subir container):** **`COMO_RODAR_DOCKER.md`** (na raiz do projeto).

---

## 💻 Rodar localmente (teste ou sem Docker)

Para executar **uma vez** (sem agendamento, sem Prefect Cloud):

```bash
# Na raiz do projeto, com o venv ativado

# Sincronização combinada (pluviométricos + meteorológicos)
python scripts/prefect/flows.py --run-once

# Apenas pluviométricos
python scripts/prefect/flows.py --run-once --flow pluviometricos

# Apenas meteorológicos
python scripts/prefect/flows.py --run-once --flow meteorologicos
```

**Windows (PowerShell)** – para evitar erro de encoding no console:

```powershell
$env:PYTHONIOENCODING='utf-8'
python scripts/prefect/flows.py --run-once
```

**Execução contínua local** (loop a cada 5 min, sem Docker):

```bash
python scripts/prefect/service.py --workflow combinado --intervalo 5
```

---

## 📁 Estrutura de arquivos

```
scripts/prefect/
├── constants.py          # Constantes: SQL queries, nomes de tabelas, defaults
├── utils.py              # Helpers (executar script, verificar BigQuery, lacunas)
├── tasks.py              # Tasks Prefect (conexões, sync, monitoramento, stub IA)
├── flows.py              # Flows (pluviométricos, meteorológicos, combinado)
├── interval_manager.py   # Cálculo de intervalo dinâmico
├── service.py            # Serviço contínuo (Docker/systemd)
├── docker-entrypoint.sh  # Entrypoint Docker
├── prefect.service       # Exemplo systemd (Linux)
├── INSTALACAO_SERVICO.md # Guia de instalação do serviço
└── README.md             # Este arquivo
```

---

## 📊 Tabelas BigQuery

- **Pluviométricos**: `alertadb_cor_raw.pluviometricos`
- **Meteorológicos**: `alertadb_cor_raw.meteorologicos`
- **Predições IA** *(futuro)*: `alertadb_cor_raw.predicoes_lncc`

Scripts de sincronização:

- `scripts/bigquery/sincronizar_pluviometricos_nimbus_bigquery.py`
- `scripts/bigquery/sincronizar_meteorologicos_nimbus_bigquery.py`

---

## ⚙️ Configuração (.env)

Variáveis usadas pelos workflows:

- **Banco origem**: `DB_ORIGEM_HOST`, `DB_ORIGEM_PORT`, `DB_ORIGEM_NAME`, `DB_ORIGEM_USER`, `DB_ORIGEM_PASSWORD`, `DB_ORIGEM_SSLMODE`
- **BigQuery**: `BIGQUERY_PROJECT_ID`, `BIGQUERY_DATASET_ID_NIMBUS` (padrão: `alertadb_cor_raw`)
- **Credenciais GCP**: arquivo `credentials/credentials.json`
- **Gypscie/LNCC** *(futuro)*: `GYPSCIE_API_URL`, `GYPSCIE_API_KEY`, `GYPSCIE_WORKFLOW_ID`

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

---

## 🔄 Opcional: Deploy no Prefect Cloud

Só é necessário se quiser agendamento e UI no Prefect Cloud:

```bash
prefect cloud login

# Deploy de todos os workflows definidos no prefect.yaml
prefect deploy --all

# Ou apenas o combinado (recomendado — 1 deployment)
prefect deploy --name sincronizacao-bigquery-combinada
```

Os deployments e schedules estão definidos em `prefect.yaml` na raiz do projeto.

---

## 🤖 Integração futura: IA LNCC / Gypscie

O `tasks.py` contém a task `executar_inferencia_ia` em modo **STUB**. Quando o modelo de IA do LNCC (para predição de chuvas no COR) estiver disponível via plataforma Gypscie, basta configurar as variáveis `GYPSCIE_API_URL` e `GYPSCIE_API_KEY` no `.env` para ativar a integração automaticamente.

---

## 📖 Mais informações

- **Instalação do serviço (Docker e systemd)**: `INSTALACAO_SERVICO.md`
