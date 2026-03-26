# 🌧️ Sistema de Sincronização de Dados Pluviométricos e Meteorológicos

Sincroniza dados pluviométricos e meteorológicos do banco **alertadb** (NIMBUS) para **alertadb_cor** (servidor 166) e Google BigQuery.

---

## 🚀 Início Rápido

### 1. Instalação

```bash
pip install -r requirements.txt
```

### 2. Configuração

Crie o arquivo `.env` baseado no `.env.example`:

```env
# Banco origem (alertadb)
DB_ORIGEM_HOST=...
DB_ORIGEM_NAME=alertadb
DB_ORIGEM_USER=...
DB_ORIGEM_PASSWORD=...

# Banco destino (alertadb_cor - servidor 166)
DB_DESTINO_HOST=10.50.30.166
DB_DESTINO_NAME=alertadb_cor
DB_DESTINO_USER=...
DB_DESTINO_PASSWORD=...
```

### 3. Carga Inicial

```bash
# Servidor 166 (obrigatório)
python scripts/servidor166/carregar_pluviometricos_historicos.py

# BigQuery (opcional)
python scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py
python scripts/bigquery/exportar_meteorologicos_nimbus_bigquery.py
```

### 4. Automação

```bash
cd automacao
chmod +x configurar_cron.sh cron.sh

# Sincronização normal (servidor 166)
./configurar_cron.sh normal

# Sincronização BigQuery
./configurar_cron.sh bigquery
```

---

## 📁 Estrutura

```
scripts/
├── servidor166/          # Sincronização com o servidor 166 (alertadb_cor)
│   ├── carregar_pluviometricos_historicos.py
│   ├── sincronizar_pluviometricos_novos.py
│   ├── validar_dados_pluviometricos.py
│   ├── exportar_pluviometricos_parquet.py
│   └── app.py            # API REST Flask
│
├── bigquery/             # Sincronização com o Google BigQuery
│   ├── exportar_pluviometricos_nimbus_bigquery.py
│   ├── exportar_meteorologicos_nimbus_bigquery.py
│   ├── sincronizar_pluviometricos_nimbus_bigquery.py
│   └── sincronizar_meteorologicos_nimbus_bigquery.py
│
└── prefect/              # Orquestração Prefect v3
    ├── constants.py      # SQL queries, tabelas e defaults
    ├── utils.py          # Helpers (execução de scripts, BigQuery)
    ├── tasks.py          # Tasks (conexões, sync, stub IA LNCC)
    ├── flows.py          # Flows de sincronização
    ├── interval_manager.py
    └── service.py        # Serviço contínuo (Docker/systemd)

automacao/
├── cron.sh               # Execução automática
└── configurar_cron.sh    # Configuração do cron

prefect.yaml              # Deployments Prefect v3
docs/                     # Documentação completa
```

---

## 📚 Documentação

- [Scripts BigQuery](scripts/bigquery/README.md) - Documentação dos scripts BigQuery
- [Automação](automacao/README.md) - Configuração de cron e Prefect
- [BigQuery — Guia Completo](docs/BIGQUERY_GUIA_COMPLETO.md) - Setup, queries, schema, troubleshooting
- [BigQuery — Compartilhar Acesso](docs/BIGQUERY_COMPARTILHAR_ACESSO.md) - Conceder acesso (LNCC/rj-cor)
- [Prefect — Guia Completo](docs/PREFECT_GUIA_COMPLETO.md) - Cloud, Docker e local
- [API e Dashboard](docs/API_E_DASHBOARD.md) - API REST Flask e dashboard web
- [Automação — Guia Completo](docs/AUTOMACAO_GUIA_COMPLETO.md) - Cron, systemd, APScheduler

---

## 🔧 Scripts Principais

### Servidor 166

- **carregar_pluviometricos_historicos.py** - Carga inicial completa
- **sincronizar_pluviometricos_novos.py** - Sincronização incremental
- **app.py** - API REST Flask
- **validar_dados_pluviometricos.py** - Validação de dados
- **exportar_pluviometricos_parquet.py** - Exportação para Parquet

### BigQuery

- **exportar_pluviometricos_nimbus_bigquery.py** - Carga inicial pluviométricos (NIMBUS)
- **exportar_meteorologicos_nimbus_bigquery.py** - Carga inicial meteorológicos (NIMBUS)
- **sincronizar_pluviometricos_nimbus_bigquery.py** - Sincronização incremental pluviométricos
- **sincronizar_meteorologicos_nimbus_bigquery.py** - Sincronização incremental meteorológicos

### Prefect (orquestração)

```bash
# Execução manual (uma vez, sem deployment)
python scripts/prefect/flows.py --run-once

# Execução contínua local (sem Docker)
python scripts/prefect/service.py --workflow combinado --intervalo 5

# Deploy via Prefect Cloud / servidor local
prefect deploy --all   # usa prefect.yaml na raiz
```

---

## ⚙️ Variáveis de Ambiente

### Banco Origem (alertadb)
- `DB_ORIGEM_HOST` - Host do banco origem
- `DB_ORIGEM_PORT` - Porta (padrão: 5432)
- `DB_ORIGEM_NAME` - Nome do banco (alertadb)
- `DB_ORIGEM_USER` - Usuário
- `DB_ORIGEM_PASSWORD` - Senha
- `DB_ORIGEM_SSLMODE` - Modo SSL (disable/require)

### Banco Destino (alertadb_cor - servidor 166)
- `DB_DESTINO_HOST` - Host (10.50.30.166)
- `DB_DESTINO_PORT` - Porta (padrão: 5432)
- `DB_DESTINO_NAME` - Nome do banco (alertadb_cor)
- `DB_DESTINO_USER` - Usuário
- `DB_DESTINO_PASSWORD` - Senha

### BigQuery
- `BIGQUERY_PROJECT_ID` - ID do projeto GCP
- `BIGQUERY_DATASET_ID_NIMBUS` - Dataset para dados NIMBUS (padrão: `alertadb_cor_raw`)
- `BIGQUERY_TABLE_ID` - Nome da tabela pluviométricos (padrão: `pluviometricos`)
- `BIGQUERY_TABLE_ID_METEOROLOGICOS` - Nome da tabela meteorológicos (padrão: `meteorologicos`)
- Credenciais GCP: arquivo `credentials/credentials.json` (service account)
- Convenção de data/hora (pluviométricos e meteorológicos):
  - `dia_utc` = `TIMESTAMP` em UTC (referência técnica para processamento)
  - `dia` = `DATETIME` em horário local de São Paulo (sem timezone, para leitura operacional)
  - `dia_original` = `STRING` com timestamp + offset original (`-0300`/`-0200`)

### Prefect
- `PREFECT_API_URL` - URL do servidor Prefect (deixe vazio para modo efêmero local)
- `PREFECT_WORKFLOW` - Workflow Docker: `combinado`, `pluviometricos` ou `meteorologicos`
- `PREFECT_INTERVALO` - Intervalo em minutos (padrão: `5`)

### IA LNCC / Gypscie *(futuro)*
- `GYPSCIE_API_URL` - Endpoint da API Gypscie
- `GYPSCIE_API_KEY` - Chave de autenticação
- `GYPSCIE_WORKFLOW_ID` - ID do workflow de predição
- `GYPSCIE_ENVIRONMENT_ID` - ID do ambiente

---

## 🆘 Suporte

Para problemas ou dúvidas, consulte a pasta [docs/](docs/).
