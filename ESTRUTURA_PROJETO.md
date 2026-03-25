# Estrutura do Projeto

## Organização de Arquivos

```
projeto/
│
├── README.md                          # Documentação principal
├── ESTRUTURA_PROJETO.md               # Este arquivo
├── requirements.txt                   # Dependências Python
├── .env                               # Configurações (criar a partir de .env.example)
├── .env.example                       # Template de configuração
├── Dockerfile                         # Imagem Docker (Prefect)
├── docker-compose.yml                 # Orquestração Docker (Prefect)
│
├── scripts/                           # Scripts principais
│   ├── servidor166/                   # Scripts para o servidor 166 (alertadb_cor)
│   │   ├── carregar_pluviometricos_historicos.py
│   │   ├── sincronizar_pluviometricos_novos.py
│   │   ├── validar_dados_pluviometricos.py
│   │   ├── exportar_pluviometricos_parquet.py
│   │   ├── app.py                     # API REST Flask
│   │   └── dashboard.html             # Dashboard web
│   │
│   ├── bigquery/                      # Scripts para Google BigQuery
│   │   ├── exportar_pluviometricos_nimbus_bigquery.py
│   │   ├── exportar_pluviometricos_servidor166_bigquery.py
│   │   ├── exportar_meteorologicos_nimbus_bigquery.py
│   │   ├── sincronizar_pluviometricos_nimbus_bigquery.py
│   │   ├── sincronizar_pluviometricos_servidor166_bigquery.py
│   │   ├── sincronizar_meteorologicos_nimbus_bigquery.py
│   │   ├── comparar_bigquery_nimbus.py
│   │   ├── verificar_duplicatas_periodo.py
│   │   └── README.md
│   │
│   └── prefect/                       # Orquestração Prefect
│       ├── constants.py               # SQL queries, tabelas e defaults
│       ├── utils.py                   # Helpers (execução de scripts, BigQuery)
│       ├── tasks.py                   # Tasks Prefect (conexões, sync, stub IA)
│       ├── flows.py                   # Flows (pluviométricos, meteorológicos, combinado)
│       ├── interval_manager.py        # Intervalo dinâmico entre execuções
│       ├── service.py                 # Serviço em loop (Docker/systemd)
│       ├── docker-entrypoint.sh       # Entrypoint Docker
│       ├── prefect.service            # Unit systemd (template)
│       └── README.md
│
├── setup/                             # Scripts de configuração inicial
│   ├── create_database.sql            # Cria tabela via dblink (PostgreSQL)
│   └── etl.py                         # ETL simples origem → destino
│
├── automacao/                         # Scripts de automação
│   ├── cron.sh                        # Execução via cron (normal|bigquery|bigquery_servidor166)
│   ├── configurar_cron.sh             # Configuração do crontab
│   ├── configurar_prefect.sh          # Configuração do Prefect local
│   ├── monitor_prefect_pluvio.sh      # Monitor do container Prefect
│   └── README.md
│
├── tests/                             # Scripts de diagnóstico
│   ├── debug_comparacao.py
│   ├── diagnosticar_inconsistencias.py
│   ├── verificar_periodo_especifico.py
│   └── verificar_registro_especifico.py
│
├── docs/                              # Documentação
│   ├── AUTOMACAO_GUIA_COMPLETO.md
│   ├── BIGQUERY_GUIA_COMPLETO.md
│   ├── BIGQUERY_AJUSTES_TIMESTAMP.md
│   ├── BIGQUERY_COMPARTILHAR_ACESSO.md
│   ├── BIGQUERY_INCONSISTENCIAS.md
│   ├── PREFECT_README.md
│   ├── PREFECT_DOCKER_TEMPO_REAL_GCP.md
│   ├── GUIA_USO_API.md
│   └── COMO_RODAR_DASHBOARD.md
│
├── exports/                           # Arquivos exportados (criado automaticamente)
│   └── pluviometricos_YYYY.parquet
│
└── logs/                              # Logs (criado automaticamente)
    └── sincronizacao_YYYYMMDD_HHMMSS.log
```

---

## Descrição das Pastas

### `scripts/servidor166/`
Scripts para sincronização com o servidor 166 (`alertadb_cor`):
- **carregar_pluviometricos_historicos.py** — carga inicial completa do NIMBUS
- **sincronizar_pluviometricos_novos.py** — sincronização incremental (usa `--once` no cron)
- **validar_dados_pluviometricos.py** — compara contagens entre origem e destino
- **exportar_pluviometricos_parquet.py** — exporta dados para formato Parquet
- **app.py** — API REST Flask para consulta dos dados
- **dashboard.html** — dashboard web para visualização

### `scripts/bigquery/`
Scripts para o Google BigQuery:
- **exportar_\*.py** — carga inicial massiva (NIMBUS ou servidor 166 → BigQuery)
- **sincronizar_\*.py** — sincronização incremental para tabelas particionadas
- **comparar_bigquery_nimbus.py** — compara contagens entre PostgreSQL e BigQuery
- **verificar_duplicatas_periodo.py** — diagnóstico de duplicatas

### `scripts/prefect/`
Orquestração via Prefect v3 (recomendado para BigQuery):
- **constants.py** — SQL queries, nomes de tabelas e valores padrão centralizados
- **utils.py** — funções auxiliares (execução de scripts, verificação de BigQuery, lacunas)
- **tasks.py** — tasks Prefect compartilhadas (conexões, sync, monitoramento, stub IA LNCC)
- **flows.py** — flows `sincronizacao_pluviometricos_flow`, `sincronizacao_meteorologicos_flow` e `sincronizacao_combinada_flow`
- **interval_manager.py** — cálculo de intervalo dinâmico baseado em tempo de execução
- **service.py** — serviço em loop contínuo para Docker/systemd

### `setup/`
Scripts de configuração inicial:
- **create_database.sql** — cria a tabela `pluviometricos` via dblink (alternativa ao Python)
- **etl.py** — ETL simples com menu interativo para sincronização completa ou incremental

### `automacao/`
Scripts de automação:
- **cron.sh** — execução via cron; aceita `normal`, `bigquery` ou `bigquery_servidor166`
- **configurar_cron.sh** — adiciona entrada no crontab automaticamente
- **configurar_prefect.sh** — configura Prefect para uso com servidor local
- **monitor_prefect_pluvio.sh** — monitora container Docker e reinicia se necessário

### `tests/`
Scripts de diagnóstico e verificação pontual:
- **diagnosticar_inconsistencias.py** — inconsistências entre origem e destino
- **verificar_periodo_especifico.py** — inspeciona registros em um intervalo de datas
- **verificar_registro_especifico.py** — inspeciona um registro individualmente

---

## Fluxo de Uso

```
1. Setup (no servidor via SSH)
   └── psql -U postgres -f setup/create_database.sql
       (ou use scripts/servidor166/carregar_pluviometricos_historicos.py)

2. Carga Inicial
   └── python scripts/servidor166/carregar_pluviometricos_historicos.py

3. Automação (escolha uma opção)
   ├── Cron:    automacao/configurar_cron.sh normal
   └── Prefect: docker compose up -d   (ver docs/PREFECT_README.md)
```

---

## Notas

- O arquivo `.env` deve estar na raiz do projeto (copie de `.env.example`)
- Logs são salvos em `logs/` (criado automaticamente)
- Credenciais GCP ficam em `credentials/credentials.json` (não versionado)
