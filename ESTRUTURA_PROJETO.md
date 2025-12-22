# 📁 Estrutura do Projeto

## Organização de Arquivos

```
projeto/
│
├── 📄 README.md                          # Documentação principal
├── 📄 requirements.txt                    # Dependências Python
├── 📄 .env                                # Configurações (criar manualmente)
│
├── 📂 scripts/                            # Scripts principais de sincronização
│   ├── servidor166/                       # Scripts para máquina virtual (servidor 166)
│   │   ├── carregar_pluviometricos_historicos.py
│   │   ├── sincronizar_pluviometricos_novos.py
│   │   ├── validar_dados_pluviometricos.py
│   │   ├── exportar_pluviometricos_parquet.py
│   │   ├── app.py
│   │   └── dashboard.html
│   │
│   ├── cloudsql/                          # Scripts para Cloud SQL GCP
│   │   ├── carregar_para_cloudsql_inicial.py
│   │   └── sincronizar_para_cloudsql.py
│   │
│   └── bigquery/                          # Scripts para Google BigQuery
│       ├── exportar_pluviometricos_nimbus_bigquery.py
│       ├── exportar_pluviometricos_servidor166_bigquery.py
│       ├── exportar_meteorologicos_nimbus_bigquery.py
│       ├── sincronizar_pluviometricos_nimbus_bigquery.py
│       ├── sincronizar_pluviometricos_servidor166_bigquery.py
│       ├── comparar_bigquery_nimbus.py
│       ├── verificar_duplicatas_periodo.py
│       ├── diagnosticar_inconsistencias.py
│       └── README.md
│
├── 📂 setup/                              # Scripts de configuração/setup
│   ├── criar_usuario_postgresql.sql       # Cria usuário no PostgreSQL
│   ├── criar_banco_servidor.sql           # Cria banco via SQL (servidor)
│   ├── criar_banco_servidor.sh            # Cria banco via shell (servidor)
│   └── testar_conexao.py                  # Testa conexão com servidor
│
├── 📂 automacao/                          # Scripts de automação
│   ├── cron.sh                            # Script unificado de cron (normal|cloudsql|bigquery|bigquery_servidor166)
│   └── configurar_cron.sh                 # Script unificado de configuração (normal|cloudsql|bigquery|bigquery_servidor166)
│
├── 📂 docs/                               # Documentação
│   ├── OPCOES_AUTOMACAO.md                 # Opções de automação
│   ├── CONFIGURAR_CRON.md                  # Como configurar cron
│   ├── GUIA_USO_API.md                     # Guia de uso da API
│   ├── GUIA_CRIACAO_USUARIO.md            # Guia de criação de usuário
│   ├── GUIA_RAPIDO_CLOUD_SQL.md            # Guia rápido Cloud SQL
│   ├── INTEGRACAO_CLOUD_SQL.md             # Integração Cloud SQL
│   ├── README_CLOUD_SQL.md                 # README Cloud SQL
│   ├── CONFIGURACAO_EXEMPLO.md             # Exemplo de configuração
│   └── COMO_RODAR_DASHBOARD.md            # Como rodar o dashboard
│
├── 📂 exports/                            # Arquivos exportados (criado automaticamente)
│   └── pluviometricos_YYYY.parquet        # Arquivos Parquet exportados
│   └── pluviometricos_export_*.zip        # Arquivos ZIP compactados
│
├── 📂 tests/                              # Scripts de teste e diagnóstico
│   ├── diagnosticar_inconsistencias.py
│   ├── verificar_periodo_especifico.py
│   └── verificar_registro_especifico.py
│
└── 📂 logs/                               # Logs (criado automaticamente)
    └── sincronizacao_YYYYMMDD_HHMMSS.log
```

---

## 📋 Descrição das Pastas

### `scripts/`
Scripts principais que fazem a sincronização e manipulação de dados:

**servidor166/** - Scripts para máquina virtual (servidor 166):
- **carregar_pluviometricos_historicos.py** - Carga inicial completa
- **sincronizar_pluviometricos_novos.py** - Sincronização incremental
- **validar_dados_pluviometricos.py** - Valida integridade dos dados
- **exportar_pluviometricos_parquet.py** - Exporta dados para formato Parquet
- **app.py** - API REST Flask para consulta dos dados
- **dashboard.html** - Dashboard web para visualização

**cloudsql/** - Scripts para Cloud SQL GCP:
- **carregar_para_cloudsql_inicial.py** - Carga inicial para Cloud SQL GCP
- **sincronizar_para_cloudsql.py** - Sincronização incremental para Cloud SQL GCP

**bigquery/** - Scripts para Google BigQuery:
- **exportar_pluviometricos_nimbus_bigquery.py** - Carga inicial de dados pluviométricos do NIMBUS
- **exportar_pluviometricos_servidor166_bigquery.py** - Carga inicial de dados pluviométricos do servidor 166
- **exportar_meteorologicos_nimbus_bigquery.py** - Carga inicial de dados meteorológicos do NIMBUS
- **sincronizar_pluviometricos_nimbus_bigquery.py** - Sincronização incremental pluviométricos (NIMBUS)
- **sincronizar_pluviometricos_servidor166_bigquery.py** - Sincronização incremental pluviométricos (servidor 166)
- **comparar_bigquery_nimbus.py** - Compara dados entre BigQuery e NIMBUS
- **verificar_duplicatas_periodo.py** - Verifica duplicatas em período específico
- **diagnosticar_inconsistencias.py** - Diagnostica inconsistências nos dados

### `setup/`
Scripts de configuração inicial do sistema:
- **criar_usuario_postgresql.sql** - Cria usuário no PostgreSQL
- **criar_banco_servidor.sql** - Script SQL para criar banco no servidor
- **criar_banco_servidor.sh** - Script shell para criar banco no servidor
- **testar_conexao.py** - Testa conexão com o servidor

### `automacao/`
Scripts para automatizar a execução:
- **cron.sh** - Script unificado que aceita parâmetro `normal`, `cloudsql`, `bigquery` ou `bigquery_servidor166`
- **configurar_cron.sh** - Script unificado de configuração que aceita parâmetro `normal`, `cloudsql`, `bigquery` ou `bigquery_servidor166`
- **prefect_flow.py** - Para usar com Prefect
- **configurar_cron_linux.sh** - Helper para configurar cron Linux
- **configurar_cron_cloudsql.sh** - Helper para configurar cron Cloud SQL

### `docs/`
Documentação adicional do projeto:
- **OPCOES_AUTOMACAO.md** - Opções de automação disponíveis
- **CONFIGURAR_CRON.md** - Como configurar cron/agendador
- **GUIA_USO_API.md** - Guia completo de uso da API REST
- **GUIA_CRIACAO_USUARIO.md** - Como criar usuários no PostgreSQL
- **GUIA_RAPIDO_CLOUD_SQL.md** - Guia rápido Cloud SQL
- **INTEGRACAO_CLOUD_SQL.md** - Documentação de integração Cloud SQL
- **README_CLOUD_SQL.md** - README específico Cloud SQL
- **CONFIGURACAO_EXEMPLO.md** - Exemplo de configuração
- **COMO_RODAR_DASHBOARD.md** - Como rodar o dashboard

### `exports/`
Arquivos exportados (criados automaticamente):
- Arquivos Parquet exportados da tabela pluviometricos
- Arquivos ZIP compactados para backup/transferência

### `tests/`
Scripts de teste e diagnóstico:
- **diagnosticar_inconsistencias.py** - Diagnostica inconsistências
- **verificar_periodo_especifico.py** - Verifica período específico
- **verificar_registro_especifico.py** - Verifica registro específico

---

## 🔄 Fluxo de Uso

```
1. Setup (no servidor via SSH)
   └── ssh servicedesk@10.50.30.166
   └── psql -U postgres -f setup/criar_banco_servidor.sql

2. Carga Inicial
   └── python scripts/servidor166/carregar_pluviometricos_historicos.py

3. Automação
   └── Linux: automacao/configurar_cron.sh normal
```

---

## 📝 Notas Importantes

- Todos os scripts de automação estão configurados para usar caminhos relativos
- Os scripts principais estão em `scripts/` e podem ser executados de qualquer lugar
- Os logs são salvos em `logs/` na raiz do projeto
- O arquivo `.env` deve estar na raiz do projeto

