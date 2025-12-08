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
│   ├── carregar_pluviometricos_historicos.py
│   │   └── Carrega TODOS os dados históricos (primeira vez)
│   │
│   ├── sincronizar_pluviometricos_novos.py
│   │   └── Sincroniza APENAS novos dados (após carga inicial)
│   │
│   ├── carregar_para_cloudsql_inicial.py
│   │   └── Carga inicial para Cloud SQL GCP
│   │
│   ├── sincronizar_para_cloudsql.py
│   │   └── Sincronização incremental para Cloud SQL GCP
│   │
│   ├── exportar_pluviometricos_parquet.py
│   │   └── Exporta dados para arquivos Parquet
│   │
│   ├── validar_dados_pluviometricos.py
│   │   └── Valida integridade dos dados entre origem e destino
│   │
│   ├── consultar_alertadb_cor.py
│   │   └── Consulta dados do banco destino
│   │
│   ├── app.py
│   │   └── API REST para consulta dos dados
│   │
│   └── dashboard.html
│       └── Dashboard web para visualização
│
├── 📂 setup/                              # Scripts de configuração/setup
│   ├── criar_usuario_postgresql.sql       # Cria usuário no PostgreSQL
│   ├── criar_banco_servidor.sql           # Cria banco via SQL (servidor)
│   ├── criar_banco_servidor.sh            # Cria banco via shell (servidor)
│   └── testar_conexao.py                  # Testa conexão com servidor
│
├── 📂 automacao/                          # Scripts de automação
│   ├── cron_linux.sh                      # Script cron para Linux
│   ├── cron_cloudsql.sh                    # Script cron para Cloud SQL
│   ├── configurar_cron_linux.sh            # Helper para configurar cron
│   ├── configurar_cron_cloudsql.sh         # Helper para configurar cron Cloud SQL
│   ├── prefect_flow.py                     # Flow Prefect
│   └── prefect_deployment.py               # Deployment Prefect
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

**Sincronização:**
- **carregar_pluviometricos_historicos.py** - Carga inicial completa
- **sincronizar_pluviometricos_novos.py** - Sincronização incremental

**Cloud SQL:**
- **carregar_para_cloudsql_inicial.py** - Carga inicial para Cloud SQL GCP
- **sincronizar_para_cloudsql.py** - Sincronização incremental para Cloud SQL GCP

**Utilitários:**
- **exportar_pluviometricos_parquet.py** - Exporta dados para formato Parquet
- **validar_dados_pluviometricos.py** - Valida integridade dos dados
- **consultar_alertadb_cor.py** - Consulta dados do banco destino

**API e Interface:**
- **app.py** - API REST Flask para consulta dos dados
- **dashboard.html** - Dashboard web para visualização

### `setup/`
Scripts de configuração inicial do sistema:
- **criar_usuario_postgresql.sql** - Cria usuário no PostgreSQL
- **criar_banco_servidor.sql** - Script SQL para criar banco no servidor
- **criar_banco_servidor.sh** - Script shell para criar banco no servidor
- **testar_conexao.py** - Testa conexão com o servidor

### `automacao/`
Scripts para automatizar a execução:
- **cron_linux.sh** - Para usar com cron no Linux
- **cron_cloudsql.sh** - Para usar com cron para Cloud SQL
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
   └── python scripts/carregar_pluviometricos_historicos.py

3. Automação (escolha uma)
   ├── Linux: automacao/cron_linux.sh
   └── Prefect: automacao/prefect_flow.py
```

---

## 📝 Notas Importantes

- Todos os scripts de automação estão configurados para usar caminhos relativos
- Os scripts principais estão em `scripts/` e podem ser executados de qualquer lugar
- Os logs são salvos em `logs/` na raiz do projeto
- O arquivo `.env` deve estar na raiz do projeto

