# 🤖 Automação

Scripts para executar sincronização automaticamente via cron e Prefect.

---

## 🚀 Opções de Automação

### Opção 1: Cron (Tradicional)

**1. Carga inicial (obrigatório antes de configurar cron):**
```bash
# Servidor 166
python scripts/servidor166/carregar_pluviometricos_historicos.py

# Cloud SQL
python scripts/cloudsql/carregar_para_cloudsql_inicial.py

# BigQuery
python scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py
```

**2. Configurar cron:**
```bash
# Sincronização normal (servidor 166)
./configurar_cron.sh normal

# Sincronização Cloud SQL
./configurar_cron.sh cloudsql

# Sincronização BigQuery
./configurar_cron.sh bigquery
```

### Opção 2: Prefect (Recomendado para BigQuery)

#### Prefect Cloud ☁️ (Recomendado - executa mesmo com máquina desligada)

**Vantagens:**
- ✅ Executa mesmo quando máquina está desligada
- ✅ Interface web sempre disponível
- ✅ Monitoramento e alertas integrados
- ✅ Gratuito para uso básico

**Configuração rápida:**
```bash
# 1. Instalar Prefect
pip install prefect prefect-gcp

# 2. Fazer login no Cloud
prefect cloud login

# 3. Criar work pool no Prefect Cloud UI
# 4. Deploy do workflow
prefect deploy scripts/bigquery/prefect_workflow_bigquery.py:sincronizacao_incremental_flow --pool seu-work-pool

# 5. Iniciar agent em servidor dedicado
prefect agent start seu-work-pool
```

**📚 Documentação completa:** [../docs/PREFECT_README.md](../docs/PREFECT_README.md)

#### Prefect Local 🖥️ (só funciona com máquina ligada)

**Vantagens:**
- ✅ Sem necessidade de conta Cloud
- ✅ Controle total local

**Configuração:**
```bash
# 1. Instalar Prefect
pip install prefect prefect-gcp

# 2. Configurar servidor local
./configurar_prefect.sh

# 3. Iniciar servidor (em terminal separado)
prefect server start

# 4. Executar workflow (em outro terminal)
python scripts/bigquery/prefect_workflow_bigquery.py
```

**📚 Documentação:** [../docs/PREFECT_README.md](../docs/PREFECT_README.md)

---

## 📋 Scripts Disponíveis

### Cron
- **cron.sh** - Script unificado de execução (aceita: `normal`, `cloudsql`, `bigquery`, `bigquery_servidor166`)
- **configurar_cron.sh** - Configuração automática do cron

### Prefect
- **configurar_prefect.sh** - Configura Prefect para usar servidor local
- **Documentação:** [../docs/PREFECT_README.md](../docs/PREFECT_README.md)

---

## 📚 Documentação Completa

- [Configurar Cron](../docs/CONFIGURAR_CRON.md) - Guia completo de configuração
- [Prefect Workflow](../docs/PREFECT_README.md) - Guia do Prefect
- [README Principal](../README.md)
