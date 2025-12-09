# 📊 Integração NIMBUS/Cloud SQL → BigQuery

Guia completo para exportar dados para o BigQuery e disponibilizar para stakeholders.

---

## 🎯 Visão Geral

É **totalmente possível** exportar dados para o BigQuery! Existem várias abordagens:

### **Opção 1: NIMBUS → BigQuery (Direto)** ⭐ NOVO
- Exporta diretamente do NIMBUS para BigQuery
- Mais rápido (menos camadas)
- Script: `scripts/bigquery/exportar_nimbus_para_bigquery.py`

### **Opção 2: Cloud SQL → BigQuery (Federated Queries)**
- Consulta dados do Cloud SQL diretamente no BigQuery
- Sem necessidade de copiar dados
- Dados sempre atualizados

### **Opção 3: Cloud SQL → BigQuery (Exportação)**
- Exporta dados do Cloud SQL para BigQuery
- Dados em BigQuery (mais rápido para consultas)
- Requer sincronização periódica

---

## 🚀 Opção 1: NIMBUS → BigQuery (Direto) ⭐ RECOMENDADO

### Como Funciona:

O script faz automaticamente:
1. ✅ Conecta ao PostgreSQL (NIMBUS)
2. ✅ Busca dados usando DISTINCT ON (mesma lógica do script original)
3. ✅ Exporta para formato **Parquet** (mais eficiente que CSV/SQL)
4. ✅ Carrega automaticamente no BigQuery
5. ✅ Cria dataset/tabela se não existir

**⚠️ IMPORTANTE:** Não existe formato "SQL" para BigQuery!
- BigQuery **NÃO** aceita arquivos `.sql` com INSERT statements
- BigQuery **NÃO** aceita dumps PostgreSQL diretamente
- Você precisa **exportar** dados do PostgreSQL para CSV/Parquet/JSON primeiro
- O script faz isso **automaticamente**!

### Vantagens:
- ✅ Mais rápido (menos camadas)
- ✅ Dados sempre da fonte original
- ✅ Formato Parquet (5-10x mais rápido que CSV)
- ✅ Ideal para análises pesadas
- ✅ Processa tudo automaticamente

### Como Usar:

#### 1. Configurar .env

```env
# BigQuery
BIGQUERY_PROJECT_ID=seu-projeto-gcp
BIGQUERY_DATASET_ID=pluviometricos
BIGQUERY_TABLE_ID=pluviometricos
BIGQUERY_CREDENTIALS_PATH=/caminho/credentials.json  # Opcional
```

#### 2. Configurar Credenciais GCP

```bash
# Opção 1: Usar credenciais padrão (recomendado)
gcloud auth application-default login

# Opção 2: Usar arquivo de credenciais
# Baixar JSON do Console GCP → IAM → Service Accounts
```

#### 3. Executar Script

```bash
python scripts/bigquery/exportar_nimbus_para_bigquery.py
```

**Pronto!** Os dados estarão no BigQuery! 🎉

📚 **Mais detalhes:** [Exportação SQL → BigQuery](BIGQUERY_EXPORTACAO_SQL.md)

---

## 📊 Formatos Suportados pelo BigQuery

O BigQuery aceita os seguintes formatos:

1. ✅ **Parquet** ⭐ RECOMENDADO (mais rápido e eficiente)
2. ✅ **CSV** (comprimido ou não)
3. ✅ **JSON** (comprimido ou não)
4. ✅ **Avro**
5. ✅ **ORC**

📚 **Guia completo:** [Formatos Suportados](BIGQUERY_FORMATOS_SUPORTADOS.md)

---

## 🔄 Opção 2: Cloud SQL → BigQuery (Federated Queries)

É **totalmente possível** conectar Cloud SQL ao BigQuery! Existem duas abordagens principais:

### **Opção 1: Federated Queries (Recomendado)**
- Consulta dados do Cloud SQL diretamente no BigQuery
- Sem necessidade de copiar dados
- Dados sempre atualizados
- Ideal para consultas ad-hoc

### **Opção 2: Exportação Periódica**
- Exporta dados do Cloud SQL para BigQuery
- Dados em BigQuery (mais rápido para consultas)
- Requer sincronização periódica
- Ideal para análises pesadas

---

## 🚀 Opção 1: Federated Queries (Recomendado)

### Vantagens:
- ✅ Dados sempre atualizados (em tempo real)
- ✅ Não precisa copiar dados
- ✅ Sem custo de armazenamento no BigQuery
- ✅ Fácil de configurar

### Desvantagens:
- ⚠️ Consultas podem ser mais lentas (depende da latência)
- ⚠️ Custo por query (mas muito baixo)

### Como Configurar:

#### 1. Habilitar Cloud SQL Connection no BigQuery

```bash
# Via Console GCP
# BigQuery → Data → External Data Sources → Add Data Source
# Escolher: Cloud SQL → PostgreSQL
```

Ou via `bq` CLI:

```bash
bq mk --connection \
  --connection_type='CLOUD_SQL' \
  --properties='{"instanceId":"alertadb-cor:us-west1:alertadb-cor","database":"alertadb_cor","type":"POSTGRES"}' \
  --connection_credential='{"username":"postgres","password":"sua_senha"}' \
  --project_id=seu-projeto \
  --location=us-west1 \
  cloudsql_connection
```

#### 2. Criar Tabela Externa no BigQuery

```sql
CREATE OR REPLACE EXTERNAL TABLE `seu-projeto.dataset.pluviometricos`
WITH CONNECTION `seu-projeto.us-west1.cloudsql_connection`
OPTIONS (
  object_metadata='LIST',
  uris=['alertadb-cor:us-west1:alertadb-cor/alertadb_cor/public/pluviometricos']
);
```

#### 3. Consultar no BigQuery

```sql
-- Agora você pode consultar diretamente!
SELECT 
  dia,
  estacao,
  h24,
  COUNT(*) as total_registros
FROM `seu-projeto.dataset.pluviometricos`
WHERE dia >= '2024-01-01'
GROUP BY dia, estacao, h24
ORDER BY dia DESC;
```

---

## 📤 Opção 2: Exportação Periódica para BigQuery

### Vantagens:
- ✅ Consultas muito rápidas (dados no BigQuery)
- ✅ Ideal para análises pesadas
- ✅ Pode usar recursos do BigQuery (ML, etc.)

### Desvantagens:
- ⚠️ Dados podem estar desatualizados (depende da frequência)
- ⚠️ Custo de armazenamento no BigQuery
- ⚠️ Requer script de sincronização

### Como Configurar:

#### 1. Criar Dataset no BigQuery

```bash
bq mk --dataset --location=us-west1 seu-projeto:pluviometricos
```

#### 2. Exportar Dados do Cloud SQL para BigQuery

**Via Console GCP:**
1. Cloud SQL → Instâncias → `alertadb-cor`
2. **Export** → **Export to BigQuery**
3. Configurar:
   - Database: `alertadb_cor`
   - Table: `pluviometricos`
   - Dataset: `pluviometricos`
   - Table: `pluviometricos`

**Via `bq` CLI:**

```bash
# Exportar tabela completa
bq extract \
  --destination_format=CSV \
  --compression=GZIP \
  alertadb-cor:us-west1:alertadb_cor.pluviometricos \
  gs://seu-bucket/pluviometricos/export_*.csv

# Carregar no BigQuery
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --replace \
  seu-projeto:pluviometricos.pluviometricos \
  gs://seu-bucket/pluviometricos/export_*.csv \
  dia:TIMESTAMP,m05:NUMERIC,m10:NUMERIC,m15:NUMERIC,h01:NUMERIC,h04:NUMERIC,h24:NUMERIC,h96:NUMERIC,estacao:STRING,estacao_id:INTEGER
```

#### 3. Automatizar Exportação (Script Python)

Criar script para exportar periodicamente:

```python
from google.cloud import bigquery
from google.cloud import sql
import psycopg2

# Conectar ao Cloud SQL
conn = psycopg2.connect(
    host='34.82.95.242',
    database='alertadb_cor',
    user='postgres',
    password='senha'
)

# Ler dados
query = "SELECT * FROM pluviometricos WHERE dia >= CURRENT_DATE - INTERVAL '1 day'"
df = pd.read_sql(query, conn)

# Carregar no BigQuery
client = bigquery.Client()
table_id = 'seu-projeto.pluviometricos.pluviometricos'
df.to_gbq(table_id, project_id='seu-projeto', if_exists='append')
```

---

## 🔄 Sincronização Automática (Recomendado)

### Usar Cloud Functions + Cloud Scheduler

1. **Cloud Function** que exporta dados
2. **Cloud Scheduler** executa diariamente/horariamente
3. Dados sempre atualizados no BigQuery

---

## 📊 Comparação das Opções

| Característica | Federated Queries | Exportação Periódica |
|----------------|-------------------|----------------------|
| **Dados atualizados** | ✅ Tempo real | ⚠️ Depende da frequência |
| **Velocidade consulta** | ⚠️ Mais lento | ✅ Muito rápido |
| **Custo armazenamento** | ✅ Grátis | ⚠️ Pago |
| **Complexidade** | ✅ Simples | ⚠️ Média |
| **Ideal para** | Consultas ad-hoc | Análises pesadas |

---

## 🎯 Recomendação

### Para Stakeholders:

**Use Federated Queries** se:
- ✅ Precisam de dados sempre atualizados
- ✅ Consultas não são muito pesadas
- ✅ Querem simplicidade

**Use Exportação Periódica** se:
- ✅ Precisam de análises muito pesadas
- ✅ Velocidade é crítica
- ✅ Podem trabalhar com dados de até 1 dia

### Híbrido (Melhor dos dois mundos):

1. **Federated Queries** para consultas em tempo real
2. **Exportação diária** para análises pesadas
3. **Dashboard** no BigQuery usando dados exportados

---

## 📝 Exemplo de Consulta para Stakeholders

### No BigQuery (Federated Query):

```sql
-- Estatísticas por estação
SELECT 
  estacao,
  COUNT(*) as total_registros,
  MIN(dia) as primeira_data,
  MAX(dia) as ultima_data,
  AVG(h24) as media_precipitacao_24h,
  SUM(h24) as total_precipitacao
FROM `seu-projeto.dataset.pluviometricos`
WHERE dia >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY estacao
ORDER BY total_precipitacao DESC;
```

---

## 🔧 Próximos Passos

1. ✅ Decidir entre Federated Queries ou Exportação
2. ✅ Configurar conexão no BigQuery
3. ✅ Criar views/datasets para stakeholders
4. ✅ Configurar permissões de acesso
5. ✅ Criar dashboards (Data Studio, Looker, etc.)

---

**Última atualização:** 2025

