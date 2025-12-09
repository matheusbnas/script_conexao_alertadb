# 📊 Exportar Dados do PostgreSQL (NIMBUS) para BigQuery

Guia completo sobre como exportar dados do banco PostgreSQL (NIMBUS) e importar no BigQuery.

---

## 🎯 Entendendo o Processo

### ❌ O que NÃO existe:
- Não existe formato "SQL" para importação no BigQuery
- BigQuery não lê arquivos `.sql` diretamente
- Não há importação direta de dump PostgreSQL

### ✅ O que existe:
1. **Exportar dados do PostgreSQL** → Arquivo (CSV/Parquet/JSON)
2. **Importar arquivo no BigQuery**
3. **Ou usar conexão direta** (Federated Query)

---

## 🔄 Processo Completo: PostgreSQL → Arquivo → BigQuery

### **Passo 1: Exportar do PostgreSQL (NIMBUS)**

Você precisa exportar os dados do PostgreSQL para um formato que o BigQuery entenda.

#### Opção A: Via `psql` (CSV)

```bash
# Conectar ao NIMBUS e exportar para CSV
psql -h 10.2.223.114 -U planejamento_cor -d alertadb -c "
COPY (
  SELECT DISTINCT ON (el.\"horaLeitura\", el.estacao_id)
    el.\"horaLeitura\" AS dia,
    elc.m05, elc.m10, elc.m15,
    elc.h01, elc.h04, elc.h24, elc.h96,
    ee.nome AS estacao,
    el.estacao_id
  FROM public.estacoes_leitura AS el
  JOIN public.estacoes_leiturachuva AS elc ON elc.leitura_id = el.id
  JOIN public.estacoes_estacao AS ee ON ee.id = el.estacao_id
  ORDER BY el.\"horaLeitura\" ASC, el.estacao_id ASC, el.id DESC
) TO STDOUT WITH CSV HEADER;
" > pluviometricos.csv
```

#### Opção B: Via Python (Parquet) ⭐ RECOMENDADO

```python
import pandas as pd
import psycopg2

# Conectar ao NIMBUS
conn = psycopg2.connect(
    host='10.2.223.114',
    database='alertadb',
    user='planejamento_cor',
    password='senha'
)

# Query
query = """
SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
    el."horaLeitura" AS dia,
    elc.m05, elc.m10, elc.m15,
    elc.h01, elc.h04, elc.h24, elc.h96,
    ee.nome AS estacao,
    el.estacao_id
FROM public.estacoes_leitura AS el
JOIN public.estacoes_leiturachuva AS elc ON elc.leitura_id = el.id
JOIN public.estacoes_estacao AS ee ON ee.id = el.estacao_id
ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
"""

# Exportar para Parquet
df = pd.read_sql(query, conn)
df.to_parquet('pluviometricos.parquet', index=False)
```

#### Opção C: Via `pg_dump` (SQL) → Converter

```bash
# Exportar estrutura e dados em SQL
pg_dump -h 10.2.223.114 -U planejamento_cor -d alertadb \
  -t estacoes_leitura -t estacoes_leiturachuva -t estacoes_estacao \
  > dump.sql

# Mas isso gera INSERT statements, não dados tabulares!
# Você precisaria processar o SQL para extrair os dados
```

**⚠️ Problema:** `pg_dump` gera arquivos SQL com comandos `INSERT`, não dados tabulares. BigQuery não entende isso diretamente.

---

### **Passo 2: Importar no BigQuery**

#### Via Console GCP:

1. **BigQuery Console** → Seu Dataset → **Create Table**
2. **Create table from:**
   - **Upload** → Escolher arquivo (CSV/Parquet/JSON)
   - **Google Cloud Storage** → Se arquivo estiver no GCS
3. **File format:** Escolher formato (CSV, Parquet, JSON, etc.)
4. **Schema:** Auto-detect ou definir manualmente
5. **Create table**

#### Via `bq` CLI:

```bash
# CSV
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --autodetect \
  seu-projeto:pluviometricos.pluviometricos \
  pluviometricos.csv

# Parquet (recomendado)
bq load \
  --source_format=PARQUET \
  --autodetect \
  seu-projeto:pluviometricos.pluviometricos \
  pluviometricos.parquet
```

#### Via Python (Script Automatizado):

```python
from google.cloud import bigquery

client = bigquery.Client(project='seu-projeto')

# Carregar Parquet
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

with open('pluviometricos.parquet', 'rb') as source_file:
    job = client.load_table_from_file(
        source_file,
        'seu-projeto.pluviometricos.pluviometricos',
        job_config=job_config
    )

job.result()  # Aguarda conclusão
```

---

## 🚀 Solução Automatizada (Já Criada)

Criei o script `scripts/bigquery/exportar_nimbus_para_bigquery.py` que faz **TUDO automaticamente**:

1. ✅ Conecta ao NIMBUS
2. ✅ Busca dados usando DISTINCT ON (mesma lógica)
3. ✅ Exporta para Parquet (formato otimizado)
4. ✅ Carrega automaticamente no BigQuery
5. ✅ Cria dataset/tabela se não existir

**Você só precisa executar:**
```bash
python scripts/bigquery/exportar_nimbus_para_bigquery.py
```

**Não precisa se preocupar com formatos!** O script faz tudo automaticamente.

---

## 📊 Formatos: PostgreSQL vs BigQuery

### **No PostgreSQL (NIMBUS):**
- Dados estão em **tabelas relacionais**
- Formato interno do PostgreSQL (não é um arquivo)
- Precisa **exportar** para um formato de arquivo

### **No BigQuery:**
- Aceita arquivos: **CSV, JSON, Parquet, Avro, ORC**
- **NÃO aceita:** Arquivos `.sql` com INSERT statements
- **NÃO aceita:** Dumps PostgreSQL diretamente

### **Conversão Necessária:**

```
PostgreSQL (tabelas)
    ↓ [EXPORTAR]
Arquivo (CSV/Parquet/JSON)
    ↓ [IMPORTAR]
BigQuery (tabelas)
```

---

## 🔄 Processo Manual Passo a Passo

Se quiser fazer manualmente:

### **1. Exportar do PostgreSQL:**

```bash
# Via psql - CSV
psql -h 10.2.223.114 -U planejamento_cor -d alertadb \
  -c "COPY (SELECT * FROM sua_view) TO STDOUT WITH CSV HEADER" \
  > dados.csv

# Via Python - Parquet (melhor)
python -c "
import pandas as pd
import psycopg2
conn = psycopg2.connect('postgresql://user:pass@host/db')
df = pd.read_sql('SELECT * FROM tabela', conn)
df.to_parquet('dados.parquet')
"
```

### **2. Fazer Upload para Cloud Storage (Opcional):**

```bash
gsutil cp dados.parquet gs://seu-bucket/dados/
```

### **3. Importar no BigQuery:**

```bash
bq load \
  --source_format=PARQUET \
  seu-projeto:dataset.tabela \
  gs://seu-bucket/dados/dados.parquet
```

---

## ✅ Recomendação

**Use o script automatizado** (`exportar_nimbus_para_bigquery.py`):

- ✅ Faz tudo automaticamente
- ✅ Usa formato Parquet (mais eficiente)
- ✅ Não precisa se preocupar com conversões
- ✅ Processa em chunks (otimiza memória)
- ✅ Cria tudo automaticamente

**Ou use Federated Queries** (se Cloud SQL já estiver configurado):
- Consulta direta do Cloud SQL no BigQuery
- Sem necessidade de exportar/importar
- Dados sempre atualizados

---

## 📝 Resumo

**Pergunta:** Como enviar dados do PostgreSQL (NIMBUS) para BigQuery em formato SQL?

**Resposta:**
1. ❌ Não existe formato "SQL" para BigQuery
2. ✅ Exporte dados do PostgreSQL para CSV/Parquet/JSON
3. ✅ Importe o arquivo no BigQuery
4. ✅ Ou use o script automatizado que faz tudo

**O script já criado faz isso automaticamente!** Você não precisa se preocupar com formatos.

---

**Última atualização:** 2025

