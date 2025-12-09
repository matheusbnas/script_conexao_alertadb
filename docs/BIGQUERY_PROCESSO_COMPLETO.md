# 📊 Processo Completo: PostgreSQL (NIMBUS) → BigQuery

Guia passo a passo explicando como os dados fluem do PostgreSQL para o BigQuery.

---

## 🎯 Entendendo o Problema

### ❌ O que você PODE estar pensando:

"Quero enviar um arquivo SQL do banco NIMBUS para o BigQuery"

### ✅ O que REALMENTE acontece:

**Não existe formato "SQL" para BigQuery!**

O processo é:
1. **PostgreSQL (NIMBUS)** → Dados em tabelas (formato interno)
2. **Exportar** → Converter para arquivo (CSV/Parquet/JSON)
3. **BigQuery** → Importar arquivo → Dados em tabelas

---

## 🔄 Fluxo Completo

```
┌─────────────────────┐
│ PostgreSQL (NIMBUS) │
│   (Tabelas SQL)     │
└──────────┬──────────┘
           │
           │ [EXPORTAR]
           │ SELECT ... → Arquivo
           ↓
    ┌──────────────┐
    │   Arquivo    │
    │ CSV/Parquet/ │
    │   JSON/etc   │
    └──────┬───────┘
           │
           │ [IMPORTAR]
           │ BigQuery lê arquivo
           ↓
    ┌──────────────┐
    │   BigQuery   │
    │  (Tabelas)   │
    └──────────────┘
```

---

## 📝 Por Que Não Existe Formato "SQL"?

### **No PostgreSQL:**
- Dados estão em **tabelas relacionais**
- Formato interno do PostgreSQL (não é um arquivo)
- Você pode fazer `pg_dump` que gera arquivo `.sql` com comandos `INSERT`

### **No BigQuery:**
- Aceita arquivos de **dados tabulares**: CSV, JSON, Parquet, Avro, ORC
- **NÃO aceita:** Arquivos `.sql` com comandos `INSERT`
- **NÃO aceita:** Dumps PostgreSQL diretamente

### **Por quê?**
- BigQuery é um data warehouse (não um banco relacional)
- Precisa de dados em formato tabular/colunar
- Não executa comandos SQL de INSERT

---

## 🚀 Solução: Script Automatizado

Criei o script `scripts/bigquery/exportar_nimbus_para_bigquery.py` que faz **TUDO automaticamente**:

### O que o script faz:

1. ✅ **Conecta ao PostgreSQL (NIMBUS)**
   ```python
   conn = psycopg2.connect(**ORIGEM)
   ```

2. ✅ **Busca dados usando SQL**
   ```sql
   SELECT DISTINCT ON (...) 
   FROM estacoes_leitura ...
   ```

3. ✅ **Converte para DataFrame (Pandas)**
   ```python
   df = pd.read_sql(query, conn)
   ```

4. ✅ **Exporta para Parquet** (formato otimizado)
   ```python
   df.to_parquet('arquivo.parquet')
   ```

5. ✅ **Carrega no BigQuery automaticamente**
   ```python
   client.load_table_from_file(...)
   ```

**Você não precisa se preocupar com formatos!** O script faz tudo.

---

## 📊 Formatos: Do Banco Para Arquivo

### **Opção 1: CSV** (mais comum, mas não ideal)

```bash
# Exportar do PostgreSQL
psql -h 10.2.223.114 -U user -d alertadb \
  -c "COPY (SELECT * FROM tabela) TO STDOUT WITH CSV HEADER" \
  > dados.csv

# Importar no BigQuery
bq load --source_format=CSV projeto:dataset.tabela dados.csv
```

**Problemas:**
- ⚠️ Perde tipos de dados (tudo vira string)
- ⚠️ Mais lento
- ⚠️ Arquivo maior

### **Opção 2: Parquet** ⭐ RECOMENDADO

```python
# Exportar do PostgreSQL
import pandas as pd
import psycopg2

conn = psycopg2.connect(...)
df = pd.read_sql("SELECT * FROM tabela", conn)
df.to_parquet('dados.parquet')  # Preserva tipos!

# Importar no BigQuery
from google.cloud import bigquery
client = bigquery.Client()
client.load_table_from_file(open('dados.parquet', 'rb'), ...)
```

**Vantagens:**
- ✅ Preserva tipos de dados
- ✅ 5-10x mais rápido
- ✅ 50-80% menor
- ✅ Formato colunar (otimizado)

### **Opção 3: JSON**

```python
df.to_json('dados.json', orient='records', lines=True)
```

**Problemas:**
- ⚠️ Arquivo muito grande
- ⚠️ Mais lento que Parquet

---

## 🔍 Exemplo Prático

### **Cenário:** Você tem dados no PostgreSQL e quer no BigQuery

#### **❌ NÃO funciona:**

```sql
-- Arquivo: dados.sql
INSERT INTO tabela VALUES (1, '2024-01-01', 10.5);
INSERT INTO tabela VALUES (2, '2024-01-02', 20.3);
```

BigQuery **NÃO** aceita isso!

#### **✅ FUNCIONA:**

```csv
# Arquivo: dados.csv
dia,m05,m10,estacao,estacao_id
2024-01-01 00:00:00,10.5,20.3,Estação A,1
2024-01-02 00:00:00,15.2,25.1,Estação B,2
```

Ou melhor ainda, **Parquet** (binário, mais eficiente).

---

## 🎯 Resumo

### **Pergunta:** Como enviar dados do PostgreSQL (NIMBUS) para BigQuery em formato SQL?

### **Resposta:**

1. ❌ **Não existe formato "SQL"** para BigQuery
2. ✅ **Exporte dados** do PostgreSQL para CSV/Parquet/JSON
3. ✅ **Importe arquivo** no BigQuery
4. ✅ **Ou use o script** que faz tudo automaticamente

### **O Script Faz:**

```
PostgreSQL → Python (pandas) → Parquet → BigQuery
```

**Você só executa:**
```bash
python scripts/bigquery/exportar_nimbus_para_bigquery.py
```

**Não precisa se preocupar com formatos!** 🎉

---

## 📚 Documentação Relacionada

- [Integração BigQuery](BIGQUERY_INTEGRACAO.md) - Guia completo
- [Formatos Suportados](BIGQUERY_FORMATOS_SUPORTADOS.md) - Detalhes técnicos
- Script: `scripts/bigquery/exportar_nimbus_para_bigquery.py`

---

**Última atualização:** 2025

