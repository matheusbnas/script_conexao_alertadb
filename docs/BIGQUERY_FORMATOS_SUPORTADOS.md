# 📊 BigQuery - Formatos de Arquivo Suportados

Guia completo sobre os formatos de arquivo que o BigQuery aceita para importação.

---

## ✅ Formatos Suportados

O BigQuery aceita os seguintes formatos de arquivo para importação:

### 1. **CSV** (Comma-Separated Values)
- ✅ **Suportado:** Sim
- ✅ **Comprimido:** Sim (GZIP)
- 📊 **Uso:** Dados tabulares simples
- ⚡ **Performance:** Boa
- 💾 **Tamanho:** Médio

**Exemplo:**
```csv
dia,m05,m10,m15,h01,h04,h24,h96,estacao,estacao_id
2024-01-01 00:00:00,10.5,20.3,30.1,40.2,50.0,60.5,70.0,Estação A,1
```

### 2. **JSON** (JavaScript Object Notation)
- ✅ **Suportado:** Sim
- ✅ **Comprimido:** Sim (GZIP)
- 📊 **Uso:** Dados estruturados/hierárquicos
- ⚡ **Performance:** Média
- 💾 **Tamanho:** Grande

**Exemplo:**
```json
[
  {
    "dia": "2024-01-01T00:00:00",
    "m05": 10.5,
    "m10": 20.3,
    "estacao": "Estação A",
    "estacao_id": 1
  }
]
```

### 3. **Parquet** ⭐ RECOMENDADO
- ✅ **Suportado:** Sim
- ✅ **Comprimido:** Sim (nativo)
- 📊 **Uso:** Dados tabulares grandes
- ⚡ **Performance:** **Excelente** (mais rápido)
- 💾 **Tamanho:** **Menor** (mais eficiente)
- 🎯 **Vantagens:**
  - Formato colunar (otimizado para análises)
  - Compressão automática
  - Preserva tipos de dados
  - Mais rápido para carregar

**Por que usar Parquet:**
- ✅ 5-10x mais rápido que CSV
- ✅ 50-80% menor que CSV
- ✅ Preserva tipos de dados (não precisa conversão)
- ✅ Ideal para BigQuery

### 4. **Avro**
- ✅ **Suportado:** Sim
- ✅ **Comprimido:** Sim (nativo)
- 📊 **Uso:** Dados estruturados com schema
- ⚡ **Performance:** Boa
- 💾 **Tamanho:** Médio

### 5. **ORC** (Optimized Row Columnar)
- ✅ **Suportado:** Sim
- ✅ **Comprimido:** Sim (nativo)
- 📊 **Uso:** Dados tabulares grandes
- ⚡ **Performance:** Excelente
- 💾 **Tamanho:** Menor

---

## 📊 Comparação de Formatos

| Formato | Velocidade | Tamanho | Compressão | Preserva Tipos | Recomendado |
|---------|------------|---------|------------|----------------|-------------|
| **Parquet** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ✅ Nativa | ✅ Sim | ✅ **SIM** |
| **ORC** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ✅ Nativa | ✅ Sim | ✅ Sim |
| **Avro** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ✅ Nativa | ✅ Sim | ⚠️ Médio |
| **CSV** | ⭐⭐⭐ | ⭐⭐ | ⚠️ GZIP | ❌ Não | ⚠️ Básico |
| **JSON** | ⭐⭐ | ⭐ | ⚠️ GZIP | ⚠️ Parcial | ❌ Não |

---

## 🚀 Recomendação para Seu Caso

### **Para Dados Pluviométricos:**

**Use Parquet** porque:
- ✅ Dados tabulares (perfeito para Parquet)
- ✅ Volume grande (Parquet é mais eficiente)
- ✅ Análises no BigQuery (formato colunar otimizado)
- ✅ Preserva tipos numéricos (m05, m10, etc.)
- ✅ Compressão automática (menor custo de storage)

---

## 📝 Como Usar Cada Formato

### 1. CSV

```python
from google.cloud import bigquery

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,  # Detecta schema automaticamente
)

# Ou especificar schema manualmente
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    schema=[
        bigquery.SchemaField("dia", "TIMESTAMP"),
        bigquery.SchemaField("m05", "NUMERIC"),
        # ...
    ],
)
```

### 2. JSON

```python
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True,
)
```

### 3. Parquet ⭐ RECOMENDADO

```python
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    schema=[
        bigquery.SchemaField("dia", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("m05", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("m10", "NUMERIC", mode="NULLABLE"),
        # ...
    ],
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Substitui dados
)
```

### 4. Avro

```python
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.AVRO,
)
```

### 5. ORC

```python
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.ORC,
)
```

---

## 🔄 Script Criado

Criei o script `scripts/bigquery/exportar_nimbus_para_bigquery.py` que:

- ✅ Conecta diretamente ao NIMBUS
- ✅ Exporta em formato **Parquet** (recomendado)
- ✅ Carrega automaticamente no BigQuery
- ✅ Processa em chunks (otimiza memória)
- ✅ Cria dataset/tabela automaticamente

**Uso:**
```bash
# Configurar .env
BIGQUERY_PROJECT_ID=seu-projeto-gcp
BIGQUERY_DATASET_ID=pluviometricos
BIGQUERY_TABLE_ID=pluviometricos
BIGQUERY_CREDENTIALS_PATH=/caminho/credentials.json  # Opcional

# Executar
python scripts/bigquery/exportar_nimbus_para_bigquery.py
```

---

## 📊 Tamanhos Estimados

Para 1 milhão de registros:

| Formato | Tamanho (sem compressão) | Tamanho (comprimido) |
|---------|--------------------------|----------------------|
| CSV | ~100 MB | ~20 MB (GZIP) |
| JSON | ~150 MB | ~25 MB (GZIP) |
| **Parquet** | **~15 MB** | **~15 MB** (nativo) |
| Avro | ~20 MB | ~18 MB (nativo) |
| ORC | ~12 MB | ~12 MB (nativo) |

**Parquet é 5-7x menor que CSV comprimido!**

---

## ⚡ Performance de Carga

Para 1 milhão de registros:

| Formato | Tempo de Carga |
|---------|----------------|
| CSV | ~30-60 segundos |
| JSON | ~45-90 segundos |
| **Parquet** | **~5-10 segundos** |
| Avro | ~8-15 segundos |
| ORC | ~5-10 segundos |

**Parquet é 5-10x mais rápido!**

---

## 🎯 Conclusão

**Para seus dados pluviométricos:**

1. ✅ **Use Parquet** - Mais rápido e eficiente
2. ✅ **Script já criado** - `exportar_nimbus_para_bigquery.py`
3. ✅ **Formato otimizado** - Ideal para BigQuery

**Outros formatos são suportados, mas Parquet é claramente a melhor escolha!**

---

**Última atualização:** 2025

