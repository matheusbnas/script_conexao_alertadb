# 🔧 Ajustes no Script BigQuery - Usando Mesma Lógica do Servidor166

## 📋 Mudanças Realizadas

O script `exportar_nimbus_para_bigquery.py` foi ajustado para usar **exatamente a mesma lógica** do script `carregar_pluviometricos_historicos.py` do servidor166.

### ✅ Mudanças Principais

1. **Coluna `dia` como TIMESTAMP** (não mais STRING)
   - Antes: `dia` era STRING no formato `2009-02-16 02:12:20.000 -0300`
   - Agora: `dia` é TIMESTAMP (convertido para UTC, padrão BigQuery)
   - **Igual ao servidor166** que usa TIMESTAMP no PostgreSQL

2. **Query SQL Idêntica**
   - Usa a **mesma query** do servidor166:
   ```sql
   SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
       el."horaLeitura" AS "Dia",
       elc.m05, elc.m10, elc.m15, elc.h01, elc.h04, elc.h24, elc.h96,
       ee.nome AS "Estacao",
       el.estacao_id
   FROM public.estacoes_leitura AS el
   JOIN public.estacoes_leiturachuva AS elc ON elc.leitura_id = el.id
   JOIN public.estacoes_estacao AS ee ON ee.id = el.estacao_id
   ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
   ```

3. **Processamento de Timestamp**
   - Converte timezone para UTC (padrão BigQuery)
   - Mantém precisão de microsegundos
   - Usa `coerce_timestamps='us'` no Parquet

4. **Particionamento por Coluna**
   - Agora usa particionamento por coluna `dia` (TIMESTAMP)
   - Melhora performance de queries
   - Reduz custos

## 📊 Estrutura da Tabela BigQuery

```sql
CREATE TABLE pluviometricos (
    dia TIMESTAMP NOT NULL,        -- TIMESTAMP (UTC)
    m05 FLOAT64,
    m10 FLOAT64,
    m15 FLOAT64,
    h01 FLOAT64,
    h04 FLOAT64,
    h24 FLOAT64,
    h96 FLOAT64,
    estacao STRING,
    estacao_id INTEGER NOT NULL
)
PARTITION BY DATE(dia)  -- Particionamento por dia
```

## 🔄 Como Usar

1. **Criar nova tabela no BigQuery:**
   ```sql
   CREATE TABLE `seu-projeto.alertadb_cor_raw.pluviometricos` (
       dia TIMESTAMP NOT NULL,
       m05 FLOAT64,
       m10 FLOAT64,
       m15 FLOAT64,
       h01 FLOAT64,
       h04 FLOAT64,
       h24 FLOAT64,
       h96 FLOAT64,
       estacao STRING,
       estacao_id INTEGER NOT NULL
   )
   PARTITION BY DATE(dia);
   ```

2. **Executar o script:**
   ```bash
   python scripts/bigquery/exportar_nimbus_para_bigquery.py
   ```

3. **Verificar dados:**
   ```sql
   SELECT 
       dia,
       m05, m10, m15, h01, h04, h24, h96,
       estacao,
       estacao_id
   FROM `seu-projeto.alertadb_cor_raw.pluviometricos`
   WHERE dia >= '2009-02-15 22:00:00'
     AND dia <= '2009-02-18 01:00:00'
     AND estacao_id = 14
   ORDER BY dia DESC;
   ```

## ✅ Garantias

- ✅ **Mesma query SQL** do servidor166
- ✅ **Mesma estrutura de dados** (TIMESTAMP para dia)
- ✅ **Mesmos valores** (DISTINCT ON garante registro mais recente)
- ✅ **Mesma lógica de processamento**

## 🔍 Comparação

| Aspecto | Servidor166 | BigQuery (Agora) |
|---------|-------------|------------------|
| Query SQL | DISTINCT ON | ✅ Igual |
| Tipo `dia` | TIMESTAMP | ✅ TIMESTAMP |
| Timezone | Preservado | ✅ Convertido para UTC |
| Valores | DISTINCT ON | ✅ DISTINCT ON |
| Estrutura | PostgreSQL | ✅ BigQuery (equivalente) |

## 💡 Observações

- BigQuery armazena TIMESTAMP em UTC internamente
- Ao consultar, você pode usar funções do BigQuery para converter para timezone do Brasil:
  ```sql
  SELECT DATETIME(dia, "America/Sao_Paulo") as dia_brasil
  FROM `seu-projeto.alertadb_cor_raw.pluviometricos`
  ```

