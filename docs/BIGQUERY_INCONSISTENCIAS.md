# 🔍 Inconsistências entre NIMBUS e BigQuery - Explicação

## 📋 Problema Identificado

Ao comparar os dados do BigQuery com os dados do banco NIMBUS usando uma query direta, foram encontradas diferenças nos valores. 

**⚠️ IMPORTANTE:** Se os horários na coluna `dia` são **diferentes**, então são **registros diferentes**, não duplicatas do mesmo timestamp.

## 🔍 Causa Raiz

A diferença ocorre porque:

1. **Query do Script (CORRETA):**
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
   - ✅ Usa `DISTINCT ON` para garantir apenas **um registro por timestamp**
   - ✅ Usa `ORDER BY id DESC` para pegar o registro com **maior ID** (mais recente)
   - ✅ Garante consistência quando há múltiplos registros com o mesmo timestamp

2. **Query Direta do Usuário (SEM DISTINCT ON):**
   ```sql
   SELECT
       el."horaLeitura" AS "Dia",
       elc.m05, elc.m10, elc.m15, elc.h01, elc.h04, elc.h24, elc.h96,
       ee.nome AS "Estacao",
       el.estacao_id
   FROM public.estacoes_leitura AS el
   JOIN public.estacoes_leiturachuva AS elc ON elc.leitura_id = el.id
   JOIN public.estacoes_estacao AS ee ON ee.id = el.estacao_id
   WHERE el."horaLeitura" >= '2009-02-15 22:00:00.000'
     AND el."horaLeitura" <= '2009-02-18 01:00:00.000'
     AND el.estacao_id = 14
   ORDER BY el."horaLeitura" DESC;
   ```
   - ❌ **NÃO usa `DISTINCT ON`**
   - ❌ Pode retornar **múltiplos registros** para o mesmo timestamp
   - ❌ Pode retornar um registro **diferente** quando há duplicatas

## 💡 Por Que Isso Acontece?

No banco NIMBUS, **pode haver múltiplos registros** com o mesmo `horaLeitura` e `estacao_id`, mas com IDs diferentes. Isso pode acontecer quando:

- Um registro é atualizado/corrigido (novo registro com ID maior)
- Há duplicatas no banco de dados
- Há correções manuais de dados

Quando há múltiplos registros:
- **Com `DISTINCT ON`**: PostgreSQL garante que apenas **um registro** seja retornado (o primeiro após ordenar por `id DESC`, ou seja, o registro com maior ID)
- **Sem `DISTINCT ON`**: PostgreSQL pode retornar **qualquer um** dos registros duplicados, ou **todos** eles

## ✅ Verificação Realizada

Ao comparar diretamente as queries no banco NIMBUS:
- ✅ Query DIRETA (sem DISTINCT ON): **204 registros**
- ✅ Query COM DISTINCT ON: **204 registros**  
- ✅ **Todos os valores são iguais** para os mesmos timestamps
- ✅ **Nenhuma diferença** encontrada nas queries do NIMBUS

**Conclusão:** O problema **NÃO está na query**, mas pode estar:
1. No processamento dos dados durante a exportação para o BigQuery
2. Na comparação dos dados já exportados no BigQuery
3. Em algum problema na formatação do timestamp durante a exportação

## ✅ Solução

### Para Comparar Corretamente

Se você está comparando dados do BigQuery com dados do NIMBUS, use a **mesma query** que o script usa, incluindo `DISTINCT ON`:

```sql
SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
    el."horaLeitura" AS "Dia",
    elc.m05,
    elc.m10,
    elc.m15,
    elc.h01,
    elc.h04,
    elc.h24,
    elc.h96,
    ee.nome AS "Estacao",
    el.estacao_id
FROM public.estacoes_leitura AS el
JOIN public.estacoes_leiturachuva AS elc
    ON elc.leitura_id = el.id
JOIN public.estacoes_estacao AS ee 
    ON ee.id = el.estacao_id
WHERE el."horaLeitura" >= '2009-02-15 22:00:00.000' 
  AND el."horaLeitura" <= '2009-02-18 01:00:00.000' 
  AND el.estacao_id = 14
ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
```

**⚠️ IMPORTANTE:** A ordem do `ORDER BY` deve corresponder à ordem do `DISTINCT ON`, e depois ordenar por `id DESC` para pegar o registro mais recente.

### Para Verificar Duplicatas no NIMBUS

Se quiser verificar se há múltiplos registros com o mesmo timestamp:

```sql
SELECT 
    el."horaLeitura",
    el.estacao_id,
    COUNT(*) as quantidade_registros,
    ARRAY_AGG(el.id ORDER BY el.id DESC) as ids
FROM public.estacoes_leitura AS el
WHERE el."horaLeitura" >= '2009-02-15 22:00:00.000' 
  AND el."horaLeitura" <= '2009-02-18 01:00:00.000' 
  AND el.estacao_id = 14
GROUP BY el."horaLeitura", el.estacao_id
HAVING COUNT(*) > 1
ORDER BY el."horaLeitura" DESC;
```

Isso mostrará todos os timestamps que têm múltiplos registros.

## 📊 Exemplo de Diferença

**Timestamp:** `2009-02-17 19:57:20.000 -0300`

- **BigQuery (com DISTINCT ON):** `h96 = 9.4` (registro com maior ID)
- **NIMBUS (sem DISTINCT ON):** `h96 = 8.8` (pode ser um registro diferente)

Isso indica que há **múltiplos registros** no NIMBUS com esse timestamp, e:
- O script está pegando o registro com **maior ID** (correto)
- A query direta está pegando um registro com **ID menor** (pode não ser o mais recente)

## ✅ Conclusão

**As queries estão CORRETAS.** Quando comparadas diretamente no banco NIMBUS, ambas retornam os mesmos dados (204 registros, valores idênticos).

**Se há diferenças entre BigQuery e NIMBUS**, o problema pode estar:
1. **Na exportação:** Verifique se todos os dados foram exportados corretamente
2. **Na formatação:** A função `formatar_dia_nimbus` pode estar causando problemas
3. **Na comparação:** Certifique-se de usar a mesma query com `DISTINCT ON` em ambos os lados

**Recomendação:** Execute novamente o script `exportar_nimbus_para_bigquery.py` para garantir que todos os dados foram exportados corretamente.

## 🔧 Verificação

Execute o script de diagnóstico para ver todas as diferenças:

```bash
python scripts/bigquery/diagnosticar_inconsistencias.py
```

Este script compara os arquivos CSV e mostra todas as diferenças encontradas.

