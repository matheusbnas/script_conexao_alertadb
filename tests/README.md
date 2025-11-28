# 🧪 Scripts de Teste e Diagnóstico

Esta pasta contém scripts auxiliares para teste, diagnóstico e investigação de problemas.

---

## 📋 Arquivos

### `diagnosticar_inconsistencias.py`
**Propósito:** Diagnostica inconsistências entre banco origem e destino.

**Uso:**
```bash
python tests/diagnosticar_inconsistencias.py [quantidade]
```

**Exemplo:**
```bash
python tests/diagnosticar_inconsistencias.py 50
```

**O que faz:**
- Compara uma amostra aleatória de registros entre origem e destino
- Identifica quais registros têm valores diferentes
- Mostra detalhadamente quais campos estão divergentes
- Sugere como corrigir

---

### `debug_comparacao.py`
**Propósito:** Script de debug para verificar exatamente o que está sendo retornado do banco origem e destino.

**Uso:**
```bash
python tests/debug_comparacao.py
```

**O que faz:**
- Compara um registro específico entre origem e destino
- Mostra os tipos de dados retornados (float vs Decimal)
- Compara valores campo a campo com detalhes
- Útil para entender diferenças de tipos e valores

---

### `verificar_periodo_especifico.py`
**Propósito:** Verifica um período específico comparando origem e destino.

**Uso:**
```bash
python tests/verificar_periodo_especifico.py [data_inicial] [data_final] [estacao_id]
```

**Exemplo:**
```bash
python tests/verificar_periodo_especifico.py '2009-10-27 23:00:00.000' '2009-10-28 01:00:00.000' 11
```

**O que faz:**
- Compara todos os registros de um período específico
- Mostra quais registros estão corretos e quais têm divergências
- Exibe estatísticas do período

---

### `verificar_registro_especifico.py`
**Propósito:** Verifica um registro específico em detalhes.

**Uso:**
```bash
python tests/verificar_registro_especifico.py
```

**O que faz:**
- Mostra todos os registros no banco origem para um timestamp
- Mostra qual registro o DISTINCT ON selecionaria
- Compara com o registro no banco destino
- Útil para debug rápido

---

## 💡 Quando Usar

Use estes scripts quando:
- ✅ Encontrar divergências entre origem e destino
- ✅ Precisar investigar um problema específico
- ✅ Validar dados após uma carga ou correção
- ✅ Diagnosticar problemas de sincronização

---

## ⚠️ Importante

Estes scripts são **apenas para diagnóstico e teste**. Eles não modificam dados.

Para corrigir problemas, use:
- `scripts/carregar_pluviometricos_historicos.py` - Recarrega todos os dados
- `scripts/corrigir_dados_pluviometricos.py` - Corrige período específico

---

## 📚 Scripts Principais

Os scripts principais estão em `scripts/`:
- `carregar_pluviometricos_historicos.py` - Carga inicial
- `sincronizar_pluviometricos_novos.py` - Sincronização incremental
- `app.py` - API REST
- `validar_dados_pluviometricos.py` - Validação completa
- `corrigir_dados_pluviometricos.py` - Correção de dados

---

## ✅ Alinhamento com Script Principal

Todos os scripts de teste seguem a mesma lógica do script principal `carregar_pluviometricos_historicos.py`.

### 1. DISTINCT ON ✅

**Script Principal:**
```sql
SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
    ...
ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
```

**Scripts de Teste:**
- ✅ `diagnosticar_inconsistencias.py` - Usa DISTINCT ON com ORDER BY correto
- ✅ `verificar_periodo_especifico.py` - Usa DISTINCT ON com ORDER BY correto
- ✅ `verificar_registro_especifico.py` - Usa DISTINCT ON com ORDER BY correto
- ✅ `debug_comparacao.py` - Usa DISTINCT ON com ORDER BY correto

**Status:** Todos alinhados ✅

---

### 2. Comparação de Valores ✅

**Script Principal:**
- Compara valores diretamente (m05, m10, m15, h01, h04, h24, h96)
- Usa `ON CONFLICT DO UPDATE` para garantir valores corretos

**Scripts de Teste:**
- ✅ `diagnosticar_inconsistencias.py` - Normaliza valores (float/Decimal) e usa tolerância de 0.0001
- ✅ `verificar_periodo_especifico.py` - Normaliza valores e usa tolerância de 0.0001
- ✅ `verificar_registro_especifico.py` - Normaliza valores e usa tolerância de 0.0001
- ✅ `debug_comparacao.py` - Normaliza valores e usa tolerância de 0.0001

**Status:** Todos alinhados com normalização e tolerância ✅

---

### 3. Tratamento de Timezone ✅

**Script Principal:**
- Preserva timezone original (-02:00 ou -03:00)
- Configura `SET timezone = 'America/Sao_Paulo'` antes de inserir
- Usa `garantir_datetime_com_timezone()` para preservar timezone

**Scripts de Teste:**
- ✅ `diagnosticar_inconsistencias.py` - Normaliza timestamp para comparação (remove timezone)
- ✅ `verificar_periodo_especifico.py` - Normaliza timestamp para comparação (remove timezone), considera timezone original
- ✅ `verificar_registro_especifico.py` - Compara timestamps diretamente
- ✅ `debug_comparacao.py` - Compara timestamps diretamente

**Status:** Todos tratam timezone corretamente para comparação ✅

---

### 4. Query Structure ✅

**Script Principal:**
```sql
SELECT DISTINCT ON (el."horaLeitura", el.estacao_id)
    el."horaLeitura" AS "Dia",
    elc.m05, elc.m10, elc.m15,
    elc.h01, elc.h04, elc.h24, elc.h96,
    ee.nome AS "Estacao",
    el.estacao_id
FROM public.estacoes_leitura AS el
JOIN public.estacoes_leiturachuva AS elc ON elc.leitura_id = el.id
JOIN public.estacoes_estacao AS ee ON ee.id = el.estacao_id
ORDER BY el."horaLeitura" ASC, el.estacao_id ASC, el.id DESC;
```

**Scripts de Teste:**
- ✅ Todos usam a mesma estrutura de JOIN
- ✅ Todos usam DISTINCT ON com mesma ordem
- ✅ Todos usam ORDER BY correto (ASC, ASC, DESC)

**Status:** Todos alinhados ✅

---

## 📊 Resumo de Correções Aplicadas

### `verificar_periodo_especifico.py`
- ✅ Corrigido ORDER BY de `DESC` para `ASC`
- ✅ Adicionada normalização de valores com tolerância para floats
- ✅ Melhorada comparação para evitar falsos positivos

### `verificar_periodo_especifico.py`
- ✅ Ajustada busca para considerar timezone original (-0200 e -0300)
- ✅ Busca no origem usando timestamp do destino como referência
- ✅ Usa intervalo de tempo para encontrar registro correto mesmo com diferença de timezone

### `verificar_registro_especifico.py`
- ✅ Adicionada normalização de valores com tolerância para floats
- ✅ Comparação completa de todos os campos (não apenas h24)
- ✅ Melhorada comparação para evitar falsos positivos

### `diagnosticar_inconsistencias.py`
- ✅ Já estava usando normalização e tolerância corretamente
- ✅ Nenhuma correção necessária

---

## ✅ Conclusão

Todos os scripts de teste estão agora **100% alinhados** com a lógica do script principal:

1. ✅ Usam DISTINCT ON corretamente
2. ✅ Usam ORDER BY correto (ASC, ASC, DESC)
3. ✅ Normalizam valores para comparação (float/Decimal)
4. ✅ Usam tolerância de 0.0001 para comparação de floats
5. ✅ Tratam timezone corretamente para comparação
6. ✅ Seguem a mesma estrutura de query

Os scripts de teste agora refletem exatamente o comportamento do script principal e podem ser usados com confiança para validar a integridade dos dados.

