# 📊 Scripts BigQuery

Scripts para exportar e sincronizar dados pluviométricos e meteorológicos para Google BigQuery.

---

## 📋 Scripts Disponíveis

### **🌧️ Dados Pluviométricos**

#### **Opção 1: NIMBUS → BigQuery (Direto)**

##### `exportar_pluviometricos_nimbus_bigquery.py`
- **Função:** Carga inicial completa de dados pluviométricos do NIMBUS para BigQuery
- **Uso:** Executar uma vez para carregar todos os dados históricos
- **Colunas de chuva:** `m05`, `m10`, `m15`, `h01`, `h02`, `h03`, `h04`, `h06`, `h12`, `h24`, `h96`, `mes`
- **Colunas de data:** `dia_utc` (TIMESTAMP UTC), `dia` (DATETIME SP), `dia_original` (STRING com offset)

##### `sincronizar_pluviometricos_nimbus_bigquery.py`
- **Função:** Sincronização incremental de dados pluviométricos do NIMBUS para BigQuery
- **Uso:** Executar via Prefect (flows.py / service.py)
- **Colunas de chuva:** `m05`, `m10`, `m15`, `h01`, `h02`, `h03`, `h04`, `h06`, `h12`, `h24`, `h96`, `mes`
- **Colunas de data:** `dia_utc` (TIMESTAMP UTC), `dia` (DATETIME SP), `dia_original` (STRING com offset)

#### **Opção 2: Servidor 166 → BigQuery (Com Controle Administrativo)**

##### `exportar_pluviometricos_servidor166_bigquery.py`
- **Função:** Carga inicial completa de dados pluviométricos do servidor 166 para BigQuery
- **Uso:** Executar uma vez para carregar todos os dados históricos
- **Vantagem:** Você tem controle total dos dados (admin do banco)
- **Coluna `dia`:** TIMESTAMP (UTC) no BigQuery, preservando timezone original

##### `sincronizar_pluviometricos_servidor166_bigquery.py`
- **Função:** Sincronização incremental de dados pluviométricos do servidor 166 para BigQuery
- **Uso:** Executar manualmente sob demanda
- **Vantagem:** Você tem controle total dos dados (admin do banco)
- **Coluna `dia`:** TIMESTAMP (UTC) no BigQuery, preservando timezone original

---

### **🌤️ Dados Meteorológicos**

#### `exportar_meteorologicos_nimbus_bigquery.py`
- **Função:** Carga inicial completa de dados meteorológicos do NIMBUS para BigQuery
- **Uso:** Executar uma vez para carregar todos os dados históricos
- **Campos:** chuva, dirVento, velVento, temperatura, pressao, umidade
- **Coluna `dia`:** TIMESTAMP (UTC) no BigQuery, preservando timezone original

---

## 🎯 Qual Opção Escolher?

### **Opção 1: NIMBUS → BigQuery (Direto)**
✅ **Use quando:**
- Quer dados direto da fonte original
- Não precisa fazer tratamentos intermediários
- Quer menos camadas (mais rápido)

### **Opção 2: Servidor 166 → BigQuery**
✅ **Use quando:**
- Quer controle total dos dados (você é admin do banco)
- Precisa fazer tratamentos antes de exportar
- Quer validar dados antes de enviar ao BigQuery
- Processo mais longo mas com controle

---

## 📊 Formato da Coluna `dia`

**Padrão atual dos scripts NIMBUS → BigQuery:**

```
dia_utc: TIMESTAMP (UTC)
dia: DATETIME (horário local de São Paulo, sem timezone)
dia_original: STRING (formato original com offset: -0300/-0200)
```

**Características:**
- ✅ `dia_utc` como referência técnica e particionamento
- ✅ `dia` em horário local SP para leitura operacional
- ✅ `dia_original` com formato exato da NIMBUS (`2009-02-16 02:12:20.000 -0300`)
- ✅ Preserva timezone original na coluna `dia_original`
- ✅ Facilita consultas operacionais sem conversão de fuso

---

## 🚀 Como Usar

### **🌧️ Dados Pluviométricos - Carga Inicial (Escolha uma opção):**

#### Opção 1: NIMBUS → BigQuery
```bash
python scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py
```

#### Opção 2: Servidor 166 → BigQuery
```bash
python scripts/bigquery/exportar_pluviometricos_servidor166_bigquery.py
```

### **🌧️ Dados Pluviométricos - Sincronização Incremental (Prefect recomendado):**

#### Opção 1: Prefect (Recomendado - com monitoramento de erros)
```bash
# Configurar Prefect
cd automacao
./configurar_prefect.sh

# Iniciar servidor Prefect (em terminal separado)
prefect server start

# Executar workflow (em outro terminal)
python scripts/prefect/flows.py --run-once
```
**Vantagens:** Interface web, detecção automática de erros, retry automático, logs estruturados  
**Documentação:** [../../docs/PREFECT_GUIA_COMPLETO.md](../../docs/PREFECT_GUIA_COMPLETO.md)

#### Opção 2: NIMBUS → BigQuery (manual, fallback)
```bash
# Testar manualmente
python scripts/bigquery/sincronizar_pluviometricos_nimbus_bigquery.py --once
```

#### Opção 3: Servidor 166 → BigQuery (manual, fallback)
```bash
# Testar manualmente
python scripts/bigquery/sincronizar_pluviometricos_servidor166_bigquery.py --once
```

### **🌤️ Dados Meteorológicos - Carga Inicial:**

```bash
python scripts/bigquery/exportar_meteorologicos_nimbus_bigquery.py
```

---

## ⚙️ Configuração

### Variáveis Obrigatórias no `.env`

#### Para NIMBUS → BigQuery (Pluviométricos):
```env
# Banco NIMBUS
DB_ORIGEM_HOST=10.2.223.114
DB_ORIGEM_NAME=alertadb
DB_ORIGEM_USER=planejamento_cor
DB_ORIGEM_PASSWORD=sua_senha

# BigQuery (NIMBUS → BigQuery)
BIGQUERY_PROJECT_ID=alertadb-cor
BIGQUERY_DATASET_ID_NIMBUS=alertadb_cor_raw
BIGQUERY_TABLE_ID=pluviometricos
```

#### Para NIMBUS → BigQuery (Meteorológicos):
```env
# Banco NIMBUS
DB_ORIGEM_HOST=10.2.223.114
DB_ORIGEM_NAME=alertadb
DB_ORIGEM_USER=planejamento_cor
DB_ORIGEM_PASSWORD=sua_senha

# BigQuery (NIMBUS → BigQuery)
BIGQUERY_PROJECT_ID=alertadb-cor
BIGQUERY_DATASET_ID_NIMBUS=alertadb_cor_raw
BIGQUERY_TABLE_ID_METEOROLOGICOS=meteorologicos
```

#### Para Servidor 166 → BigQuery (Pluviométricos):
```env
# Banco Servidor 166
DB_DESTINO_HOST=localhost
DB_DESTINO_NAME=alertadb_cor
DB_DESTINO_USER=postgres
DB_DESTINO_PASSWORD=sua_senha

# BigQuery (Servidor 166 → BigQuery)
BIGQUERY_PROJECT_ID=alertadb-cor
BIGQUERY_DATASET_ID_SERVIDOR166=alertadb_166_raw
BIGQUERY_TABLE_ID=pluviometricos
```

---

## 📚 Documentação

- **Guia Completo:** [docs/BIGQUERY_GUIA_COMPLETO.md](../../docs/BIGQUERY_GUIA_COMPLETO.md)
- **Compartilhar Acesso:** [docs/BIGQUERY_COMPARTILHAR_ACESSO.md](../../docs/BIGQUERY_COMPARTILHAR_ACESSO.md)
- **Automação:** [docs/AUTOMACAO_GUIA_COMPLETO.md](../../docs/AUTOMACAO_GUIA_COMPLETO.md)
- **Prefect Workflow:** [../../docs/PREFECT_GUIA_COMPLETO.md](../../docs/PREFECT_GUIA_COMPLETO.md)

---

**Última atualização:** 2025

