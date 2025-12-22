# 📊 Scripts BigQuery

Scripts para exportar e sincronizar dados pluviométricos e meteorológicos para Google BigQuery.

---

## 📋 Scripts Disponíveis

### **🌧️ Dados Pluviométricos**

#### **Opção 1: NIMBUS → BigQuery (Direto)**

##### `exportar_pluviometricos_nimbus_bigquery.py`
- **Função:** Carga inicial completa de dados pluviométricos do NIMBUS para BigQuery
- **Uso:** Executar uma vez para carregar todos os dados históricos
- **Coluna `dia`:** TIMESTAMP (UTC) no BigQuery, preservando timezone original

##### `sincronizar_pluviometricos_nimbus_bigquery.py`
- **Função:** Sincronização incremental de dados pluviométricos do NIMBUS para BigQuery
- **Uso:** Executar via cron a cada 5 minutos
- **Coluna `dia`:** TIMESTAMP (UTC) no BigQuery, preservando timezone original

#### **Opção 2: Servidor 166 → BigQuery (Com Controle Administrativo)**

##### `exportar_pluviometricos_servidor166_bigquery.py`
- **Função:** Carga inicial completa de dados pluviométricos do servidor 166 para BigQuery
- **Uso:** Executar uma vez para carregar todos os dados históricos
- **Vantagem:** Você tem controle total dos dados (admin do banco)
- **Coluna `dia`:** TIMESTAMP (UTC) no BigQuery, preservando timezone original

##### `sincronizar_pluviometricos_servidor166_bigquery.py`
- **Função:** Sincronização incremental de dados pluviométricos do servidor 166 para BigQuery
- **Uso:** Executar via cron a cada 5 minutos
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

**Todos os scripts usam TIMESTAMP no BigQuery:**

```
Tipo no BigQuery: TIMESTAMP (UTC)
Coluna adicional: dia_original (STRING) - formato exato da NIMBUS
```

**Características:**
- ✅ Coluna `dia`: TIMESTAMP em UTC (padrão BigQuery)
- ✅ Coluna `dia_original`: STRING com formato exato da NIMBUS (`2009-02-16 02:12:20.000 -0300`)
- ✅ Preserva timezone original na coluna `dia_original`
- ✅ Facilita consultas usando TIMESTAMP nativo do BigQuery

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

### **🌧️ Dados Pluviométricos - Sincronização Incremental (Escolha uma opção):**

#### Opção 1: NIMBUS → BigQuery
```bash
# Testar manualmente
python scripts/bigquery/sincronizar_pluviometricos_nimbus_bigquery.py --once

# Configurar cron
cd automacao
./configurar_cron.sh bigquery
```

#### Opção 2: Servidor 166 → BigQuery
```bash
# Testar manualmente
python scripts/bigquery/sincronizar_pluviometricos_servidor166_bigquery.py --once

# Configurar cron
cd automacao
./configurar_cron.sh bigquery_servidor166
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

---

**Última atualização:** 2025

