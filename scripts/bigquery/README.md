# 📊 Scripts BigQuery

Scripts para exportar e sincronizar dados pluviométricos para Google BigQuery.

---

## 📋 Scripts Disponíveis

### **Opção 1: NIMBUS → BigQuery (Direto)**

#### `exportar_nimbus_para_bigquery.py`
- **Função:** Carga inicial completa do NIMBUS para BigQuery
- **Uso:** Executar uma vez para carregar todos os dados históricos
- **Coluna `dia`:** STRING no formato exato da NIMBUS (`2009-02-16 02:12:20.000 -0300`)

#### `sincronizar_nimbus_para_bigquery.py`
- **Função:** Sincronização incremental do NIMBUS para BigQuery
- **Uso:** Executar via cron a cada 5 minutos
- **Coluna `dia`:** STRING no formato exato da NIMBUS (`2009-02-16 02:12:20.000 -0300`)

---

### **Opção 2: Servidor 166 → BigQuery (Com Controle Administrativo)**

#### `exportar_servidor166_para_bigquery.py`
- **Função:** Carga inicial completa do servidor 166 para BigQuery
- **Uso:** Executar uma vez para carregar todos os dados históricos
- **Vantagem:** Você tem controle total dos dados (admin do banco)
- **Coluna `dia`:** STRING no formato exato da NIMBUS (`2009-02-16 02:12:20.000 -0300`)

#### `sincronizar_servidor166_para_bigquery.py`
- **Função:** Sincronização incremental do servidor 166 para BigQuery
- **Uso:** Executar via cron a cada 5 minutos
- **Vantagem:** Você tem controle total dos dados (admin do banco)
- **Coluna `dia`:** STRING no formato exato da NIMBUS (`2009-02-16 02:12:20.000 -0300`)

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

**Todos os scripts preservam o formato exato da NIMBUS:**

```
Formato: 2009-02-16 02:12:20.000 -0300
Tipo no BigQuery: STRING
```

**Características:**
- ✅ Formato exato como vem da NIMBUS
- ✅ Preserva timezone (`-0300` ou `-0200`)
- ✅ Mostra claramente horário padrão vs horário de verão
- ✅ Formato legível e fácil de consultar

---

## 🚀 Como Usar

### **Carga Inicial (Escolha uma opção):**

#### Opção 1: NIMBUS → BigQuery
```bash
python scripts/bigquery/exportar_nimbus_para_bigquery.py
```

#### Opção 2: Servidor 166 → BigQuery
```bash
python scripts/bigquery/exportar_servidor166_para_bigquery.py
```

### **Sincronização Incremental (Escolha uma opção):**

#### Opção 1: NIMBUS → BigQuery
```bash
# Testar manualmente
python scripts/bigquery/sincronizar_nimbus_para_bigquery.py --once

# Configurar cron
cd automacao
./configurar_cron.sh bigquery
```

#### Opção 2: Servidor 166 → BigQuery
```bash
# Testar manualmente
python scripts/bigquery/sincronizar_servidor166_para_bigquery.py --once

# Configurar cron
cd automacao
./configurar_cron.sh bigquery_servidor166
```

---

## ⚙️ Configuração

### Variáveis Obrigatórias no `.env`

#### Para NIMBUS → BigQuery:
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

#### Para Servidor 166 → BigQuery:
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

