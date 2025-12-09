# 🔧 Como Configurar Variáveis do BigQuery no .env

Guia passo a passo para encontrar todas as configurações necessárias no GCP/BigQuery.

---

## 📋 Variáveis Necessárias

```env
# BigQuery
BIGQUERY_PROJECT_ID=seu-projeto-id
BIGQUERY_DATASET_ID=nome-do-dataset
BIGQUERY_TABLE_ID=nome-da-tabela
BIGQUERY_CREDENTIALS_PATH=/caminho/credentials.json  # Opcional
BIGQUERY_CONNECTION_ID=projects/.../connections/...  # Opcional
```

---

## 1️⃣ BIGQUERY_PROJECT_ID

### Onde encontrar:

#### **Opção A: Via Console GCP (Mais Fácil)**

1. Acesse: https://console.cloud.google.com
2. No topo da página, você verá o **ID do projeto** ao lado do nome do projeto
3. Exemplo: Se o nome é "Meu Projeto", o ID pode ser `1029418267270`

**Visual:**
```
[Menu ☰]  Meu Projeto (1029418267270)  [Seletor de Projeto ▼]
```

#### **Opção B: Via BigQuery Console**

1. Acesse: https://console.cloud.google.com/bigquery
2. No painel esquerdo, você verá o projeto listado
3. Clique no projeto → Veja o ID na URL ou nas propriedades

#### **Opção C: Via Linha de Comando**

```bash
# Listar projetos
gcloud projects list

# Ver projeto atual
gcloud config get-value project
```

### Configurar no .env:

```env
BIGQUERY_PROJECT_ID=1029418267270
```

**💡 Dica:** Use o **ID numérico**, não o nome do projeto!

---

## 2️⃣ BIGQUERY_DATASET_ID

### Onde encontrar:

#### **Via BigQuery Console:**

1. Acesse: https://console.cloud.google.com/bigquery
2. No painel esquerdo, você verá os **datasets** do projeto
3. Se não existir, você precisa criar um:

**Criar Dataset:**

1. No BigQuery Console, clique em **"Create Dataset"** ou **"Criar conjunto de dados"**
2. Configure:
   - **Dataset ID:** `pluviometricos` (ou o nome que preferir)
   - **Location type:** `Multi-region` ou `Region` (ex: `us-west1`)
   - **Default table expiration:** Deixe em branco ou configure
3. Clique em **"Create dataset"**

**Visual:**
```
BigQuery Console
├── 1029418267270 (projeto)
    ├── pluviometricos (dataset) ← Este é o DATASET_ID
        └── pluviometricos (tabela)
```

### Configurar no .env:

```env
BIGQUERY_DATASET_ID=pluviometricos
```

**💡 Dica:** O dataset será criado automaticamente pelo script se não existir!

---

## 3️⃣ BIGQUERY_TABLE_ID

### Onde encontrar:

#### **Via BigQuery Console:**

1. Acesse: https://console.cloud.google.com/bigquery
2. Expanda o dataset no painel esquerdo
3. Você verá as tabelas dentro do dataset
4. Se não existir, o script criará automaticamente!

**Visual:**
```
BigQuery Console
├── 1029418267270
    ├── pluviometricos (dataset)
        ├── pluviometricos (tabela) ← Este é o TABLE_ID
        └── outras_tabelas...
```

### Configurar no .env:

```env
BIGQUERY_TABLE_ID=pluviometricos
```

**💡 Dica:** O script criará a tabela automaticamente na primeira execução!

---

## 4️⃣ BIGQUERY_CREDENTIALS_PATH (Opcional)

### Quando usar:

- ✅ Se você está rodando o script **localmente** (não no GCP)
- ✅ Se precisa de autenticação específica
- ✅ Se não quer usar `gcloud auth application-default login`

### Como obter:

**📚 GUIA COMPLETO:** Veja `docs/BIGQUERY_OBTER_CREDENCIAIS.md` para instruções detalhadas passo a passo.

#### **Resumo Rápido:**

1. Acesse: https://console.cloud.google.com/iam-admin/serviceaccounts
2. Crie uma Service Account
3. Adicione as roles: `BigQuery Data Editor`, `BigQuery Job User`, `BigQuery User`
4. Crie uma chave JSON
5. Baixe o arquivo e coloque em `credentials/credentials.json`

**💡 Dica:** O script detecta automaticamente `credentials/credentials.json` se não configurar no `.env`

### Configurar no .env (Opcional):

```env
# Se quiser especificar um caminho diferente:
BIGQUERY_CREDENTIALS_PATH=/caminho/completo/credentials.json

# Ou deixe vazio para usar o padrão: credentials/credentials.json
```

**💡 Dica:** Se não configurar, o script usará automaticamente `credentials/credentials.json` ou as credenciais padrão do ambiente (`gcloud auth application-default login`)

---

## 5️⃣ BIGQUERY_CONNECTION_ID (Opcional)

### Quando usar:

- ✅ Se você já tem uma conexão BigQuery configurada
- ✅ Para referência futura (não é usado no script atual)
- ⚠️ **Nota:** Esta variável é opcional e não é usada pelo script de exportação atual

### Onde encontrar:

#### **Via BigQuery Console:**

1. Acesse: https://console.cloud.google.com/bigquery
2. No painel esquerdo, vá em **"External data sources"** ou **"Fontes de dados externas"**
3. Você verá suas conexões listadas
4. Clique na conexão desejada (ex: `alertadb_cor_raw`)
5. Veja o **Connection ID** nas propriedades

**Visual:**
```
BigQuery Console
├── External data sources
    └── alertadb_cor_raw
        └── Connection ID: projects/1029418267270/locations/us/connections/conexao_alerta_db
```

#### **Via GCP Console:**

1. Acesse: https://console.cloud.google.com/bigquery/connections
2. Você verá todas as conexões
3. Clique na conexão desejada
4. Veja o **Connection ID** completo

**Exemplo de Connection ID:**
```
projects/1029418267270/locations/us/connections/conexao_alerta_db
```

### Configurar no .env (Opcional):

```env
BIGQUERY_CONNECTION_ID=projects/1029418267270/locations/us/connections/conexao_alerta_db
```

**💡 Dica:** Esta variável é apenas para referência. O script atual não a utiliza, mas pode ser útil para scripts futuros que usem Federated Queries.

---

## 📝 Exemplo Completo de .env

```env
# ============================================================================
# BIGQUERY - Configurações
# ============================================================================

# ID do Projeto GCP (obrigatório)
# Encontre em: https://console.cloud.google.com (topo da página)
BIGQUERY_PROJECT_ID=1029418267270

# Dataset ID (opcional, padrão: pluviometricos)
# Encontre em: https://console.cloud.google.com/bigquery (painel esquerdo)
# Ou crie em: BigQuery → Create Dataset
BIGQUERY_DATASET_ID=pluviometricos

# Table ID (opcional, padrão: pluviometricos)
# Encontre em: https://console.cloud.google.com/bigquery (dentro do dataset)
# Ou será criado automaticamente pelo script
BIGQUERY_TABLE_ID=pluviometricos

# Caminho para credentials.json (opcional)
# Crie em: https://console.cloud.google.com/iam-admin/serviceaccounts
# Ou use: gcloud auth application-default login
BIGQUERY_CREDENTIALS_PATH=/caminho/completo/credentials.json

# Connection ID (opcional, não usado atualmente)
# Encontre em: https://console.cloud.google.com/bigquery/connections
BIGQUERY_CONNECTION_ID=projects/1029418267270/locations/us/connections/conexao_alerta_db
```

---

## ✅ Verificação Rápida

### 1. Verificar Projeto:

```bash
# Via CLI
gcloud config get-value project

# Ou veja no console
https://console.cloud.google.com
```

### 2. Verificar/Criar Dataset:

```bash
# Listar datasets
bq ls

# Criar dataset (se não existir)
bq mk --dataset --location=us pluviometricos
```

### 3. Verificar Credenciais:

```bash
# Verificar se está autenticado
gcloud auth application-default print-access-token

# Se não estiver, autenticar
gcloud auth application-default login
```

---

## 🚀 Próximos Passos

1. ✅ Configure `BIGQUERY_PROJECT_ID` (obrigatório)
2. ✅ Configure `BIGQUERY_DATASET_ID` (opcional, padrão: pluviometricos)
3. ✅ Configure `BIGQUERY_TABLE_ID` (opcional, padrão: pluviometricos)
4. ⚠️ Configure `BIGQUERY_CREDENTIALS_PATH` (opcional, se não usar gcloud auth)
5. ✅ Execute o script: `python scripts/bigquery/exportar_nimbus_para_bigquery.py`

---

## 📚 Links Úteis

- **BigQuery Console:** https://console.cloud.google.com/bigquery
- **Service Accounts:** https://console.cloud.google.com/iam-admin/serviceaccounts
- **BigQuery Connections:** https://console.cloud.google.com/bigquery/connections
- **GCP Projects:** https://console.cloud.google.com/cloud-resource-manager

---

**Última atualização:** 2025

