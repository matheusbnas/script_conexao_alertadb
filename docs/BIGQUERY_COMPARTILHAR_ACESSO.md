# 🔐 Compartilhar Acesso ao BigQuery - Guia Completo

Guia para conceder acesso de **leitura (consulta)** no BigQuery para clientes usando Service Accounts.

---

## 🎯 Objetivo

Conceder acesso de **somente leitura** (consulta) no BigQuery para um cliente usando Service Account, sem permitir modificações nos dados.

---

## 📋 Informações da Service Account do Cliente

```
Project ID: rj-cor
Client Email: lncc-cefet@rj-cor.iam.gserviceaccount.com
Client ID: 108254407799378387529
```

**⚠️ IMPORTANTE:** 
- A service account do cliente (`lncc-cefet@rj-cor.iam.gserviceaccount.com`) já existe no projeto `rj-cor` do cliente
- **VOCÊ NÃO PRECISA TER ACESSO** à service account do cliente
- Você só precisa **conceder permissões** no seu projeto `alertadb-cor` para essa service account
- O **CLIENTE** é quem precisa ter acesso à service account dele para obter as credenciais JSON

---

## 🔄 Fluxo de Acesso

```
┌─────────────────────────────────────────────────────────────┐
│ 1. CLIENTE possui Service Account no projeto dele (rj-cor) │
│    Service Account: lncc-cefet@rj-cor.iam.gserviceaccount.com│
│    ⚠️ Você NÃO tem acesso a essa service account           │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. VOCÊ concede acesso no SEU projeto (alertadb-cor)      │
│    Dataset: alertadb_cor_raw                               │
│    Role: BigQuery Data Viewer (somente leitura)           │
│    ✅ Você só precisa saber o EMAIL da service account     │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. CLIENTE obtém credenciais JSON da service account dele  │
│    (CLIENTE faz isso no projeto rj-cor dele)               │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. CLIENTE usa credenciais para consultar dados            │
│    no projeto alertadb-cor                                 │
└─────────────────────────────────────────────────────────────┘
```

**📌 Resumo:**
- **Você:** Concede permissões no projeto `alertadb-cor` usando apenas o EMAIL da service account
- **Cliente:** Obtém credenciais JSON da service account dele e usa para consultar dados

---

## 🔧 Método 1: Via Console GCP (Recomendado)

### Passo 1: Acessar o BigQuery Console

1. Acesse: https://console.cloud.google.com/bigquery
2. **IMPORTANTE:** Selecione o projeto **`alertadb-cor`** (projeto onde estão os dados, NÃO o projeto do cliente)
3. Certifique-se de estar no projeto correto verificando o seletor de projeto no topo da página

### Passo 2: Compartilhar Dataset

1. No painel esquerdo, localize o **dataset** (ex: `alertadb_cor_raw`)
2. Clique com o botão direito no dataset → **"Share dataset"** ou **"Compartilhar conjunto de dados"**
3. Ou clique no dataset e depois em **"SHARING"** → **"Permissions"**

### Passo 3: Adicionar Service Account

1. Clique em **"Add principal"** ou **"Adicionar principal"**
2. No campo **"New principals"**, cole o email da service account:
   ```
   lncc-cefet@rj-cor.iam.gserviceaccount.com
   ```
3. Em **"Select a role"**, escolha uma das opções abaixo:

#### Opção A: BigQuery Data Viewer (Recomendado) ⭐

- **Role:** `BigQuery Data Viewer`
- **Permissões:**
  - ✅ Consultar dados (SELECT)
  - ✅ Visualizar tabelas
  - ✅ Ver schema
  - ❌ Não pode modificar dados
  - ❌ Não pode criar tabelas
  - ❌ Não pode deletar dados

#### Opção B: BigQuery User (Mais Permissivo)

- **Role:** `BigQuery User`
- **Permissões:**
  - ✅ Consultar dados (SELECT)
  - ✅ Criar queries
  - ✅ Criar tabelas temporárias (para queries)
  - ✅ Visualizar tabelas
  - ❌ Não pode modificar dados existentes
  - ❌ Não pode deletar dados

**💡 Recomendação:** Use **BigQuery Data Viewer** para acesso somente leitura.

4. Clique em **"Save"** ou **"Salvar"**

### Passo 4: Verificar Permissões

1. Volte para o dataset
2. Clique em **"SHARING"** → **"Permissions"**
3. Verifique se a service account aparece na lista com a role correta

---

## 👤 Para o Cliente: Como Configurar e Usar

### ⚠️ IMPORTANTE: O Cliente NÃO Precisa Criar Service Account

A service account `lncc-cefet@rj-cor.iam.gserviceaccount.com` **já existe** no projeto `rj-cor` do cliente. O cliente só precisa:

1. **Ter acesso ao projeto `rj-cor`** no GCP Console
2. **Obter as credenciais (JSON)** da service account
3. **Usar essas credenciais** para consultar dados no projeto `alertadb-cor`

### Passo 1: Cliente Obtém Credenciais da Service Account

**⚠️ IMPORTANTE:** O cliente precisa ter acesso ao projeto `rj-cor` dele para fazer isso. Você não precisa fazer nada nesta etapa.

**Como o CLIENTE obtém as credenciais:**
1. Cliente acessa: https://console.cloud.google.com/iam-admin/serviceaccounts?project=rj-cor
2. Cliente localiza a service account: `lncc-cefet`
3. Cliente clica na service account
4. Cliente vai na aba **"KEYS"**
5. Cliente clica em **"ADD KEY"** → **"Create new key"**
6. Cliente escolhe **JSON**
7. Cliente baixa o arquivo (ex: `credentials-rj-cor.json`)

**💡 Você só precisa informar ao cliente:**
- O email da service account: `lncc-cefet@rj-cor.iam.gserviceaccount.com`
- Que ele precisa obter as credenciais JSON dessa service account
- Que ele vai usar essas credenciais para consultar dados no projeto `alertadb-cor`

### Passo 2: Cliente Usa as Credenciais para Consultar Dados

O cliente usa o arquivo JSON baixado para autenticar e consultar dados no projeto `alertadb-cor`:

#### Via Python

```python
from google.cloud import bigquery
from google.oauth2 import service_account

# Caminho para o arquivo JSON da service account do cliente
CREDENTIALS_PATH = 'credentials-rj-cor.json'  # Arquivo baixado pelo cliente

# Carregar credenciais
credentials = service_account.Credentials.from_service_account_file(
    CREDENTIALS_PATH
)

# Criar cliente BigQuery
# IMPORTANTE: project='alertadb-cor' (projeto onde estão os dados)
client = bigquery.Client(
    credentials=credentials,
    project='alertadb-cor'  # Projeto onde VOCÊ compartilhou os dados
)

# Consultar dados
query = """
SELECT 
    dia,
    estacao,
    estacao_id,
    h24
FROM `alertadb-cor.alertadb_cor_raw.pluviometricos`
WHERE dia >= '2009-02-15 22:00:00.000 -0300'
  AND dia <= '2009-02-18 01:00:00.000 -0300'
  AND estacao_id = 14
ORDER BY dia DESC
LIMIT 10
"""

results = client.query(query).result()
for row in results:
    print(f"{row.dia} | {row.estacao} | {row.h24}")
```

#### Via bq CLI

```bash
# 1. Autenticar com service account do cliente
gcloud auth activate-service-account \
  lncc-cefet@rj-cor.iam.gserviceaccount.com \
  --key-file=credentials-rj-cor.json

# 2. Definir projeto (onde estão os dados)
gcloud config set project alertadb-cor

# 3. Consultar dados
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as total FROM \`alertadb-cor.alertadb_cor_raw.pluviometricos\`"
```

### Estrutura do Arquivo JSON de Credenciais

O arquivo JSON que o cliente baixa tem esta estrutura:

```json
{
  "type": "service_account",
  "project_id": "rj-cor",
  "private_key_id": "...",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "lncc-cefet@rj-cor.iam.gserviceaccount.com",
  "client_id": "108254407799378387529",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "..."
}
```

**⚠️ IMPORTANTE:**
- `project_id`: `rj-cor` (projeto do cliente onde a service account foi criada)
- `client_email`: `lncc-cefet@rj-cor.iam.gserviceaccount.com` (service account do cliente)
- Mas o cliente consulta dados no projeto: `alertadb-cor` (projeto onde VOCÊ compartilhou os dados)

---

## 🔧 Método 2: Via Linha de Comando (bq CLI)

### Pré-requisitos

1. Instalar Google Cloud SDK: https://cloud.google.com/sdk/docs/install
2. Autenticar-se:
   ```bash
   gcloud auth login
   ```

### Conceder Acesso ao Dataset

```bash
# Definir variáveis
PROJECT_ID="alertadb-cor"
DATASET_ID="alertadb_cor_raw"  # Ajuste conforme seu dataset
SERVICE_ACCOUNT="lncc-cefet@rj-cor.iam.gserviceaccount.com"
ROLE="roles/bigquery.dataViewer"  # Para somente leitura

# Conceder acesso
bq add-iam-member \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="${ROLE}" \
  "${PROJECT_ID}:${DATASET_ID}"
```

### Conceder Acesso a uma Tabela Específica

Se quiser dar acesso apenas a uma tabela específica:

```bash
# Definir variáveis
PROJECT_ID="alertadb-cor"
DATASET_ID="alertadb_cor_raw"
TABLE_ID="pluviometricos"
SERVICE_ACCOUNT="lncc-cefet@rj-cor.iam.gserviceaccount.com"
ROLE="roles/bigquery.dataViewer"

# Conceder acesso à tabela
bq add-iam-member \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="${ROLE}" \
  "${PROJECT_ID}:${DATASET_ID}.${TABLE_ID}"
```

---

## 🔧 Método 3: Via IAM do Projeto (Acesso Completo ao Projeto)

⚠️ **Não recomendado** para acesso somente leitura, mas pode ser necessário em alguns casos.

### Via Console GCP

1. Acesse: https://console.cloud.google.com/iam-admin/iam
2. Selecione o projeto: `alertadb-cor`
3. Clique em **"Grant Access"** ou **"Conceder acesso"**
4. Cole o email: `lncc-cefet@rj-cor.iam.gserviceaccount.com`
5. Selecione a role: **BigQuery Data Viewer**
6. Clique em **"Save"**

### Via Linha de Comando

```bash
# Conceder acesso ao projeto inteiro
gcloud projects add-iam-policy-binding alertadb-cor \
  --member="serviceAccount:lncc-cefet@rj-cor.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
```

---

## 📊 Roles Disponíveis para BigQuery

| Role | Permissões | Recomendado Para |
|------|------------|-------------------|
| **BigQuery Data Viewer** | ✅ Consultar dados<br>✅ Visualizar tabelas<br>❌ Não pode modificar | ⭐ **Somente leitura** |
| **BigQuery User** | ✅ Consultar dados<br>✅ Criar queries<br>✅ Tabelas temporárias<br>❌ Não pode modificar dados existentes | Consultas avançadas |
| **BigQuery Data Editor** | ✅ Consultar dados<br>✅ Modificar dados<br>✅ Criar tabelas | ⚠️ Muito permissivo |
| **BigQuery Admin** | ✅ Todas as permissões | ⚠️ Administrador |

**💡 Para seu caso:** Use **BigQuery Data Viewer** (somente leitura).

---

## ✅ Verificar Acesso

### Via Console GCP

1. Acesse: https://console.cloud.google.com/bigquery
2. No painel esquerdo, clique no dataset
3. Vá em **"SHARING"** → **"Permissions"**
4. Verifique se `lncc-cefet@rj-cor.iam.gserviceaccount.com` aparece na lista

### Via Linha de Comando

```bash
# Ver permissões do dataset
bq show --format=prettyjson \
  alertadb-cor:alertadb_cor_raw \
  | grep -A 20 "access"

# Ver permissões do projeto
gcloud projects get-iam-policy alertadb-cor \
  --flatten="bindings[].members" \
  --filter="bindings.members:lncc-cefet@rj-cor.iam.gserviceaccount.com"
```

---

## 🧪 Testar Acesso (Como Cliente)

O cliente pode testar o acesso usando a service account dele. Veja a seção **"Para o Cliente: Como Configurar e Usar"** acima para exemplos completos.

---

## 🔒 Segurança

### Boas Práticas

✅ **FAÇA:**
- Use **BigQuery Data Viewer** para acesso somente leitura
- Conceda acesso apenas ao dataset necessário (não ao projeto inteiro)
- Revise permissões periodicamente
- Use Service Accounts ao invés de contas pessoais

❌ **NÃO FAÇA:**
- Não conceda roles administrativas (BigQuery Admin)
- Não conceda acesso de escrita (BigQuery Data Editor) se não necessário
- Não compartilhe credenciais via email ou chat
- Não conceda acesso ao projeto inteiro se só precisa de um dataset

---

## 🚨 Troubleshooting

### Erro: "Access Denied" ou "Permission Denied"

**Causa:** Service account não tem permissões suficientes.

**Solução:**
1. Verifique se a service account foi adicionada corretamente
2. Verifique se a role está correta (BigQuery Data Viewer)
3. Aguarde alguns minutos (pode levar até 5 minutos para propagar)

### Erro: "Dataset not found"

**Causa:** Service account não tem acesso ao dataset.

**Solução:**
1. Verifique se o dataset está compartilhado com a service account
2. Verifique se o nome do dataset está correto
3. Verifique se está usando o projeto correto

### Erro: "Table not found"

**Causa:** Service account não tem acesso à tabela específica.

**Solução:**
1. Conceda acesso ao dataset (não apenas à tabela)
2. Ou conceda acesso específico à tabela usando o Método 2

---

## 📝 Resumo Rápido

### Para VOCÊ Conceder Acesso (Proprietário dos Dados)

**Contexto:**
- Você tem os dados no projeto: `alertadb-cor`
- Dataset: `alertadb_cor_raw`
- Cliente tem service account: `lncc-cefet@rj-cor.iam.gserviceaccount.com`
- **⚠️ Você NÃO precisa ter acesso à service account do cliente**
- **✅ Você só precisa saber o EMAIL da service account**

**Passos:**

```bash
# Via Console GCP (Recomendado)
1. Acesse: https://console.cloud.google.com/bigquery
2. Selecione projeto: alertadb-cor (SEU projeto)
3. Dataset: alertadb_cor_raw → Share dataset
4. Adicionar: lncc-cefet@rj-cor.iam.gserviceaccount.com (apenas o email)
5. Role: BigQuery Data Viewer
6. Salvar

# Via CLI
bq add-iam-member \
  --member="serviceAccount:lncc-cefet@rj-cor.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer" \
  "alertadb-cor:alertadb_cor_raw"
```

**✅ Pronto!** Após isso, o cliente pode usar a service account dele para consultar os dados.

### Para o CLIENTE Usar (Após Você Conceder Acesso)

**Contexto:**
- Cliente tem service account no projeto: `rj-cor` (projeto do cliente)
- Service account: `lncc-cefet@rj-cor.iam.gserviceaccount.com`
- Cliente consulta dados no projeto: `alertadb-cor` (onde você compartilhou)

**Passos:**

1. **Cliente obtém credenciais JSON** da service account dele (no projeto `rj-cor`)
   - Cliente precisa ter acesso ao projeto `rj-cor` dele
   - Cliente baixa o arquivo JSON da service account
2. **Cliente usa credenciais** para consultar dados no projeto `alertadb-cor`

```python
from google.cloud import bigquery
from google.oauth2 import service_account

# Arquivo JSON baixado pelo cliente da service account dele
credentials = service_account.Credentials.from_service_account_file(
    'credentials-rj-cor.json'  # Arquivo do cliente
)

# Criar cliente apontando para projeto onde VOCÊ compartilhou os dados
client = bigquery.Client(
    credentials=credentials,
    project='alertadb-cor'  # Projeto onde estão os dados
)

# Consultar dados
query = """
SELECT dia, estacao, estacao_id, h24
FROM `alertadb-cor.alertadb_cor_raw.pluviometricos`
WHERE estacao_id = 14
ORDER BY dia DESC
LIMIT 10
"""
results = client.query(query).result()
for row in results:
    print(f"{row.dia} | {row.estacao} | {row.h24}")
```

**⚠️ PONTOS IMPORTANTES:**
- Service account do cliente está no projeto `rj-cor` (projeto do cliente)
- Dados estão no projeto `alertadb-cor` (seu projeto)
- **Você NÃO precisa ter acesso à service account do cliente**
- **Você só precisa conceder permissões** no projeto `alertadb-cor` usando o EMAIL da service account
- Cliente usa credenciais do projeto `rj-cor` para acessar dados do projeto `alertadb-cor`
- Isso funciona porque VOCÊ concedeu acesso no projeto `alertadb-cor`

---

## 📚 Links Úteis

- **BigQuery Console:** https://console.cloud.google.com/bigquery
- **IAM Console:** https://console.cloud.google.com/iam-admin/iam
- **Documentação BigQuery IAM:** https://cloud.google.com/bigquery/docs/access-control
- **BigQuery Roles:** https://cloud.google.com/bigquery/docs/access-control#roles

---

## ❓ Perguntas Frequentes

### Eu preciso ter acesso à service account do cliente?

**NÃO.** Você não precisa ter acesso à service account do cliente. Você só precisa:
- Saber o **EMAIL** da service account: `lncc-cefet@rj-cor.iam.gserviceaccount.com`
- Conceder permissões no seu projeto `alertadb-cor` usando esse email

### O cliente precisa criar uma service account no projeto alertadb-cor?

**NÃO.** O cliente usa a service account dele (`lncc-cefet@rj-cor.iam.gserviceaccount.com`) que está no projeto `rj-cor`. Você apenas concede acesso a essa service account para visualizar dados no projeto `alertadb-cor`.

### Por que o email da service account é @rj-cor.iam.gserviceaccount.com mas os dados estão em alertadb-cor?

Porque:
- A **service account** foi criada no projeto `rj-cor` (do cliente)
- Os **dados** estão no projeto `alertadb-cor` (seu projeto)
- Você **compartilha** os dados do projeto `alertadb-cor` com a service account do projeto `rj-cor`

Isso é normal e funciona perfeitamente no GCP.

### O cliente pode modificar os dados?

**NÃO.** Com a role `BigQuery Data Viewer`, o cliente só pode:
- ✅ Consultar dados (SELECT)
- ✅ Visualizar tabelas e schemas
- ❌ Não pode modificar, criar ou deletar dados

### E se o cliente não tiver acesso às credenciais da service account?

O cliente precisa:
1. Ter acesso ao projeto `rj-cor` no GCP Console
2. Ir em IAM → Service Accounts
3. Localizar `lncc-cefet`
4. Criar uma nova chave JSON se necessário

**⚠️ Isso é responsabilidade do cliente, não sua.**

---

**Última atualização:** 2025

