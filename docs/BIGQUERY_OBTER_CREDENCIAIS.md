# 🔑 Como Obter Credenciais do Google Cloud para BigQuery

Guia passo a passo para criar e baixar as credenciais (Service Account JSON) necessárias para usar o BigQuery.

---

## 🎯 Método Recomendado: Service Account

O método mais seguro e recomendado é criar uma **Service Account** com permissões específicas para BigQuery.

---

## 📋 Passo a Passo Completo

### **Passo 1: Acessar o Console do GCP**

1. Acesse: https://console.cloud.google.com
2. Selecione seu projeto (ou crie um novo se necessário)

---

### **Passo 2: Criar Service Account**

1. No menu lateral, vá em **"IAM & Admin"** → **"Service Accounts"**
   - Ou acesse diretamente: https://console.cloud.google.com/iam-admin/serviceaccounts

2. Clique em **"Create Service Account"** ou **"Criar conta de serviço"**

3. Preencha os dados:
   - **Service account name:** `bigquery-exporter` (ou o nome que preferir)
   - **Service account ID:** Será gerado automaticamente (ex: `bigquery-exporter@projeto.iam.gserviceaccount.com`)
   - **Description:** `Service account para exportar dados para BigQuery`

4. Clique em **"Create and Continue"**

---

### **Passo 3: Conceder Permissões (Roles)**

Agora você precisa conceder as permissões necessárias para o BigQuery:

1. Em **"Grant this service account access to project"**, clique em **"Select a role"**

2. Adicione as seguintes roles (uma por vez):

   **Role 1: BigQuery Data Editor**
   - Procure por: `BigQuery Data Editor`
   - Selecione: **"BigQuery Data Editor"**
   - Clique em **"Add Another Role"**

   **Role 2: BigQuery Job User**
   - Procure por: `BigQuery Job User`
   - Selecione: **"BigQuery Job User"**
   - Clique em **"Add Another Role"**

   **Role 3: BigQuery User** (opcional, mas recomendado)
   - Procure por: `BigQuery User`
   - Selecione: **"BigQuery User"**

3. Clique em **"Continue"**

---

### **Passo 4: Pular Grant Access (Opcional)**

1. Na tela **"Grant users access to this service account"**, você pode pular
2. Clique em **"Done"**

---

### **Passo 5: Criar e Baixar a Chave JSON**

1. Na lista de Service Accounts, clique na conta que você acabou de criar (ex: `bigquery-exporter`)

2. Vá na aba **"Keys"** (Chaves)

3. Clique em **"Add Key"** → **"Create new key"**

4. Selecione o formato: **"JSON"**

5. Clique em **"Create"**

6. O arquivo JSON será baixado automaticamente!

   **⚠️ IMPORTANTE:** 
   - Guarde este arquivo com segurança
   - Não compartilhe publicamente
   - Não faça commit no Git (já está no .gitignore)

---

### **Passo 6: Salvar o Arquivo**

1. Renomeie o arquivo baixado para: `credentials.json`
2. Coloque na pasta `credentials/` na raiz do projeto:

```
projeto/
└── credentials/
    └── credentials.json  ← Arquivo baixado aqui
```

---

## 📁 Estrutura do Arquivo JSON

O arquivo baixado terá este formato:

```json
{
  "type": "service_account",
  "project_id": "seu-projeto-id",
  "private_key_id": "abc123...",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "bigquery-exporter@seu-projeto.iam.gserviceaccount.com",
  "client_id": "123456789...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}
```

**⚠️ NÃO compartilhe este arquivo!** Ele contém credenciais sensíveis.

---

## ✅ Verificação

### **Verificar se o arquivo está correto:**

1. O arquivo deve estar em: `credentials/credentials.json`
2. O arquivo deve ter extensão `.json`
3. O arquivo deve conter `"type": "service_account"`

### **Testar as credenciais:**

Execute o script e veja se conecta:

```bash
python scripts/bigquery/exportar_nimbus_para_bigquery.py
```

Se aparecer:
```
📦 Conectando ao BigQuery...
   🔑 Usando credenciais: C:\...\credentials\credentials.json
✅ Dataset 'pluviometricos' criado/verificado no BigQuery!
```

**✅ Sucesso!** As credenciais estão funcionando!

---

## 🔄 Método Alternativo: Credenciais Padrão do Ambiente

Se você não quiser usar Service Account, pode usar as credenciais padrão do ambiente:

### **Via gcloud CLI:**

1. Instale o Google Cloud SDK: https://cloud.google.com/sdk/docs/install

2. Autentique-se:
```bash
gcloud auth application-default login
```

3. Siga as instruções no navegador para autenticar

4. **Não precisa** configurar `BIGQUERY_CREDENTIALS_PATH` no `.env`

5. O script usará automaticamente as credenciais padrão

**⚠️ Limitação:** Este método usa suas credenciais pessoais, não é ideal para produção.

---

## 🎯 Resumo das Permissões Necessárias

A Service Account precisa ter estas **roles**:

| Role | Descrição | Necessário |
|------|-----------|------------|
| **BigQuery Data Editor** | Permite criar/atualizar tabelas e datasets | ✅ Sim |
| **BigQuery Job User** | Permite executar jobs (queries, loads) | ✅ Sim |
| **BigQuery User** | Permite consultar dados | ⚠️ Opcional |

---

## 🚨 Troubleshooting

### **Erro: "Permission denied"**

**Causa:** Service Account não tem permissões suficientes.

**Solução:**
1. Vá em **IAM & Admin** → **Service Accounts**
2. Clique na sua Service Account
3. Vá em **"Permissions"** ou **"IAM"**
4. Adicione as roles mencionadas acima

---

### **Erro: "Invalid credentials"**

**Causa:** Arquivo JSON corrompido ou caminho incorreto.

**Solução:**
1. Verifique se o arquivo está em `credentials/credentials.json`
2. Verifique se o arquivo não está corrompido
3. Tente baixar novamente a chave JSON

---

### **Erro: "Project not found"**

**Causa:** `project_id` no JSON não corresponde ao projeto atual.

**Solução:**
1. Verifique o `BIGQUERY_PROJECT_ID` no `.env`
2. Certifique-se de que corresponde ao `project_id` no JSON

---

## 📚 Links Úteis

- **Service Accounts:** https://console.cloud.google.com/iam-admin/serviceaccounts
- **IAM Roles:** https://console.cloud.google.com/iam-admin/roles
- **BigQuery Console:** https://console.cloud.google.com/bigquery
- **Google Cloud SDK:** https://cloud.google.com/sdk/docs/install

---

## 🔒 Segurança

### **Boas Práticas:**

✅ **FAÇA:**
- Mantenha o arquivo JSON em local seguro
- Use `.gitignore` para não fazer commit
- Use Service Account com permissões mínimas necessárias
- Rotacione as chaves periodicamente

❌ **NÃO FAÇA:**
- Compartilhar o arquivo JSON publicamente
- Fazer commit do arquivo no Git
- Usar credenciais de administrador
- Deixar o arquivo em locais públicos

---

**Última atualização:** 2025

