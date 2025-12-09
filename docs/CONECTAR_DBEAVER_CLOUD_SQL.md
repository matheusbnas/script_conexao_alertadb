# 🔌 Conectar Cloud SQL no DBeaver

Guia para conectar o Cloud SQL GCP no DBeaver usando as configurações atuais.

---

## 📋 Informações do Cloud SQL

- **Nome da Instância:** `alertadb-cor:us-west1:alertadb-cor`
- **IP Público:** `34.82.95.242`
- **Porta:** `5432` (PostgreSQL padrão)
- **Conectividade IP Público:** Ativado ✅

---

## 🔧 Configuração no DBeaver

### 1. Criar Nova Conexão

1. Abra o DBeaver
2. Clique em **Nova Conexão** (ícone de plug) ou `Ctrl+Shift+N`
3. **IMPORTANTE:** Selecione **PostgreSQL** (não "Google Cloud SQL" ou similar)
4. Clique em **Próximo**

⚠️ **ATENÇÃO:** Use conexão PostgreSQL padrão, não Cloud SQL Proxy!

### 2. Configurações de Conexão

**Aba "Principal":**

```
Host: 34.82.95.242
Porta: 5432
Banco de dados: alertadb_cor
Usuário: postgres
Senha: [sua senha do Cloud SQL]
```

**Aba "SSL":**

```
✅ Usar SSL: Marcar esta opção
Modo SSL: require
```

**Aba "Driver Properties" (opcional):**

Se necessário, adicione:
```
sslmode=require
connectTimeout=10
```

**⚠️ IMPORTANTE - Aba "Cloud SQL" (se existir):**

- **NÃO** marque "Use Cloud SQL Proxy"
- **NÃO** configure credenciais do Google Cloud
- Use conexão direta via IP público

### 3. Testar Conexão

1. Clique em **Testar Conexão**
2. Se pedir para baixar o driver PostgreSQL, clique em **Baixar**
3. Aguarde o teste completar

---

## ⚠️ Importante: Liberar IP no Cloud SQL

Antes de conectar, você precisa liberar o IP público da sua máquina no Cloud SQL:

### Descobrir seu IP Público

```bash
# No PowerShell ou CMD
curl https://api.ipify.org
```

Ou acesse: https://api.ipify.org

### Liberar IP no Console GCP

1. Acesse o [Console GCP](https://console.cloud.google.com/)
2. Vá em **SQL** → **Instâncias**
3. Clique na instância `alertadb-cor`
4. Vá em **Conexões** → **Redes autorizadas**
5. Clique em **Adicionar rede**
6. Cole o IP público da sua máquina
7. Clique em **Salvar**

**Nota:** O IP de saída mostrado (`136.118.184.17`) pode ser diferente do IP público da sua máquina. Use o IP retornado pelo `api.ipify.org`.

---

## 🔍 Verificar Configurações do .env

As configurações no `.env` devem estar assim:

```env
CLOUDSQL_HOST=34.82.95.242
CLOUDSQL_PORT=5432
CLOUDSQL_DATABASE=alertadb_cor
CLOUDSQL_USER=postgres
CLOUDSQL_PASSWORD=sua_senha_aqui
CLOUDSQL_SSLMODE=require
```

---

## 🧪 Testar Conexão via Linha de Comando

Antes de usar no DBeaver, teste via `psql`:

```bash
psql -h 34.82.95.242 -U postgres -d alertadb_cor -c "SELECT 1;"
```

Ou usando a string de conexão completa:

```bash
psql "host=34.82.95.242 port=5432 dbname=alertadb_cor user=postgres password=sua_senha sslmode=require"
```

---

## 📝 Resumo Rápido

**DBeaver:**
- Host: `34.82.95.242`
- Porta: `5432`
- Database: `alertadb_cor`
- User: `postgres`
- Password: `[sua senha]`
- SSL: ✅ Habilitado (require)

**Não esqueça:** Liberar seu IP público no Console GCP antes de conectar!

---

## 🆘 Problemas Comuns

### ❌ Erro: "Unable to obtain credentials to communicate with the Cloud SQL API"

**Causa:** DBeaver está tentando usar Cloud SQL Proxy/API do Google.

**Solução:**
1. ✅ Use conexão **PostgreSQL padrão**, não "Google Cloud SQL"
2. ✅ **NÃO** marque "Use Cloud SQL Proxy" em nenhuma aba
3. ✅ Use conexão direta via IP público (`34.82.95.242`)
4. ✅ Configure apenas Host, Porta, Database, User, Password e SSL

**Se o erro persistir:**
- Feche e reabra o DBeaver
- Crie uma nova conexão do zero
- Certifique-se de selecionar "PostgreSQL" (não "Google Cloud SQL")

---

### Erro: "Connection refused"
- ✅ Verifique se o IP público está liberado no Cloud SQL
- ✅ Verifique se está usando o IP correto (`34.82.95.242`)

### Erro: "SSL required"
- ✅ Marque a opção "Usar SSL" no DBeaver
- ✅ Configure `sslmode=require`

### Erro: "Authentication failed"
- ✅ Verifique usuário e senha
- ✅ Confirme que o usuário `postgres` existe no Cloud SQL

---

## 📚 Referências

- [Guia Rápido Cloud SQL](GUIA_RAPIDO_CLOUD_SQL.md)
- [Integração Cloud SQL](INTEGRACAO_CLOUD_SQL.md)

