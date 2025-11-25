# 🔐 Guia: Criar Usuário para o Servidor 10.50.30.166

## 📋 Resumo

O servidor `10.50.30.166` precisa de um **usuário diferente** do banco de origem (`alertadb`). 
O usuário `planejamento_cor` é apenas para o banco original e não funciona no servidor de destino.

## 🎯 Passo a Passo

### 1. Criar o Usuário no Servidor PostgreSQL

Você precisa ter acesso ao servidor `10.50.30.166` como superusuário (`postgres`).

#### Opção A: Via psql (linha de comando)

```bash
# Conectar ao servidor como postgres
psql -h 10.50.30.166 -U postgres -d postgres

# Executar os comandos SQL:
CREATE USER alertario_cor_user WITH PASSWORD 'sua_senha_segura_aqui';
ALTER USER alertario_cor_user CREATEDB;
GRANT CONNECT ON DATABASE postgres TO alertario_cor_user;
```

#### Opção B: Via arquivo SQL

1. Edite o arquivo `setup/criar_usuario_postgresql.sql`
2. Ajuste o nome do usuário e senha conforme necessário
3. Execute:

```bash
psql -h 10.50.30.166 -U postgres -f setup/criar_usuario_postgresql.sql
```

#### Opção C: Via pgAdmin ou outra ferramenta gráfica

1. Conecte-se ao servidor `10.50.30.166` como `postgres`
2. Execute os comandos SQL do arquivo `setup/criar_usuario_postgresql.sql`
3. Ajuste o nome do usuário e senha antes de executar

### 2. Configurar Credenciais no Arquivo .env

Adicione as seguintes linhas no arquivo `.env` na raiz do projeto:

```env
# Credenciais para criar o banco no servidor 10.50.30.166
DB_ALERTARIO_COR_USER=alertario_cor_user
DB_ALERTARIO_COR_PASSWORD=sua_senha_segura_aqui
```

**⚠️ IMPORTANTE:** 
- Use o **mesmo nome de usuário e senha** que você criou no passo 1
- **NÃO** use `DB_ORIGEM_USER` e `DB_ORIGEM_PASSWORD` para isso

### 3. Testar a Conexão

Execute o script de teste para verificar se as credenciais estão corretas:

```bash
python setup/testar_conexao.py
```

Se tudo estiver correto, você verá:
```
✅ CONEXÃO ESTABELECIDA COM SUCESSO!
```

### 4. Criar o Banco de Dados no Servidor

Conecte-se ao servidor via SSH e execute:

```bash
# Conectar ao servidor
ssh servicedesk@10.50.30.166

# No servidor, execute o script SQL:
psql -U postgres -f setup/criar_banco_servidor.sql

# Ou use o script shell:
bash setup/criar_banco_servidor.sh
```

## 🔍 Verificação

Para verificar se o usuário foi criado corretamente, execute no servidor:

```sql
SELECT usename, usecreatedb, usesuper 
FROM pg_user 
WHERE usename = 'alertario_cor_user';
```

O resultado deve mostrar:
- `usename`: `alertario_cor_user`
- `usecreatedb`: `t` (true - pode criar bancos)
- `usesuper`: `f` (false - não é superusuário, recomendado)

## ❓ Problemas Comuns

### Erro: "password authentication failed"

- Verifique se a senha no `.env` está correta
- Verifique se o usuário existe no servidor `10.50.30.166`
- Execute o teste: `python setup/testar_conexao.py`

### Erro: "permission denied to create database"

- O usuário precisa ter a permissão `CREATEDB`
- Execute: `ALTER USER alertario_cor_user CREATEDB;`

### Erro: "no pg_hba.conf entry"

- O servidor não permite conexões do seu IP
- Contate o administrador do servidor para adicionar seu IP ao `pg_hba.conf`

## 📝 Notas de Segurança

1. **Use senhas fortes** para o usuário do banco de dados
2. **Não compartilhe** o arquivo `.env` (ele está no `.gitignore`)
3. **Não use** o usuário `postgres` para operações normais
4. **Dê apenas as permissões necessárias** ao usuário (CREATEDB, não SUPERUSER)

