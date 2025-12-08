# ⚙️ Exemplo de Configuração (.env)

Crie um arquivo `.env` na raiz do projeto com o seguinte conteúdo:

```env
# ============================================================================
# CONFIGURAÇÃO DO SISTEMA DE SINCRONIZAÇÃO
# ============================================================================

# ═══════════════════════════════════════════════════════════════════════════
# BANCO DE ORIGEM (alertadb)
# ═══════════════════════════════════════════════════════════════════════════
DB_ORIGEM_HOST=seu_host_origem
DB_ORIGEM_NAME=alertadb
DB_ORIGEM_USER=seu_usuario
DB_ORIGEM_PASSWORD=sua_senha
DB_ORIGEM_PORT=5432
DB_ORIGEM_SSLMODE=disable

# ═══════════════════════════════════════════════════════════════════════════
# BANCO DE DESTINO (alertario_cor)
# ═══════════════════════════════════════════════════════════════════════════
# O banco será criado no servidor 10.50.30.166 (padrão)
DB_DESTINO_HOST=10.50.30.166
DB_DESTINO_NAME=alertario_cor
DB_DESTINO_USER=seu_usuario
DB_DESTINO_PASSWORD=sua_senha
DB_DESTINO_PORT=5432

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO ESPECÍFICA PARA CRIAÇÃO DO BANCO ALERTARIO_COR
# ═══════════════════════════════════════════════════════════════════════════
# ⚠️ IMPORTANTE: O servidor 10.50.30.166 precisa de um usuário DIFERENTE
#    do banco de origem (alertadb). Crie o usuário primeiro usando o script:
#    setup/criar_usuario_postgresql.sql
#
# DB_ALERTARIO_COR_HOST=10.50.30.166  # Padrão, não precisa configurar
DB_ALERTARIO_COR_PORT=5432
DB_ALERTARIO_COR_USER=alertario_cor_user  # ⚠️ Usuário específico para 10.50.30.166
DB_ALERTARIO_COR_PASSWORD=senha_do_usuario_alertario_cor  # ⚠️ Senha do novo usuário
DB_ALERTARIO_COR_SSLMODE=require  # Recomendado: require (tenta SSL primeiro)
DB_ALERTARIO_COR_NAME=alertario_cor

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES DE SINCRONIZAÇÃO
# ═══════════════════════════════════════════════════════════════════════════
INTERVALO_VERIFICACAO=300  # Segundos (padrão: 300 = 5 minutos)

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES PREFECT (opcional)
# ═══════════════════════════════════════════════════════════════════════════
PREFECT_INTERVALO_MINUTOS=5  # Minutos (padrão: 5)

# ═══════════════════════════════════════════════════════════════════════════
# API REST - Dados Pluviométricos (scripts/app.py)
# ═══════════════════════════════════════════════════════════════════════════
# ⚠️ IMPORTANTE: A API agora usa as MESMAS variáveis do banco de destino
#    (DB_DESTINO_*) para manter consistência. As variáveis abaixo são apenas
#    para retrocompatibilidade se você já tinha configurado antes.
#
# A API usa automaticamente:
# - DB_DESTINO_HOST (ou DB_HOST como fallback)
# - DB_DESTINO_PORT (ou DB_PORT como fallback)
# - DB_DESTINO_NAME (ou DB_NAME como fallback) - padrão: alertario_cor
# - DB_DESTINO_USER (ou DB_USER como fallback)
# - DB_DESTINO_PASSWORD (ou DB_PASSWORD como fallback)
#
# ⚠️ Se você já configurou DB_DESTINO_* acima, não precisa configurar estas:
# DB_HOST=10.50.30.166
# DB_PORT=5432
# DB_NAME=alertario_cor
# DB_USER=seu_usuario
# DB_PASSWORD=sua_senha_aqui

# API Key (opcional - se não configurada, a API será acessível sem autenticação)
# Para usar autenticação, configure uma chave secreta e envie no header: X-API-Key
API_KEY=sua_chave_secreta_aqui

# Configurações do servidor Flask (opcional)
# SERVER_HOST=0.0.0.0  # Padrão: 0.0.0.0 (permite acesso de qualquer interface)
#                      # Use 127.0.0.1 para apenas localhost
#                      # Use IP específico (ex: 10.50.30.166) se necessário
# SERVER_PORT=5000     # Padrão: 5000
# DEBUG=False          # Padrão: False (use True apenas em desenvolvimento)
```

---

## 📝 Notas Importantes

1. **Servidor padrão:** O script `criar_banco_alertario_cor.py` **SEMPRE** usa **10.50.30.166** como padrão
2. **Prioridade de configuração:**
   - `DB_ALERTARIO_COR_HOST` (se configurado explicitamente)
   - `10.50.30.166` (padrão - sempre usado se DB_ALERTARIO_COR_HOST não estiver configurado)
   - ⚠️ **IMPORTANTE:** O script ignora `DB_ORIGEM_HOST` para criação do banco
3. **SSL:** O script tenta automaticamente diferentes modos SSL (require, prefer, disable) se necessário
4. **⚠️ CREDENCIAIS OBRIGATÓRIAS:** 
   - O servidor `10.50.30.166` precisa de um **usuário diferente** do banco de origem
   - **NÃO use** `DB_ORIGEM_USER` e `DB_ORIGEM_PASSWORD` para criar o banco
   - **Configure** `DB_ALERTARIO_COR_USER` e `DB_ALERTARIO_COR_PASSWORD` com credenciais válidas para `10.50.30.166`
   - Crie o usuário primeiro usando o script `setup/criar_usuario_postgresql.sql`

---

## 🚀 Uso Rápido

```bash
# 1. Criar arquivo .env (copie o exemplo acima e ajuste)

# 2. Criar usuário no servidor 10.50.30.166 (execute como postgres)
#    - Conecte-se ao servidor 10.50.30.166
#    - Execute: psql -U postgres -f setup/criar_usuario_postgresql.sql
#    - Ajuste o nome do usuário e senha no script SQL antes de executar

# 3. Configurar credenciais no .env:
#    DB_ALERTARIO_COR_USER=seu_novo_usuario
#    DB_ALERTARIO_COR_PASSWORD=senha_do_novo_usuario

# 4. Testar conexão
python setup/testar_conexao.py

# 5. Criar banco de dados no servidor 10.50.30.166 (via SSH)
ssh servicedesk@10.50.30.166
psql -U postgres -f setup/criar_banco_servidor.sql

# 6. Carregar dados históricos
python scripts/carregar_pluviometricos_historicos.py

# 7. Sincronizar novos dados
python scripts/sincronizar_pluviometricos_novos.py
```

