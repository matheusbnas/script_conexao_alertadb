# 🌩️ Cloud SQL - Guia Completo

Guia completo para integração com Google Cloud SQL, incluindo configuração, conexão, ajustes e troubleshooting.

---

## 📋 Índice

1. [Visão Geral](#visão-geral)
2. [Arquitetura](#arquitetura)
3. [Configuração Inicial](#configuração-inicial)
4. [Guia Rápido](#guia-rápido)
5. [Integração](#integração)
6. [Conectar DBeaver](#conectar-dbeaver)
7. [Ajustes Recomendados](#ajustes-recomendados)
8. [Troubleshooting](#troubleshooting)

---

## 🎯 Visão Geral

### O Que Foi Adicionado

Esta é uma **extensão** do projeto existente que adiciona sincronização do Servidor 166 para o Cloud SQL GCP.

### Arquitetura Completa

**ANTES (2 camadas):**
```
NIMBUS (10.2.223.114)
    ↓ [carregar_pluviometricos_historicos.py]
    ↓ [sincronizar_pluviometricos_novos.py]
    ↓ [cron.sh normal - a cada 5 min]
    ↓
Servidor 166 (alertadb_cor)
    └─ API REST (app.py)
```

**DEPOIS (3 camadas):**
```
NIMBUS (10.2.223.114)
    ↓ [carregar_pluviometricos_historicos.py]
    ↓ [sincronizar_pluviometricos_novos.py]
    ↓ [cron.sh normal - a cada 5 min]
    ↓
Servidor 166 (alertadb_cor)
  ├─ API REST (app.py)
  └─ [carregar_para_cloudsql_inicial.py]     🆕 NOVO
     [sincronizar_para_cloudsql.py]          🆕 NOVO
     [cron.sh cloudsql - a cada 5 min]       🆕 NOVO
        ↓
    Cloud SQL GCP (34.82.95.242)
```

---

## 🏗️ Arquitetura

### Arquivos Criados

#### Scripts (`scripts/cloudsql/`)

| Arquivo | Baseado Em | Função |
|---------|-----------|--------|
| `carregar_para_cloudsql_inicial.py` | `carregar_pluviometricos_historicos.py` | Carga inicial completa |
| `sincronizar_para_cloudsql.py` | `sincronizar_pluviometricos_novos.py` | Sync incremental (5 min) |

#### Automação (`automacao/`)

| Arquivo | Baseado Em | Função |
|---------|-----------|--------|
| `cron.sh cloudsql` | `cron.sh normal` | Script cron |
| `configurar_cron.sh cloudsql` | `configurar_cron.sh normal` | Instalador automático |

### Compatibilidade

#### Scripts Existentes ✅
- ✅ `carregar_pluviometricos_historicos.py` - Continua funcionando
- ✅ `sincronizar_pluviometricos_novos.py` - Continua funcionando
- ✅ `app.py` - Continua funcionando
- ✅ `cron.sh normal` - Continua funcionando

#### Sem Conflitos
- ✅ Usa variáveis diferentes no .env (`CLOUDSQL_*`)
- ✅ Logs separados (`cloudsql_*.log` vs `sincronizacao_*.log`)
- ✅ Cron independente
- ✅ Não afeta sincronização NIMBUS→166

---

## ⚙️ Configuração Inicial

### Passo 1: Descobrir IP do Servidor 166

```bash
# No servidor 166
curl https://api.ipify.org

# Anotar resultado (ex: 200.123.45.67)
```

### Passo 2: Liberar IP no Cloud SQL GCP

**Via Console GCP:**

1. Acesse: https://console.cloud.google.com/sql/instances
2. Clique em: `alertadb-cor`
3. Menu: `Connections` → `Networking`
4. `Authorized networks` → `+ ADD NETWORK`
5. Configurar:
   ```
   Name: Servidor 166 COR
   Network: [IP_DO_PASSO_1]/32
   ```
6. `DONE` → `SAVE`
7. Aguardar 1 minuto

### Passo 3: Configurar .env

Adicione estas novas variáveis ao arquivo `.env` **existente**:

```env
# ═══════════════════════════════════════════════════════════════════════════
# 🌩️ CLOUD SQL GCP (Nova Camada de Sincronização)
# ═══════════════════════════════════════════════════════════════════════════

CLOUDSQL_HOST=34.82.95.242
CLOUDSQL_PORT=5432
CLOUDSQL_DATABASE=alertadb_cor
CLOUDSQL_USER=postgres
CLOUDSQL_PASSWORD=SENHA_CLOUD_SQL_AQUI
CLOUDSQL_SSLMODE=require
```

**IMPORTANTE:** Mantenha todas as variáveis existentes (`DB_ORIGEM_*`, `DB_DESTINO_*`)!

### Passo 4: Testar Conexões

```bash
# Testar servidor 166 → Cloud SQL
psql -h 34.82.95.242 -U postgres -d alertadb_cor -c "SELECT 1;"

# Se funcionar: ✅ Pronto para carga inicial!
```

### Passo 5: Executar Carga Inicial

```bash
cd /opt/sync-nimbus
python3 scripts/cloudsql/carregar_para_cloudsql_inicial.py
```

**Saída esperada:**
```
🌧️ CARGA INICIAL COMPLETA - Servidor 166 → Cloud SQL GCP
✅ Conectado ao ORIGEM: alertadb_cor@localhost
✅ Conectado ao DESTINO: alertadb_cor@34.82.95.242

📦 Lote 1: 10,000 registros processados (Total: 10,000)
📦 Lote 2: 10,000 registros processados (Total: 20,000)
...

✅ CARGA INICIAL COMPLETA FINALIZADA!
📊 Total inserido: 150,000 registros
```

### Passo 6: Configurar Automação

```bash
cd automacao
./configurar_cron.sh cloudsql
```

OU manualmente:

```bash
chmod +x automacao/cron.sh

crontab -e
# Adicionar:
*/5 * * * * /opt/sync-nimbus/automacao/cron.sh cloudsql
```

---

## ⚡ Guia Rápido

### Instalação Rápida (15 min)

```bash
# 1. Descobrir IP
curl https://api.ipify.org

# 2. Liberar IP no Cloud SQL (console GCP)

# 3. Configurar .env
nano .env
# Adicionar variáveis CLOUDSQL_*

# 4. Testar conexão
psql -h 34.82.95.242 -U postgres -d alertadb_cor -c "SELECT 1;"

# 5. Carga inicial
python3 scripts/cloudsql/carregar_para_cloudsql_inicial.py

# 6. Configurar cron
./automacao/configurar_cron.sh cloudsql
```

### Comandos Essenciais

#### Sincronização

```bash
# Carga inicial (uma vez)
python3 scripts/cloudsql/carregar_para_cloudsql_inicial.py

# Sync incremental manual
python3 scripts/cloudsql/sincronizar_para_cloudsql.py --once

# Sync contínuo
python3 scripts/cloudsql/sincronizar_para_cloudsql.py
```

#### Automação

```bash
# Configurar cron
./automacao/configurar_cron.sh cloudsql

# Verificar cron
crontab -l | grep cloudsql

# Remover cron
crontab -e
# Remover linha correspondente

# Testar script cron
./automacao/cron.sh cloudsql
```

#### Logs

```bash
# Ver últimos logs
tail -20 logs/cloudsql_*.log

# Monitorar em tempo real
tail -f logs/cloudsql_*.log

# Buscar erros
grep -i erro logs/cloudsql_*.log
grep -i error logs/cloudsql_*.log

# Contar sincronizações hoje
grep "$(date +%Y-%m-%d)" logs/cloudsql_*.log | grep "sincronizado" | wc -l
```

#### Validação

```bash
# Contar registros
psql -h 34.82.95.242 -U postgres -d alertadb_cor \
  -c "SELECT COUNT(*) FROM pluviometricos;"

# Ver último registro
psql -h 34.82.95.242 -U postgres -d alertadb_cor \
  -c "SELECT MAX(dia) FROM pluviometricos;"

# Últimos 5 registros
psql -h 34.82.95.242 -U postgres -d alertadb_cor \
  -c "SELECT * FROM pluviometricos ORDER BY dia DESC LIMIT 5;"

# Comparar servidor 166 vs Cloud SQL
diff \
  <(psql -h localhost -U postgres -d alertadb_cor -t -c "SELECT COUNT(*) FROM pluviometricos;") \
  <(psql -h 34.82.95.242 -U postgres -d alertadb_cor -t -c "SELECT COUNT(*) FROM pluviometricos;")
```

---

## 🔄 Integração

### Fluxo Completo de Sincronização

#### NIMBUS → Servidor 166 (Existente)

```bash
# Carga inicial (já executado)
python3 scripts/servidor166/carregar_pluviometricos_historicos.py

# Sync contínuo (cron ativo)
*/5 * * * * /opt/sync-nimbus/automacao/cron.sh normal
```

#### Servidor 166 → Cloud SQL (Novo)

```bash
# Carga inicial (executar uma vez)
python3 scripts/cloudsql/carregar_para_cloudsql_inicial.py

# Sync contínuo (novo cron)
*/5 * * * * /opt/sync-nimbus/automacao/cron.sh cloudsql
```

### Características

#### Mesma Lógica dos Scripts Existentes
- ✅ DISTINCT ON para evitar duplicatas
- ✅ ON CONFLICT DO UPDATE para atualizar dados
- ✅ Timezone preservado (-02:00 / -03:00)
- ✅ Processamento em lotes (10.000 registros)
- ✅ Modo --once para cron
- ✅ Logs detalhados

#### Vantagens
- ✅ Não mexe na NIMBUS (zero risco)
- ✅ Aproveita dados já sincronizados no 166
- ✅ Latência zero (localhost → internet)
- ✅ Mesma estrutura do projeto existente
- ✅ Fácil integração e manutenção

---

## 🔌 Conectar DBeaver

### Informações do Cloud SQL

- **Nome da Instância:** `alertadb-cor:us-west1:alertadb-cor`
- **IP Público:** `34.82.95.242`
- **Porta:** `5432` (PostgreSQL padrão)
- **Conectividade IP Público:** Ativado ✅

### Configuração no DBeaver

#### 1. Criar Nova Conexão

1. Abra o DBeaver
2. Clique em **Nova Conexão** (ícone de plug) ou `Ctrl+Shift+N`
3. **IMPORTANTE:** Selecione **PostgreSQL** (não "Google Cloud SQL" ou similar)
4. Clique em **Próximo**

⚠️ **ATENÇÃO:** Use conexão PostgreSQL padrão, não Cloud SQL Proxy!

#### 2. Configurações de Conexão

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

#### 3. Testar Conexão

1. Clique em **Testar Conexão**
2. Se pedir para baixar o driver PostgreSQL, clique em **Baixar**
3. Aguarde o teste completar

### Liberar IP no Cloud SQL

Antes de conectar, você precisa liberar o IP público da sua máquina no Cloud SQL:

#### Descobrir seu IP Público

```bash
# No PowerShell ou CMD
curl https://api.ipify.org
```

Ou acesse: https://api.ipify.org

#### Liberar IP no Console GCP

1. Acesse o [Console GCP](https://console.cloud.google.com/)
2. Vá em **SQL** → **Instâncias**
3. Clique na instância `alertadb-cor`
4. Vá em **Conexões** → **Redes autorizadas**
5. Clique em **Adicionar rede**
6. Cole o IP público da sua máquina
7. Clique em **Salvar**

### Testar Conexão via Linha de Comando

Antes de usar no DBeaver, teste via `psql`:

```bash
psql -h 34.82.95.242 -U postgres -d alertadb_cor -c "SELECT 1;"
```

---

## ⚙️ Ajustes Recomendados

### Configurações Atuais (Boa)

- **PostgreSQL 17.7** - Versão recente e compatível ✅
- **8 vCPU, 64 GB RAM** - Excelente capacidade ✅
- **Cache de dados: 375 GB** - Ótimo para performance ✅
- **Capacidade de rede: 2.000 MB/s** - Excelente ✅
- **IOPS: 9.000/15.000** - Boa capacidade ✅

### Ajustes Recomendados

#### 1. Armazenamento: 100 GB SSD

**Status:** ⚠️ Pode ser insuficiente dependendo do volume de dados

**Recomendação:**
- Verifique o tamanho atual dos dados no servidor 166:
  ```sql
  SELECT pg_size_pretty(pg_total_relation_size('pluviometricos'));
  ```
- Se os dados forem > 50 GB, considere aumentar para 200-500 GB
- Cloud SQL permite aumentar storage facilmente (sem downtime)

**Estimativa de espaço:**
- ~100 bytes por registro
- 1 milhão de registros ≈ 100 MB
- 10 milhões de registros ≈ 1 GB
- 100 milhões de registros ≈ 10 GB

#### 2. Backup: Manual

**Status:** ⚠️ Recomendado mudar para automático

**Recomendação:**
- Ative **Backup Automático** no Console GCP
- Configure backup diário (recomendado: 2:00 AM)
- Retenção: 7 dias (padrão) ou mais conforme necessidade

**Como ativar:**
1. Console GCP → SQL → Instâncias → `alertadb-cor`
2. Aba **Backups**
3. Marcar **Enable automated backups**
4. Configurar horário e retenção

#### 3. Recuperação Pontual: Desativada

**Status:** ⚠️ Recomendado ativar para produção

**Recomendação:**
- Ative **Point-in-time Recovery (PITR)**
- Permite restaurar para qualquer ponto no tempo
- Essencial para ambientes de produção

**Como ativar:**
1. Console GCP → SQL → Instâncias → `alertadb-cor`
2. Aba **Backups**
3. Marcar **Enable point-in-time recovery**
4. Requer backup automático ativado

#### 4. Disponibilidade: Única Zona

**Status:** ⚠️ OK para desenvolvimento/teste, não recomendado para produção

**Recomendação:**
- Para produção, considere **Alta Disponibilidade (HA)**
- HA oferece redundância entre zonas
- 99.95% de SLA vs 99.5% (zona única)
- Custo adicional: ~2x

### Otimizações Já Implementadas nos Scripts

#### Durante Carga Inicial:
- ✅ `synchronous_commit = off` - Melhora performance (desabilitado após carga)
- ✅ `work_mem = 256MB` - Melhora ordenações/agregações
- ✅ `maintenance_work_mem = 1GB` - Melhora operações de manutenção
- ✅ `autovacuum_enabled = false` - Desabilitado durante carga (reabilitado após)

#### Após Carga:
- ✅ Todas as configurações são restauradas para valores padrão
- ✅ Autovacuum reabilitado automaticamente

---

## 🐛 Troubleshooting

### Erro: "Could not connect to DESTINO (Cloud SQL)"

**Verificar:**
```bash
# IP está autorizado?
curl https://api.ipify.org

# Porta aberta?
telnet 34.82.95.242 5432

# Senha correta?
grep CLOUDSQL_PASSWORD .env
```

### Erro: "Tabela está VAZIA"

**Solução:**
```bash
# Executar carga inicial primeiro
python3 scripts/cloudsql/carregar_para_cloudsql_inicial.py
```

### Erro: "Unable to obtain credentials to communicate with the Cloud SQL API" (DBeaver)

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

### Script não executa no cron

**Verificar:**
```bash
# Caminho do Python
which python3

# Permissões
ls -la automacao/cron.sh

# Testar manualmente
cd /opt/sync-nimbus
automacao/cron.sh cloudsql
```

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

## ✅ Checklist de Integração

- [ ] IP servidor 166 descoberto
- [ ] IP liberado no Cloud SQL GCP
- [ ] Arquivos copiados para diretórios corretos
- [ ] .env atualizado com variáveis Cloud SQL
- [ ] Conexão Cloud SQL testada
- [ ] Carga inicial executada com sucesso
- [ ] Cron Cloud SQL configurado
- [ ] Logs sendo gerados corretamente
- [ ] Validado dados no Cloud SQL
- [ ] Sistema monitorado por 24h

---

## 📚 Documentação Relacionada

- [README.md](../README.md) - Documentação principal
- [scripts/README.md](../scripts/README.md) - Documentação dos scripts
- [automacao/README.md](../automacao/README.md) - Documentação da automação

---

**Sistema completo de 3 camadas:**
```
NIMBUS → Servidor 166 → Cloud SQL GCP
   ↓         ↓             ↓
 alertadb  alertadb_cor  alertadb_cor
```

**Ambas sincronizações rodando a cada 5 minutos automaticamente!** 🎉

---

**Última atualização:** 2025

