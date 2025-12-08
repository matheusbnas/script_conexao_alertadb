# 🔄 INTEGRAÇÃO DA NOVA CAMADA - Cloud SQL GCP

## 🎯 Visão Geral

Este guia explica como integrar a **nova camada de sincronização** ao projeto existente.

```
ANTES:
NIMBUS (10.2.223.114) → Servidor 166 (alertadb_cor)

DEPOIS:
NIMBUS (10.2.223.114) → Servidor 166 (alertadb_cor) → Cloud SQL GCP (34.82.95.242)
                    ↑ [existente]                 ↑ [NOVO]
```

---

## 📦 Novos Arquivos Adicionados

### **Scripts Principais** (`scripts/`)
```
scripts/
├── carregar_para_cloudsql_inicial.py  # Carga inicial Cloud SQL
├── sincronizar_para_cloudsql.py       # Sync incremental Cloud SQL
```

### **Automação** (`automacao/`)
```
automacao/
├── cron_cloudsql.sh                   # Script cron Cloud SQL
├── configurar_cron_cloudsql.sh        # Instalador automático
```

---

## ⚙️ Configuração do .env

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

---

## 🚀 Instalação - Passo a Passo

### **Passo 1: Descobrir IP do Servidor 166**

```bash
# No servidor 166
curl https://api.ipify.org

# Anotar resultado (ex: 200.123.45.67)
```

---

### **Passo 2: Liberar IP no Cloud SQL GCP**

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

---

### **Passo 3: Copiar Novos Arquivos**

```bash
# No servidor 166
cd /opt/sync-nimbus  # ou diretório do projeto

# Copiar scripts principais
cp /caminho/carregar_para_cloudsql_inicial.py scripts/
cp /caminho/sincronizar_para_cloudsql.py scripts/

# Copiar automação
cp /caminho/cron_cloudsql.sh automacao/
cp /caminho/configurar_cron_cloudsql.sh automacao/

# Tornar executáveis
chmod +x scripts/*.py
chmod +x automacao/cron_cloudsql.sh
chmod +x automacao/configurar_cron_cloudsql.sh
```

---

### **Passo 4: Configurar .env**

```bash
# Editar .env
nano .env

# Adicionar variáveis Cloud SQL (veja seção acima)
# SALVAR: Ctrl+O, Enter, Ctrl+X
```

---

### **Passo 5: Testar Conexões**

```bash
# Testar servidor 166 → Cloud SQL
psql -h 34.82.95.242 -U postgres -d alertadb_cor -c "SELECT 1;"

# Se funcionar: ✅ Pronto para carga inicial!
```

---

### **Passo 6: Executar Carga Inicial**

```bash
cd /opt/sync-nimbus
python3 scripts/carregar_para_cloudsql_inicial.py
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

---

### **Passo 7: Configurar Automação**

```bash
cd automacao
./configurar_cron_cloudsql.sh
```

OU manualmente:

```bash
chmod +x automacao/cron_cloudsql.sh

crontab -e
# Adicionar:
*/5 * * * * /opt/sync-nimbus/automacao/cron_cloudsql.sh
```

---

### **Passo 8: Verificar Funcionamento**

```bash
# Ver logs
tail -f logs/cloudsql_*.log

# Testar manualmente
python3 scripts/sincronizar_para_cloudsql.py --once

# Verificar cron
crontab -l
```

---

## 📊 Estrutura Final do Projeto

```
/opt/sync-nimbus/
├── .env                                    # ⚙️ ATUALIZADO (novas variáveis)
├── scripts/
│   ├── carregar_pluviometricos_historicos.py  # Existente (NIMBUS→166)
│   ├── sincronizar_pluviometricos_novos.py    # Existente (NIMBUS→166)
│   ├── carregar_para_cloudsql_inicial.py      # 🆕 NOVO (166→Cloud SQL)
│   ├── sincronizar_para_cloudsql.py           # 🆕 NOVO (166→Cloud SQL)
│   └── app.py                                 # Existente (API REST)
├── automacao/
│   ├── cron_linux.sh                          # Existente (NIMBUS→166)
│   ├── configurar_cron_linux.sh               # Existente (NIMBUS→166)
│   ├── cron_cloudsql.sh                       # 🆕 NOVO (166→Cloud SQL)
│   └── configurar_cron_cloudsql.sh            # 🆕 NOVO (166→Cloud SQL)
└── logs/
    ├── sincronizacao_*.log                    # Logs NIMBUS→166
    └── cloudsql_*.log                         # 🆕 Logs 166→Cloud SQL
```

---

## 🔄 Fluxo Completo de Sincronização

### **NIMBUS → Servidor 166** (Existente)

```bash
# Carga inicial (já executado)
python3 scripts/carregar_pluviometricos_historicos.py

# Sync contínuo (cron ativo)
*/5 * * * * /opt/sync-nimbus/automacao/cron_linux.sh
```

### **Servidor 166 → Cloud SQL** (Novo)

```bash
# Carga inicial (executar uma vez)
python3 scripts/carregar_para_cloudsql_inicial.py

# Sync contínuo (novo cron)
*/5 * * * * /opt/sync-nimbus/automacao/cron_cloudsql.sh
```

---

## 📈 Monitoramento

### **Ver Logs em Tempo Real**

```bash
# Logs NIMBUS → 166 (existente)
tail -f logs/sincronizacao_*.log

# Logs 166 → Cloud SQL (novo)
tail -f logs/cloudsql_*.log
```

### **Verificar Dados**

```bash
# Servidor 166
psql -h localhost -U postgres -d alertadb_cor \
  -c "SELECT COUNT(*), MAX(dia) FROM pluviometricos;"

# Cloud SQL
psql -h 34.82.95.242 -U postgres -d alertadb_cor \
  -c "SELECT COUNT(*), MAX(dia) FROM pluviometricos;"
```

### **Comparar Contagens**

```bash
# Script de validação
cat > /tmp/validar_sync.sh << 'EOF'
#!/bin/bash
echo "NIMBUS → 166:"
psql -h localhost -U postgres -d alertadb_cor -t -c "SELECT COUNT(*) FROM pluviometricos;"

echo "166 → Cloud SQL:"
psql -h 34.82.95.242 -U postgres -d alertadb_cor -t -c "SELECT COUNT(*) FROM pluviometricos;"
EOF

chmod +x /tmp/validar_sync.sh
/tmp/validar_sync.sh
```

---

## 🐛 Troubleshooting

### **Erro: "Could not connect to DESTINO (Cloud SQL)"**

**Verificar:**
```bash
# IP está autorizado?
curl https://api.ipify.org

# Porta aberta?
telnet 34.82.95.242 5432

# Senha correta?
grep CLOUDSQL_PASSWORD .env
```

---

### **Erro: "Tabela está VAZIA"**

**Solução:**
```bash
# Executar carga inicial primeiro
python3 scripts/carregar_para_cloudsql_inicial.py
```

---

### **Script não executa no cron**

**Verificar:**
```bash
# Caminho do Python
which python3

# Permissões
ls -la automacao/cron_cloudsql.sh

# Testar manualmente
cd /opt/sync-nimbus
automacao/cron_cloudsql.sh
```

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

## 🎯 Comandos Essenciais

```bash
# Executar carga inicial
python3 scripts/carregar_para_cloudsql_inicial.py

# Testar sync incremental
python3 scripts/sincronizar_para_cloudsql.py --once

# Configurar cron
./automacao/configurar_cron_cloudsql.sh

# Ver logs
tail -f logs/cloudsql_*.log

# Verificar cron
crontab -l | grep cloudsql

# Testar conexão
psql -h 34.82.95.242 -U postgres -d alertadb_cor -c "SELECT COUNT(*) FROM pluviometricos;"
```

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
