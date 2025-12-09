# ⚡ GUIA RÁPIDO - Cloud SQL

## 🚀 Instalação Rápida (15 min)

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

---

## 📋 Comandos Essenciais

### **Sincronização**

```bash
# Carga inicial (uma vez)
python3 scripts/cloudsql/carregar_para_cloudsql_inicial.py

# Sync incremental manual
python3 scripts/cloudsql/sincronizar_para_cloudsql.py --once

# Sync contínuo
python3 scripts/cloudsql/sincronizar_para_cloudsql.py
```

### **Automação**

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

### **Logs**

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

### **Validação**

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

### **Conexão**

```bash
# Testar ping
ping -c 3 34.82.95.242

# Testar porta
telnet 34.82.95.242 5432

# Testar psql
psql -h 34.82.95.242 -U postgres -d alertadb_cor

# Ver IP público do servidor
curl https://api.ipify.org
```

---

## 🐛 Troubleshooting Rápido

### **Erro: Conexão recusada**

```bash
# Verificar IP autorizado
curl https://api.ipify.org
# Liberar no Cloud SQL

# Testar porta
telnet 34.82.95.242 5432
```

### **Erro: Tabela vazia**

```bash
# Executar carga inicial
python3 scripts/cloudsql/carregar_para_cloudsql_inicial.py
```

### **Erro: Senha incorreta**

```bash
# Verificar .env
grep CLOUDSQL_PASSWORD .env

# Resetar senha no console GCP
```

### **Cron não executa**

```bash
# Ver caminho Python
which python3

# Usar caminho completo no cron
crontab -e
# Mudar para: */5 * * * * /usr/bin/python3 ...
```

---

## 📊 One-Liners Úteis

```bash
# Status geral
python3 scripts/cloudsql/sincronizar_para_cloudsql.py --once; \
psql -h 34.82.95.242 -U postgres -d alertadb_cor -c "SELECT COUNT(*), MAX(dia) FROM pluviometricos;"

# Ver última sincronização
tail -1 logs/cloudsql_*.log

# Forçar sync agora
./automacao/cron.sh cloudsql

# Comparar dados
echo "Servidor 166:" && psql -h localhost -U postgres -d alertadb_cor -t -c "SELECT COUNT(*) FROM pluviometricos;" && \
echo "Cloud SQL:" && psql -h 34.82.95.242 -U postgres -d alertadb_cor -t -c "SELECT COUNT(*) FROM pluviometricos;"
```

---

## 🔥 Comandos de Emergência

```bash
# Parar sync
crontab -e
# Comentar linha (adicionar #)

# Limpar logs antigos
find logs/ -name "cloudsql_*.log" -mtime +30 -delete

# Reconectar se travou
pkill -f sincronizar_para_cloudsql.py

# Verificar se está rodando
ps aux | grep sincronizar_para_cloudsql
```

---

## 📝 Variáveis .env Necessárias

```env
CLOUDSQL_HOST=34.82.95.242
CLOUDSQL_PORT=5432
CLOUDSQL_DATABASE=alertadb_cor
CLOUDSQL_USER=postgres
CLOUDSQL_PASSWORD=senha_aqui
CLOUDSQL_SSLMODE=require
```

---

## ✅ Checklist Diário

```bash
# 1. Ver último sync
tail -5 logs/cloudsql_*.log

# 2. Comparar contagens
echo "166:" && psql -h localhost -U postgres -d alertadb_cor -t -c "SELECT COUNT(*) FROM pluviometricos;"
echo "SQL:" && psql -h 34.82.95.242 -U postgres -d alertadb_cor -t -c "SELECT COUNT(*) FROM pluviometricos;"

# 3. Ver último timestamp
psql -h 34.82.95.242 -U postgres -d alertadb_cor -c "SELECT MAX(dia) FROM pluviometricos;"

# 4. Verificar cron
crontab -l | grep cloudsql
```

---

**Dica:** Salve este arquivo em local de fácil acesso! 📌
