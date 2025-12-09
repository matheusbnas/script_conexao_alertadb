# 🤖 Automação

Scripts para executar sincronização automaticamente via cron.

---

## 🚀 Configuração Rápida

**1. Carga inicial (obrigatório antes de configurar cron):**
```bash
# Servidor 166
python scripts/servidor166/carregar_pluviometricos_historicos.py

# Cloud SQL
python scripts/cloudsql/carregar_para_cloudsql_inicial.py
```

**2. Configurar cron:**
```bash
# Sincronização normal (servidor 166)
./configurar_cron.sh normal

# Sincronização Cloud SQL
./configurar_cron.sh cloudsql
```

---

## 📋 Scripts Disponíveis

- **cron.sh** - Script unificado de execução (aceita: `normal` ou `cloudsql`)
- **configurar_cron.sh** - Configuração automática do cron (aceita: `normal` ou `cloudsql`)

---

## 📚 Documentação Completa

- [Configurar Cron](../docs/CONFIGURAR_CRON.md) - Guia completo de configuração
- [README Principal](../README.md)
