# 🌩️ NOVA CAMADA: Servidor 166 → Cloud SQL GCP

## 🎯 O Que Foi Adicionado

Esta é uma **extensão** do projeto existente que adiciona sincronização do Servidor 166 para o Cloud SQL GCP.

---

## 🏗️ Arquitetura Completa

### **ANTES (2 camadas):**
```
NIMBUS (10.2.223.114)
    ↓ [carregar_pluviometricos_historicos.py]
    ↓ [sincronizar_pluviometricos_novos.py]
    ↓ [cron_linux.sh - a cada 5 min]
    ↓
Servidor 166 (alertadb_cor)
    └─ API REST (app.py)
```

### **DEPOIS (3 camadas):**
```
NIMBUS (10.2.223.114)
    ↓ [carregar_pluviometricos_historicos.py]
    ↓ [sincronizar_pluviometricos_novos.py]
    ↓ [cron_linux.sh - a cada 5 min]
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

## 📦 Arquivos Criados

### **Scripts (`scripts/`)**

| Arquivo | Baseado Em | Função |
|---------|-----------|--------|
| `carregar_para_cloudsql_inicial.py` | `carregar_pluviometricos_historicos.py` | Carga inicial completa |
| `sincronizar_para_cloudsql.py` | `sincronizar_pluviometricos_novos.py` | Sync incremental (5 min) |

### **Automação (`automacao/`)**

| Arquivo | Baseado Em | Função |
|---------|-----------|--------|
| `cron.sh cloudsql` | `cron_linux.sh` | Script cron |
| `configurar_cron.sh cloudsql` | `configurar_cron_linux.sh` | Instalador automático |

### **Documentação**

| Arquivo | Descrição |
|---------|-----------|
| `INTEGRACAO_CLOUD_SQL.md` | Guia completo de integração |
| `GUIA_RAPIDO_CLOUD_SQL.md` | Comandos rápidos |
| `.env.completo.example` | Exemplo de .env completo |
| `README_CLOUD_SQL.md` | Este arquivo |

---

## 🚀 Início Rápido

### **1. Atualizar .env**

```bash
# Adicionar ao .env existente:
CLOUDSQL_HOST=34.82.95.242
CLOUDSQL_PORT=5432
CLOUDSQL_DATABASE=alertadb_cor
CLOUDSQL_USER=postgres
CLOUDSQL_PASSWORD=senha_aqui
CLOUDSQL_SSLMODE=require
```

### **2. Liberar IP no Cloud SQL**

```bash
# Descobrir IP
curl https://api.ipify.org

# Liberar no console GCP:
# SQL → alertadb-cor → Connections → Authorized networks
```

### **3. Carga Inicial**

```bash
python3 scripts/cloudsql/carregar_para_cloudsql_inicial.py
```

### **4. Automação**

```bash
./automacao/configurar_cron.sh cloudsql
```

**Pronto!** 🎉

---

## 📊 Compatibilidade

### **Scripts Existentes** ✅
- ✅ `carregar_pluviometricos_historicos.py` - Continua funcionando
- ✅ `sincronizar_pluviometricos_novos.py` - Continua funcionando
- ✅ `app.py` - Continua funcionando
- ✅ `cron_linux.sh` - Continua funcionando

### **Sem Conflitos**
- ✅ Usa variáveis diferentes no .env (`CLOUDSQL_*`)
- ✅ Logs separados (`cloudsql_*.log` vs `sincronizacao_*.log`)
- ✅ Cron independente
- ✅ Não afeta sincronização NIMBUS→166

---

## 🔄 Como Funciona

### **Camada 1: NIMBUS → 166** (Existente)
```bash
# Cron a cada 5 min
*/5 * * * * /opt/sync-nimbus/automacao/cron_linux.sh

# Busca novos dados da NIMBUS
# Sincroniza para alertadb_cor (servidor 166)
```

### **Camada 2: 166 → Cloud SQL** (Novo)
```bash
# Cron a cada 5 min
*/5 * * * * /opt/sync-nimbus/automacao/cron.sh cloudsql

# Busca novos dados do alertadb_cor (servidor 166)
# Sincroniza para Cloud SQL GCP
```

**Resultado:** Dados fluem automaticamente através das 3 camadas! 🌊

---

## 📝 Características

### **Mesma Lógica dos Scripts Existentes**
- ✅ DISTINCT ON para evitar duplicatas
- ✅ ON CONFLICT DO UPDATE para atualizar dados
- ✅ Timezone preservado (-02:00 / -03:00)
- ✅ Processamento em lotes (10.000 registros)
- ✅ Modo --once para cron
- ✅ Logs detalhados

### **Vantagens**
- ✅ Não mexe na NIMBUS (zero risco)
- ✅ Aproveita dados já sincronizados no 166
- ✅ Latência zero (localhost → internet)
- ✅ Mesma estrutura do projeto existente
- ✅ Fácil integração e manutenção

---

## 🐛 Troubleshooting

Ver: [GUIA_RAPIDO_CLOUD_SQL.md](GUIA_RAPIDO_CLOUD_SQL.md)

---

## 📚 Documentação Completa

- **Integração:** [INTEGRACAO_CLOUD_SQL.md](INTEGRACAO_CLOUD_SQL.md)
- **Comandos:** [GUIA_RAPIDO_CLOUD_SQL.md](GUIA_RAPIDO_CLOUD_SQL.md)
- **Config .env:** [.env.completo.example](.env.completo.example)

---

## ✅ Status

```
✅ Scripts criados e testados
✅ Segue padrão do projeto existente
✅ Documentação completa
✅ Compatível com sistema atual
✅ Pronto para produção
```

---

## 🎯 Próximos Passos

1. ✅ Atualizar .env
2. ✅ Liberar IP no Cloud SQL
3. ✅ Executar carga inicial
4. ✅ Configurar cron
5. ✅ Monitorar por 24h

---

**Criado por:** Matheus Bernardes - Matech AI  
**Data:** Dezembro 2025  
**Versão:** 1.0  
**Status:** ✅ Pronto para Uso
