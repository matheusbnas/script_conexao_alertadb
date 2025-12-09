# ⚙️ Ajustes Recomendados para Cloud SQL

Com base nas especificações do seu Cloud SQL, aqui estão os ajustes recomendados:

---

## ✅ Configurações Atuais (Boa)

- **PostgreSQL 17.7** - Versão recente e compatível ✅
- **8 vCPU, 64 GB RAM** - Excelente capacidade ✅
- **Cache de dados: 375 GB** - Ótimo para performance ✅
- **Capacidade de rede: 2.000 MB/s** - Excelente ✅
- **IOPS: 9.000/15.000** - Boa capacidade ✅

---

## ⚠️ Ajustes Recomendados

### 1. **Armazenamento: 100 GB SSD**

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

### 2. **Backup: Manual**

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

### 3. **Recuperação Pontual: Desativada**

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

### 4. **Disponibilidade: Única Zona**

**Status:** ⚠️ OK para desenvolvimento/teste, não recomendado para produção

**Recomendação:**
- Para produção, considere **Alta Disponibilidade (HA)**
- HA oferece redundância entre zonas
- 99.95% de SLA vs 99.5% (zona única)
- Custo adicional: ~2x

---

## 🚀 Otimizações Já Implementadas nos Scripts

Os scripts já incluem otimizações automáticas durante a carga:

### Durante Carga Inicial:
- ✅ `synchronous_commit = off` - Melhora performance (desabilitado após carga)
- ✅ `work_mem = 256MB` - Melhora ordenações/agregações
- ✅ `maintenance_work_mem = 1GB` - Melhora operações de manutenção
- ✅ `autovacuum_enabled = false` - Desabilitado durante carga (reabilitado após)

### Após Carga:
- ✅ Todas as configurações são restauradas para valores padrão
- ✅ Autovacuum reabilitado automaticamente

---

## 📊 Monitoramento Recomendado

### Durante Carga Inicial:

1. **Monitorar uso de storage:**
   ```sql
   SELECT pg_size_pretty(pg_database_size('alertadb_cor'));
   ```

2. **Monitorar conexões:**
   ```sql
   SELECT count(*) FROM pg_stat_activity;
   ```

3. **Monitorar performance:**
   - Console GCP → SQL → Instâncias → `alertadb-cor` → **Monitoring**
   - Verificar CPU, RAM, IOPS, Latência

### Após Carga:

1. **Executar ANALYZE:**
   ```sql
   ANALYZE pluviometricos;
   ```

2. **Verificar índices:**
   ```sql
   SELECT indexname, indexdef 
   FROM pg_indexes 
   WHERE tablename = 'pluviometricos';
   ```

---

## 🔧 Configurações Adicionais (Opcional)

### Aumentar Limite de Conexões

Se necessário, ajuste `max_connections`:

```sql
-- Verificar limite atual
SHOW max_connections;

-- No Cloud SQL, configure via:
-- Console GCP → SQL → Instâncias → alertadb-cor → Edit
-- → Flags → Adicionar flag: max_connections = 100
```

### Configurar Timeout

Os scripts já incluem `connect_timeout = 10` segundos.

---

## 📝 Checklist de Ajustes

- [ ] Verificar tamanho dos dados no servidor 166
- [ ] Aumentar storage se necessário (> 50 GB de dados)
- [ ] Ativar backup automático
- [ ] Ativar recuperação pontual (PITR)
- [ ] Considerar HA para produção (opcional)
- [ ] Executar ANALYZE após carga inicial
- [ ] Monitorar performance durante carga

---

## 🆘 Suporte

Para ajustar configurações no Cloud SQL:
- Console GCP: https://console.cloud.google.com/sql/instances
- Documentação: https://cloud.google.com/sql/docs/postgres

---

**Última atualização:** 2025

