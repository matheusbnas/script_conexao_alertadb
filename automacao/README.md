# 🤖 Automação

Automação oficial do projeto focada em **Prefect + Docker**.

---

## 🚀 Opções de Automação

### Prefect + Docker (padrão do projeto)

#### Prefect Cloud ☁️ (Recomendado - executa mesmo com máquina desligada)

**Vantagens:**
- ✅ Executa mesmo quando máquina está desligada
- ✅ Interface web sempre disponível
- ✅ Monitoramento e alertas integrados
- ✅ Gratuito para uso básico

**Configuração rápida:**
```bash
# 1. Instalar Prefect
pip install prefect prefect-gcp

# 2. Fazer login no Cloud
prefect cloud login

# 3. Criar work pool no Prefect Cloud UI
# 4. Deploy do workflow
prefect deploy --name sincronizacao-bigquery-combinada   # usa prefect.yaml na raiz

# 5. Iniciar agent em servidor dedicado
prefect agent start seu-work-pool
```

**📚 Documentação completa:** [../docs/PREFECT_GUIA_COMPLETO.md](../docs/PREFECT_GUIA_COMPLETO.md)

#### Prefect Local 🖥️ (só funciona com máquina ligada)

**Vantagens:**
- ✅ Sem necessidade de conta Cloud
- ✅ Controle total local

**Configuração:**
```bash
# 1. Instalar Prefect
pip install prefect prefect-gcp

# 2. Configurar servidor local
./configurar_prefect.sh

# 3. Iniciar via Docker Compose (na raiz do projeto)
docker compose up -d

# 4. Ou executar workflow diretamente
python scripts/prefect/flows.py --run-once
```

**📚 Documentação:** [../docs/PREFECT_GUIA_COMPLETO.md](../docs/PREFECT_GUIA_COMPLETO.md)

---

## 📋 Scripts Disponíveis

### Prefect
- **configurar_prefect.sh** - Configura `PREFECT_API_URL` para servidor local
- **Documentação:** [../docs/PREFECT_GUIA_COMPLETO.md](../docs/PREFECT_GUIA_COMPLETO.md)

### Observação
- O legado de automação via cron/monitor foi removido para simplificar operação.

---

## 📚 Documentação Completa

- [Automação Guia Completo](../docs/AUTOMACAO_GUIA_COMPLETO.md) - Guia completo de configuração
- [Prefect](../docs/PREFECT_GUIA_COMPLETO.md) - Guia do Prefect
- [README Principal](../README.md)
