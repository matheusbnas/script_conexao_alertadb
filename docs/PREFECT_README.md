# 🔄 Workflow Prefect - Sincronização BigQuery

Workflow Prefect para orquestrar a sincronização de dados do NIMBUS para BigQuery com agendamento automático e monitoramento de erros.

## 🎯 Escolha sua Opção

### ☁️ Opção 1: Prefect Cloud (Recomendado - executa mesmo com máquina desligada)

**Vantagens:**
- ✅ Executa mesmo quando máquina está desligada
- ✅ Interface web sempre disponível
- ✅ Monitoramento e alertas integrados
- ✅ Gratuito para uso básico

**Sua API do Prefect Cloud:** `cli-41fbdcc9-2a85-4885-a7cd-4390df02c7e4`

### 🖥️ Opção 2: Prefect Local (só funciona com máquina ligada)

**Vantagens:**
- ✅ Sem necessidade de conta Cloud
- ✅ Controle total local
- ✅ Sem custos

**Limitação:** Precisa manter máquina ligada e servidor rodando

---

## 📋 Pré-requisitos

### 1. Instalar Prefect

```bash
pip install prefect prefect-gcp
```

### 2. Escolher: Cloud ou Local

#### Para Prefect Cloud:
```bash
prefect cloud login
```

#### Para Prefect Local:
```bash
# Configurar para servidor local
./configurar_prefect.sh

# Iniciar servidor (em terminal separado)
prefect server start
```

---

## 🚀 Configuração Prefect Cloud (Passo a Passo)

### Passo 1: Criar Conta e Workspace

1. Acesse: https://app.prefect.cloud/
2. Crie uma conta (gratuita)
3. Crie ou use um workspace

### Passo 2: Fazer Login

```bash
prefect cloud login
```

Siga as instruções no navegador para autenticar.

### Passo 3: Verificar Configuração

```bash
prefect config view | grep PREFECT_API_URL
```

**Deve mostrar:** `PREFECT_API_URL='https://api.prefect.cloud/api/...'`

**Sua API:** `cli-41fbdcc9-2a85-4885-a7cd-4390df02c7e4` (configurada automaticamente após login)

### Passo 4: Criar Work Pool

**No Prefect Cloud UI (https://app.prefect.cloud/):**

1. Vá em **Work Pools** (menu lateral)
2. Clique em **Create Work Pool** ou **+**
3. **Escolha o tipo de infraestrutura:**

#### ☁️ Google Cloud Run (Recomendado - executa sempre, sem servidor dedicado)

**Vantagens:**
- ✅ Executa no GCP (mesmo ambiente do BigQuery)
- ✅ Não precisa manter servidor ligado
- ✅ Executa automaticamente quando necessário
- ✅ Escalável e gerenciado pelo GCP
- ✅ Custo muito baixo (alguns centavos por mês)

**Como configurar:**
1. Selecione **Google Cloud Run** ou **Google Cloud Run V2**
2. Clique em **Next**
3. Configure:
   - **Name:** `bigquery-sync-pool`
   - **Description:** "Pool para sincronização BigQuery"
4. Na próxima tela, configure as credenciais GCP (se necessário)
5. Clique em **Create**

#### 🖥️ Process (Alternativa - precisa de servidor dedicado)

**Vantagens:**
- ✅ Mais simples de configurar
- ✅ Controle total sobre execução
- ✅ Sem custos adicionais do GCP

**Limitação:**
- ❌ Precisa manter servidor/VM sempre ligado

**Como configurar:**
1. Selecione **Process**
2. Clique em **Next**
3. Configure:
   - **Name:** `bigquery-sync-pool`
   - **Description:** "Pool para sincronização BigQuery"
4. Clique em **Create**

**Depois:** Você precisará iniciar um agent em um servidor dedicado:
```bash
prefect agent start bigquery-sync-pool
```

### Passo 5: Deploy do Workflow

```bash
cd scripts/bigquery

# Deploy do workflow
prefect deploy prefect_workflow_bigquery.py:sincronizacao_incremental_flow \
  --name sincronizacao-bigquery-incremental \
  --pool bigquery-sync-pool \
  --cron "*/5 * * * *"
```

### Passo 6: Configurar Agente (Apenas para Process)

Se você escolheu **Process**, inicie o agent em um servidor dedicado:

```bash
prefect agent start bigquery-sync-pool
```

**Para iniciar automaticamente (Linux):** Configure systemd ou supervisor.

---

## 🚀 Como Usar

### Executar Workflow com Agendamento (Recomendado)

O workflow está configurado para executar **a cada 5 minutos** automaticamente:

```bash
python scripts/bigquery/prefect_workflow_bigquery.py
```

O processo ficará rodando e executando automaticamente conforme o agendamento.

### Executar Workflow Uma Vez (Teste)

Edite o arquivo `scripts/bigquery/prefect_workflow_bigquery.py` e descomente:

```python
if __name__ == "__main__":
    sincronizacao_incremental_flow()  # Executa uma vez
```

---

## 📊 O que o Workflow Faz

1. ✅ **Verifica conexões** (NIMBUS e GCP)
2. ✅ **Sincroniza dados incrementais** (apenas dados novos)
3. ✅ **Monitora erros de carregamento** (detecta problemas automaticamente)
4. ✅ **Verifica status final** no BigQuery
5. ✅ **Reporta estatísticas** (registros processados, datas, etc.)

## 🎯 Vantagens

- ✅ **Atualização automática** a cada 5 minutos
- ✅ **Monitoramento de erros** em tempo real
- ✅ **Interface web** para acompanhar execuções
- ✅ **Retry automático** em caso de falha
- ✅ **Logs estruturados** e organizados
- ✅ **Histórico completo** de todas as execuções

---

## 🛠️ Troubleshooting

### Erro: "Can't connect to Server API at https://api.prefect.cloud"

**Solução:** Configure para servidor local ou faça login no Cloud:

```bash
# Para Cloud
prefect cloud login

# Para Local
prefect config unset PREFECT_API_URL
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
```

### Erro: "Connection refused" ao acessar localhost:4200

**Solução:** O servidor Prefect não está rodando. Inicie com:

```bash
prefect server start
```

### Erro: "ModuleNotFoundError: No module named 'prefect'"

**Solução:**

```bash
pip install prefect prefect-gcp
```

### Erro: "No module named 'scripts.bigquery'"

**Solução:** Já corrigido no workflow. Se ainda ocorrer, verifique se está executando do diretório raiz do projeto.

---

## 📚 Arquivos Relacionados

- **Workflow:** `scripts/bigquery/prefect_workflow_bigquery.py`
- **Script de sincronização:** `scripts/bigquery/sincronizar_pluviometricos_nimbus_bigquery.py`
- **Script de exportação:** `scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py`
- **Script de configuração:** `automacao/configurar_prefect.sh`

---

## 🔗 Referências

- [Prefect Cloud](https://app.prefect.cloud/)
- [Prefect Open Source Quickstart](https://docs.prefect.io/v3/get-started/quickstart#open-source)
- [Prefect GCP Integration](https://docs.prefect.io/latest/integrations/google-cloud/)
