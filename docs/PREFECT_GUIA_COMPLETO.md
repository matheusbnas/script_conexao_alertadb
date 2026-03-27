# 🔄 Prefect — Guia Completo (Cloud, Local e Docker)

Guia unificado para orquestrar a sincronização NIMBUS → BigQuery via Prefect v3,
cobrindo todas as formas de execução: Cloud, servidor local e Docker.

---

## 🎯 Escolha a forma de execução

| Opção | Máquina precisa ficar ligada? | UI web | Configuração |
|---|---|---|---|
| **Prefect Cloud** | ❌ Não | ✅ Sempre disponível | Moderada |
| **Docker local** | ✅ Sim | ✅ `localhost:4200` | Simples |
| **CLI local** | ✅ Sim | ✅ `localhost:4200` | Simples |
| **`--run-once`** | ✅ Sim | ❌ Sem UI | Mínima |

---

## 📋 Pré-requisitos (todas as opções)

```bash
pip install prefect prefect-gcp
```

- Arquivo `.env` configurado na raiz do projeto
- `credentials/credentials.json` (service account GCP)
- Variáveis obrigatórias no `.env`:
  ```env
  DB_ORIGEM_HOST=...
  DB_ORIGEM_NAME=alertadb
  DB_ORIGEM_USER=...
  DB_ORIGEM_PASSWORD=...
  BIGQUERY_PROJECT_ID=...
  BIGQUERY_DATASET_ID_NIMBUS=alertadb_cor_raw
  ```

---

## 🐳 Opção 1 — Docker (recomendado para uso contínuo local)

Sobe serviços separados com UI do Prefect em tempo real.

### Subir serviços separados (recomendado)

```bash
docker compose up -d --build prefect-server prefect-pluviometricos prefect-meteorologicos
```

Ou o fluxo combinado (pluviométricos + meteorológicos em um único container):

```bash
docker compose up -d --build prefect-server prefect-service
```

### Monitoramento

```bash
# Ver status dos containers
docker compose ps

# Logs em tempo real
docker compose logs -f prefect-pluviometricos
docker compose logs -f prefect-meteorologicos

# Parar tudo
docker compose down
```

### UI Prefect local

Abra no navegador: `http://localhost:4200`

Se estiver em servidor remoto: `http://IP_DO_SERVIDOR:4200`

### Intervalo de atualização

Controlado por `PREFECT_INTERVALO` no `.env` (padrão: `5` minutos):

```env
PREFECT_INTERVALO=5
```

Após alterar, reaplique:

```bash
docker compose up -d --build prefect-pluviometricos prefect-meteorologicos
```

### Verificações

```bash
# Confirmar credenciais GCP
ls -la credentials/credentials.json

# Ver últimas execuções (estado salvo localmente)
ls -la scripts/prefect/state/
```

---

## ☁️ Opção 2 — Prefect Cloud

Executa mesmo com a máquina desligada. Requer conta gratuita em [app.prefect.cloud](https://app.prefect.cloud/).

### Passo 1: Criar conta e workspace

1. Acesse: https://app.prefect.cloud/
2. Crie uma conta (plano gratuito disponível)
3. Crie ou selecione um workspace

### Passo 2: Fazer login

```bash
prefect cloud login
```

### Passo 3: Verificar configuração

```bash
prefect config view | grep PREFECT_API_URL
# Deve mostrar: PREFECT_API_URL='https://api.prefect.cloud/api/...'
```

### Passo 4: Criar Work Pool

No **Prefect Cloud UI → Work Pools → Create Work Pool**, escolha:

**Google Cloud Run** (recomendado — executa no GCP sem servidor dedicado)
- Executa no GCP, próximo ao BigQuery
- Não precisa manter máquina ligada
- Custo muito baixo (centavos/mês)

**Process** (alternativa — precisa de servidor dedicado sempre ligado)
- Mais simples de configurar
- Sem custo adicional do GCP

Nome sugerido para ambos: `bigquery-sync-pool`

### Passo 5: Deploy

```bash
# Deploy de todos os workflows (definidos no prefect.yaml da raiz)
prefect deploy --all

# Ou deploy individual
prefect deploy --name sincronizacao-bigquery-combinada
```

### Passo 6: Iniciar agent (apenas para Process)

```bash
prefect agent start bigquery-sync-pool
```

Para iniciar automaticamente no Linux, configure systemd ou supervisor.

---

## 🖥️ Opção 3 — Servidor Prefect local (CLI)

Sem Docker, com UI local.

```bash
# Terminal 1: iniciar servidor
prefect server start

# Terminal 2: configurar para usar servidor local
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

# Deploy e execução
prefect deploy --all
```

Ou use o script de configuração:

```bash
./automacao/configurar_prefect.sh
```

---

## ▶️ Execução manual (sem deployment)

Para rodar **uma vez** sem criar deployment (ideal para testes ou cron):

```bash
# Flow combinado (padrão)
python scripts/prefect/flows.py --run-once

# Apenas pluviométricos
python scripts/prefect/flows.py --run-once --flow pluviometricos

# Apenas meteorológicos
python scripts/prefect/flows.py --run-once --flow meteorologicos
```

**Windows (PowerShell)** — evitar erro de encoding:

```powershell
$env:PYTHONIOENCODING='utf-8'
python scripts/prefect/flows.py --run-once
```

**Serviço contínuo local** (loop a cada N min, sem Docker):

```bash
python scripts/prefect/service.py --workflow combinado --intervalo 5
```

---

## 📊 O que o workflow faz

1. Verifica conexão com o NIMBUS e credenciais GCP
2. Detecta lacunas de dados (registros no NIMBUS ainda não sincronizados)
3. Executa sincronização incremental via scripts `sincronizar_*.py`
4. Verifica status final no BigQuery
5. Reporta estatísticas (registros processados, datas, erros)
6. [Futuro] Dispara inferência IA LNCC/Gypscie

---

## 📁 Arquivos relevantes

| Arquivo | Descrição |
|---|---|
| `scripts/prefect/flows.py` | Flows de sincronização |
| `scripts/prefect/tasks.py` | Tasks Prefect (conexões, sync, stub IA) |
| `scripts/prefect/constants.py` | SQL queries e constantes |
| `scripts/prefect/service.py` | Serviço em loop (Docker/systemd) |
| `prefect.yaml` | Deployments e schedules (raiz do projeto) |
| `docker-compose.yml` | Serviços Docker |
| `scripts/prefect/prefect.service` | Template systemd (Linux) |

---

## 🛠️ Troubleshooting

### Alertas por e-mail em caso de falha

O `scripts/prefect/service.py` envia alerta automático por e-mail quando um workflow falha
(com cooldown para evitar spam). Configure no `.env`:

```env
PREFECT_ALERT_EMAIL_TO=matheusbnas@gmail.com,matheus.bernardes@cor.rio
PREFECT_ALERT_COOLDOWN_MINUTES=30

SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=seu_email@gmail.com
SMTP_APP_PASSWORD=sua_app_password
ALERT_FROM=seu_email@gmail.com
SMTP_STARTTLS=true
```

> Se `PREFECT_ALERT_EMAIL_TO` não estiver configurado, os alertas ficam desativados.

### Erro 401 Unauthorized (Prefect Cloud)

```bash
# Refazer login
prefect cloud login

# Ou usar modo local
prefect config unset PREFECT_API_URL
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

# Ou rodar sem deployment
python scripts/prefect/flows.py --run-once
```

### Porta 4200 não abre (Docker)

```bash
# Verificar se o container prefect-server está rodando
docker compose ps

# Ver logs do servidor
docker compose logs prefect-server
```

### Credenciais GCP não encontradas

```bash
ls -la credentials/credentials.json
# Deve existir. Copie o arquivo de service account para credentials/
```

### Erro "No module named 'scripts.bigquery'"

Execute sempre a partir da **raiz do projeto** (`python scripts/prefect/flows.py`), não de dentro da pasta `scripts/prefect/`.

### Limite de deployments no Prefect Cloud

O plano gratuito tem limite de 5 deployments. Use o **flow combinado** (1 deployment) ou rode via Docker/`--run-once` (não consomem deployments).

---

## 🔗 Referências

- [Prefect Cloud](https://app.prefect.cloud/)
- [Prefect v3 Quickstart](https://docs.prefect.io/v3/get-started/quickstart)
- [Prefect GCP Integration](https://docs.prefect.io/latest/integrations/google-cloud/)
- [Docker Compose — `docker-compose.yml`](../docker-compose.yml)
