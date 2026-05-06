# 🤖 Automação - Guia Completo (Prefect + Docker)

Guia oficial para automatizar a sincronização de dados usando **Prefect + Docker**.
Este projeto não utiliza mais cron como caminho recomendado.

---

## 📋 Índice

1. [Visão Geral](#visão-geral)
2. [Configuração Prefect + Docker](#configuração-prefect--docker)
3. [Docker: só pluviométricos, só meteorológicos ou combinado](#docker-só-pluviométricos-só-meteorológicos-ou-combinado)
4. [Operação Diária](#operação-diária)
5. [Troubleshooting](#troubleshooting)

---

## 🎯 Visão Geral

Este guia explica como executar o pipeline continuamente via `docker compose`, com
orquestração pelo Prefect e alerta automático por e-mail em caso de falha.

### Pré-requisitos

Antes de subir o serviço, certifique-se de que:

1. ✅ **Carga inicial concluída**:
   - Para servidor 166: Execute `carregar_pluviometricos_historicos.py`
   - Para BigQuery: Execute `exportar_pluviometricos_nimbus_bigquery.py`
2. ✅ **Tabela populada**: A tabela `pluviometricos` contém dados históricos
3. ✅ **Arquivo .env configurado**: Todas as variáveis de ambiente estão corretas
4. ✅ **Script testado manualmente**:
   - `sincronizar_pluviometricos_novos.py --once` (servidor 166)
   - `sincronizar_pluviometricos_nimbus_bigquery.py --once` (BigQuery)

---

## ⚙️ Configuração Prefect + Docker

### 1) Configurar Prefect local (opcional para UI local)

```bash
cd automacao
./configurar_prefect.sh local-api
```

### 2) Subir serviços

Na raiz do projeto (onde está o `docker-compose.yml`):

```bash
docker compose up -d
```

O comando acima sobe o perfil padrão definido no Compose (em geral inclui o **`prefect-service`**, workflow **combinado**: pluviométricos + meteorológicos).

Para escolher explicitamente **somente pluviométricos**, **somente meteorológicos** ou **combinado**, use a seção [Docker: só pluviométricos, só meteorológicos ou combinado](#docker-só-pluviométricos-só-meteorológicos-ou-combinado).

### 3) Verificar containers

```bash
docker compose ps
```

### 4) Ver logs

```bash
docker compose logs -f prefect-service
```

### 5) Alertas por e-mail

Configure no `.env`:

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

---

## Docker: só pluviométricos, só meteorológicos ou combinado

O `docker-compose.yml` define estes serviços relevantes:

| Serviço | Container (nome) | O que executa |
|---------|------------------|---------------|
| `prefect-server` | `prefect-server-local` | API/UI do Prefect (`http://localhost:4200`) |
| `prefect-service` | `prefect-bigquery-sync` | Incremental **combinado** (pluviométricos + meteorológicos) |
| `prefect-pluviometricos` | `prefect-bigquery-pluviometricos` | Incremental **apenas** pluviométricos |
| `prefect-meteorologicos` | `prefect-bigquery-meteorologicos` | Incremental **apenas** meteorológicos |

Os workers (`prefect-service`, `prefect-pluviometricos`, `prefect-meteorologicos`) dependem do `prefect-server`.

### Somente pluviométricos (sem meteorológicos)

```bash
docker compose up -d prefect-server prefect-pluviometricos
docker compose logs -f prefect-pluviometricos
```

### Somente meteorológicos (sem pluviométricos)

```bash
docker compose up -d prefect-server prefect-meteorologicos
docker compose logs -f prefect-meteorologicos
```

### Combinado (pluviométricos + meteorológicos em um único worker)

```bash
docker compose up -d prefect-server prefect-service
docker compose logs -f prefect-service
```

### Ambos workers dedicados (pluviométricos e meteorológicos em paralelo)

Útil quando você **não** quer o combinado e prefere dois processos independentes:

```bash
docker compose up -d --build prefect-server prefect-pluviometricos prefect-meteorologicos
```

### Parar só um worker (ex.: manutenção na tabela meteorológica)

Sem derrubar o servidor Prefect nem o outro tipo de sync:

```bash
docker compose stop prefect-meteorologicos
# ou, pelo nome do container:
docker stop prefect-bigquery-meteorologicos
```

Para religar:

```bash
docker compose start prefect-meteorologicos
```

### Evitar trabalho duplicado

Não mantenha o **`prefect-service` (combinado)** rodando ao mesmo tempo que os workers **`prefect-pluviometricos`** e **`prefect-meteorologicos`** se o objetivo for “um único modo”. Isso pode **duplicar** execuções e carga contra NIMBUS/BigQuery. Escolha **uma** estratégia:

- só **combinado** (`prefect-server` + `prefect-service`), ou  
- só **dedicados** (`prefect-server` + um ou dois workers dedicados).

Mais detalhes e comandos auxiliares: [`scripts/prefect/README.md`](../scripts/prefect/README.md).

---

## 🔍 Operação Diária

### Verificar logs

```bash
docker compose logs -f prefect-service
docker compose logs -f prefect-pluviometricos
docker compose logs -f prefect-meteorologicos
```

### Reiniciar serviço

```bash
docker compose restart prefect-service
```

Reinício apenas do worker meteorológico ou pluviométrico dedicado:

```bash
docker compose restart prefect-meteorologicos
docker compose restart prefect-pluviometricos
```

### Atualizar imagem/código e reaplicar

Reconstrói e sobe todos os serviços do Compose declarados ao subir (depende do que estiver rodando):

```bash
docker compose up -d --build
```

Exemplo reconstruir **somente** o par servidor + meteorológicos:

```bash
docker compose up -d --build prefect-server prefect-meteorologicos
```

---

## 🛠️ Troubleshooting

### Container não sobe

1. Verifique logs do container:
   ```bash
   docker compose logs --tail=100 prefect-service
   ```
2. Confirme variáveis do `.env`.
3. Confirme `credentials/credentials.json`.

### E-mail de alerta não chega

1. Verifique se `PREFECT_ALERT_EMAIL_TO` está preenchido.
2. Verifique `SMTP_USER` e `SMTP_APP_PASSWORD`.
3. Se Gmail, use senha de app (não senha normal).
4. Confira logs:
   ```bash
   docker compose logs prefect-service | rg "Alerta|SMTP|email|e-mail"
   ```

### Serviço está “Up” mas sem atualização

1. Verifique se há falhas nos logs do workflow.
2. Verifique conectividade com NIMBUS e credenciais GCP.
3. Execute manualmente:
   ```bash
   python scripts/prefect/flows.py --run-once
   ```

## 💡 Dicas

1. Sempre teste `--run-once` após alterar scripts
2. Monitore logs diariamente na primeira semana
3. Configure alerta por e-mail antes de produção
4. Use **`prefect-service` (combinado)** como padrão operacional completo; use **`prefect-pluviometricos`** ou **`prefect-meteorologicos`** quando precisar isolar um tipo de dado ou pausar só uma pipeline

---

## 📚 Documentação Relacionada

- [README.md](../README.md) - Documentação principal
- [BIGQUERY_GUIA_COMPLETO.md](BIGQUERY_GUIA_COMPLETO.md) - Guia BigQuery completo
- [PREFECT_GUIA_COMPLETO.md](PREFECT_GUIA_COMPLETO.md) - Guia Prefect (Cloud, Docker, local)

---

**Última atualização:** 2026

