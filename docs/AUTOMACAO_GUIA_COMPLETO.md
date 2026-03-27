# 🤖 Automação - Guia Completo (Prefect + Docker)

Guia oficial para automatizar a sincronização de dados usando **Prefect + Docker**.
Este projeto não utiliza mais cron como caminho recomendado.

---

## 📋 Índice

1. [Visão Geral](#visão-geral)
2. [Configuração Prefect + Docker](#configuração-prefect--docker)
3. [Operação Diária](#operação-diária)
4. [Troubleshooting](#troubleshooting)

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

```bash
cd ..
docker compose up -d
```

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

### Atualizar imagem/código e reaplicar

```bash
docker compose up -d --build
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
4. Use `prefect-service` como padrão e flows dedicados quando necessário

---

## 📚 Documentação Relacionada

- [README.md](../README.md) - Documentação principal
- [BIGQUERY_GUIA_COMPLETO.md](BIGQUERY_GUIA_COMPLETO.md) - Guia BigQuery completo
- [PREFECT_GUIA_COMPLETO.md](PREFECT_GUIA_COMPLETO.md) - Guia Prefect (Cloud, Docker, local)

---

**Última atualização:** 2026

