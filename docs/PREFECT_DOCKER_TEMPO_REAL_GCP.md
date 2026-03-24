# Prefect + Docker em Tempo Real (NIMBUS -> GCP)

Este guia mostra como rodar a atualizacao dos dados da NIMBUS para o BigQuery com Docker, usando:

- servicos separados (`pluviometricos` e `meteorologicos`);
- execucao continua a cada 5 minutos;
- visualizacao em tempo real na interface web do Prefect local.

---

## Objetivo

Subir o ambiente para:

- coletar dados da NIMBUS;
- atualizar tabelas no BigQuery (GCP);
- monitorar execucoes em tempo real via UI do Prefect.

---

## Pre-requisitos

1. Docker e Docker Compose instalados.
2. Arquivo `.env` configurado na raiz do projeto.
3. Arquivo de credenciais GCP em `credentials/credentials.json`.

Variaveis essenciais no `.env`:

- `DB_ORIGEM_HOST`, `DB_ORIGEM_PORT`, `DB_ORIGEM_NAME`, `DB_ORIGEM_USER`, `DB_ORIGEM_PASSWORD`
- `BIGQUERY_PROJECT_ID`
- `BIGQUERY_DATASET_ID_NIMBUS` (ex: `alertadb_cor_raw`)
- `BIGQUERY_TABLE_ID=pluviometricos`
- `BIGQUERY_TABLE_ID_METEOROLOGICOS=meteorologicos`
- `PREFECT_INTERVALO=5`

---

## Servicos no docker-compose

Este projeto usa os seguintes servicos:

- `prefect-server`: interface web local do Prefect (`http://localhost:4200`)
- `prefect-pluviometricos`: atualiza apenas pluviometricos
- `prefect-meteorologicos`: atualiza apenas meteorologicos
- `prefect-service`: fluxo combinado (opcional, nao usar junto se quiser isolamento)

Para evitar impacto cruzado em caso de falha, rode apenas os servicos separados.

---

## Subir servicos separados (recomendado)

Na raiz do projeto:

```bash
docker compose down
docker compose up -d --build prefect-server prefect-pluviometricos prefect-meteorologicos
```

Verificar status:

```bash
docker compose ps
```

---

## Monitoramento em tempo real (estilo Prefect Online)

1. Abra no navegador:
   - `http://localhost:4200`
2. Acompanhe os flow runs em tempo real.
3. Verifique logs por servico:

```bash
docker compose logs -f prefect-pluviometricos
docker compose logs -f prefect-meteorologicos
```

---

## Intervalo de atualizacao (5 minutos)

O intervalo e controlado por `PREFECT_INTERVALO` no `.env`:

```env
PREFECT_INTERVALO=5
```

Depois de alterar, reaplique:

```bash
docker compose up -d --build prefect-pluviometricos prefect-meteorologicos
```

---

## Validacoes importantes

1. Confirmar que as credenciais existem:

```bash
ls -la credentials/credentials.json
```

2. Confirmar conectividade e execucoes:

```bash
docker compose logs --tail=100 prefect-pluviometricos
docker compose logs --tail=100 prefect-meteorologicos
```

3. Conferir ultima execucao dos estados locais:

```bash
ls -la scripts/prefect/state
```

---

## Troubleshooting rapido

### Erro: credenciais nao encontradas

- Garanta o arquivo em `credentials/credentials.json`.
- Suba novamente os containers.

### Nao aparece atualizacao no BigQuery

- Confira se `BIGQUERY_PROJECT_ID` e dataset/tabelas estao corretos no `.env`.
- Verifique logs dos dois servicos separados.
- Confira na UI do Prefect se os runs estao `Completed` ou `Failed`.

### Porta 4200 nao abre

- Verifique se `prefect-server` esta `Up` no `docker compose ps`.
- Se estiver em servidor remoto, abra `http://IP_DO_SERVIDOR:4200`.

---

## Comandos uteis

Subir somente pluviometricos:

```bash
docker compose up -d --build prefect-server prefect-pluviometricos
```

Subir somente meteorologicos:

```bash
docker compose up -d --build prefect-server prefect-meteorologicos
```

Parar tudo:

```bash
docker compose down
```

