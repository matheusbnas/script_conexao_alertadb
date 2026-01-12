# 🔄 Workflows Prefect - BigQuery

## 📁 Estrutura de Arquivos

A estrutura foi organizada seguindo boas práticas de separação de responsabilidades:

```
scripts/prefect/
├── prefect_common_tasks.py              # Tasks compartilhadas (verificação de conexões)
├── prefect_helpers.py                   # Funções auxiliares reutilizáveis
├── prefect_interval_manager.py          # Gerenciador de intervalo dinâmico
├── prefect_service.py                  # Serviço de execução contínua
├── prefect_workflow_pluviometricos.py   # Workflow para dados PLUVIOMÉTRICOS
├── prefect_workflow_meteorologicos.py   # Workflow para dados METEOROLÓGICOS
├── prefect_workflow_combinado.py        # Workflow COMBINADO (usa apenas 1 deployment)
├── prefect.service                      # Arquivo systemd (Linux)
├── docker-entrypoint.sh                 # Script de entrada Docker
├── INSTALACAO_SERVICO.md                # Guia de instalação do serviço
└── README.md                            # Este arquivo
```

## 🎯 Por que separar?

### ✅ Vantagens da separação:

1. **Responsabilidade única**: Cada arquivo tem uma responsabilidade clara
2. **Manutenção facilitada**: Mudanças em um tipo de dado não afetam o outro
3. **Deploy independente**: Cada workflow pode ser deployado separadamente
4. **Escalabilidade**: Fácil adicionar novos tipos de dados no futuro
5. **Código reutilizável**: Tasks e helpers comuns em módulos separados
6. **Testes isolados**: Cada workflow pode ser testado independentemente
7. **Organização**: Arquivos Prefect separados dos scripts BigQuery

### ❌ Problemas do arquivo único:

- Arquivo muito grande e difícil de manter
- Mudanças em um tipo afetam o outro
- Deploy conjunto (não pode parar um sem parar o outro)
- Dificulta escalabilidade
- Mistura lógica de orquestração com scripts de sincronização

## 🚀 Como usar

### ⚠️ Limite de Deployments no Prefect Cloud

O plano gratuito do Prefect Cloud tem limite de **5 deployments**. Se você atingir esse limite, verá o erro:
```
You have reached the maximum number of deployments for your workspace. Current limit: 5 deployments.
```

### Soluções para o limite de deployments:

#### Solução 1: Flow Combinado (RECOMENDADO - usa apenas 1 deployment)

Use o flow combinado que sincroniza ambos os tipos em um único deployment:

```bash
# Executar sem criar deployment (teste)
python scripts/prefect/prefect_workflow_combinado.py --run-once

# Ou criar deployment (usa apenas 1 deployment)
python scripts/prefect/prefect_workflow_combinado.py
```

#### Solução 2: Executar sem criar deployment

Execute os workflows diretamente sem criar deployment (útil para testes ou quando limite atingido):

**Pluviométricos:**
```bash
python scripts/prefect/prefect_workflow_pluviometricos.py --run-once
```

**Meteorológicos:**
```bash
python scripts/prefect/prefect_workflow_meteorologicos.py --run-once
```

#### Solução 3: Deletar deployments antigos

1. Acesse o Prefect Cloud UI
2. Vá em Deployments
3. Delete deployments antigos que não são mais necessários
4. Execute os workflows novamente

#### Solução 4: Usar Prefect Local

Use Prefect Local (sem limite de deployments):

```bash
# Descomente no arquivo: os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
prefect server start
python scripts/prefect/prefect_workflow_pluviometricos.py
```

### Opção 2: Deploy no Prefect Cloud (quando há espaço)

**Pluviométricos:**
```bash
prefect deploy scripts/prefect/prefect_workflow_pluviometricos.py:sincronizacao_pluviometricos_flow --pool seu-work-pool
```

**Meteorológicos:**
```bash
prefect deploy scripts/prefect/prefect_workflow_meteorologicos.py:sincronizacao_meteorologicos_flow --pool seu-work-pool
```

**Flow Combinado (usa apenas 1 deployment):**
```bash
prefect deploy scripts/prefect/prefect_workflow_combinado.py:sincronizacao_combinada_flow --pool seu-work-pool
```

### Opção 2: Deploy no Prefect Cloud

**Pluviométricos:**
```bash
prefect deploy scripts/prefect/prefect_workflow_pluviometricos.py:sincronizacao_pluviometricos_flow --pool seu-work-pool
```

**Meteorológicos:**
```bash
prefect deploy scripts/prefect/prefect_workflow_meteorologicos.py:sincronizacao_meteorologicos_flow --pool seu-work-pool
```

### Opção 3: Executar ambos simultaneamente

Se você quiser executar ambos ao mesmo tempo, pode usar um script wrapper ou executar em terminais separados.

## 📊 Tabelas BigQuery

- **Pluviométricos**: `alertadb_cor_raw.pluviometricos`
- **Meteorológicos**: `alertadb_cor_raw.meteorologicos`

## ⚙️ Configuração

Ambos os workflows usam as mesmas variáveis de ambiente do `.env`:
- `DB_ORIGEM_HOST`, `DB_ORIGEM_NAME`, `DB_ORIGEM_USER`, `DB_ORIGEM_PASSWORD`
- `BIGQUERY_PROJECT_ID`
- `BIGQUERY_DATASET_ID_NIMBUS` (padrão: `alertadb_cor_raw`)

## 🔄 Agendamento

Por padrão, ambos os workflows executam a cada 5 minutos (`*/5 * * * *`).

Para alterar o agendamento, edite o parâmetro `cron` no método `.serve()` de cada arquivo.

## 📂 Relação com scripts BigQuery

Os workflows Prefect executam os scripts de sincronização que estão em:
- `scripts/bigquery/sincronizar_pluviometricos_nimbus_bigquery.py`
- `scripts/bigquery/sincronizar_meteorologicos_nimbus_bigquery.py`

Esta separação mantém:
- **Scripts BigQuery**: Lógica de sincronização e exportação
- **Workflows Prefect**: Orquestração, agendamento e monitoramento

---

## 🚀 Execução Contínua como Serviço

Para executar automaticamente sem intervenção manual, com ajuste automático de intervalo e reinício automático:

### ⭐ RECOMENDADO: Docker

```bash
# Construir e executar
docker-compose up -d

# Ver logs
docker-compose logs -f

# Parar
docker-compose down
```

### 🖥️ Alternativa: systemd (Linux)

```bash
# Instalar serviço
sudo cp scripts/prefect/prefect.service /etc/systemd/system/prefect-bigquery.service
sudo systemctl daemon-reload
sudo systemctl enable prefect-bigquery.service
sudo systemctl start prefect-bigquery.service
```

### 📖 Documentação Completa

Veja `INSTALACAO_SERVICO.md` para guia completo de instalação e configuração.

### ⚙️ Funcionalidades do Serviço

- ✅ **Ajuste automático de intervalo**: Se coleta demorar mais de 5 minutos, ajusta automaticamente
- ✅ **Reinício automático**: Reinicia automaticamente em caso de falha
- ✅ **Detecção de lacunas**: Detecta lacunas grandes e ajusta intervalo temporariamente
- ✅ **Estado persistente**: Mantém intervalo ajustado entre reinícios
- ✅ **Execução contínua**: Roda indefinidamente sem intervenção manual

### 🔄 Como Funciona o Ajuste Automático

1. **Intervalo padrão**: 5 minutos
2. **Se execução demorar mais de 5 minutos**:
   - Intervalo ajustado para `tempo_execucao * 1.5`
   - Arredondado para múltiplo de 5 minutos
   - Exemplo: 8 minutos → intervalo vira 15 minutos
3. **Se há lacuna grande** (> 1 dia):
   - Intervalo temporário maior (15-60 minutos)
   - Volta ao normal quando lacuna for resolvida
4. **Se execução ficar muito rápida**:
   - Intervalo reduzido gradualmente até voltar ao padrão
