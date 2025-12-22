# ⏰ Automação - Guia Completo

Guia completo para automatizar a sincronização de dados, incluindo configuração de cron e comparação de diferentes opções de automação.

---

## 📋 Índice

1. [Visão Geral](#visão-geral)
2. [Configuração de Cron](#configuração-de-cron)
3. [Opções de Automação](#opções-de-automação)
4. [Troubleshooting](#troubleshooting)

---

## 🎯 Visão Geral

Este guia explica como configurar o cron para executar automaticamente os scripts de sincronização incremental após a carga inicial dos dados históricos.

### Pré-requisitos

Antes de configurar o cron, certifique-se de que:

1. ✅ **Carga inicial concluída**: 
   - Para servidor 166: Executeu `carregar_pluviometricos_historicos.py`
   - Para Cloud SQL: Executeu `carregar_para_cloudsql_inicial.py`
   - Para BigQuery: Executeu `exportar_pluviometricos_nimbus_bigquery.py`
2. ✅ **Tabela populada**: A tabela `pluviometricos` contém dados históricos
3. ✅ **Arquivo .env configurado**: Todas as variáveis de ambiente estão corretas
4. ✅ **Script testado manualmente**: 
   - `sincronizar_pluviometricos_novos.py --once` (servidor 166)
   - `sincronizar_para_cloudsql.py --once` (Cloud SQL)
   - `sincronizar_pluviometricos_nimbus_bigquery.py --once` (BigQuery)

---

## ⏰ Configuração de Cron

### Opção 1: Usando o Script de Configuração Automática (Recomendado)

1. **Navegue até a pasta de automação:**
   ```bash
   cd automacao
   ```

2. **Torne o script executável:**
   ```bash
   chmod +x configurar_cron.sh cron.sh
   ```

3. **Execute o script de configuração:**
   ```bash
   # Para sincronização normal (servidor 166)
   ./configurar_cron.sh normal
   # ou apenas: ./configurar_cron.sh
   
   # Para sincronização Cloud SQL
   ./configurar_cron.sh cloudsql
   
   # Para sincronização BigQuery
   ./configurar_cron.sh bigquery
   ```

O script irá:
- ✅ Verificar se o script de cron existe
- ✅ Tornar o script executável
- ✅ Adicionar entrada ao crontab automaticamente
- ✅ Configurar para executar a cada 5 minutos

### Opção 2: Configuração Manual

1. **Edite o crontab:**
   ```bash
   crontab -e
   ```

2. **Adicione a seguinte linha:**
   ```bash
   # Para sincronização normal (servidor 166)
   */5 * * * * /caminho/completo/para/automacao/cron.sh normal
   
   # Para sincronização Cloud SQL
   */5 * * * * /caminho/completo/para/automacao/cron.sh cloudsql
   
   # Para sincronização BigQuery
   */5 * * * * /caminho/completo/para/automacao/cron.sh bigquery
   ```

   **Exemplo:**
   ```bash
   # Normal
   */5 * * * * /home/usuario/repos/testarconexao/automacao/cron.sh normal
   
   # Cloud SQL
   */5 * * * * /home/usuario/repos/testarconexao/automacao/cron.sh cloudsql
   
   # BigQuery
   */5 * * * * /home/usuario/repos/testarconexao/automacao/cron.sh bigquery
   ```

3. **Salve e feche o editor** (no vim: `:wq`, no nano: `Ctrl+X` e depois `Y`)

4. **Verifique se foi adicionado:**
   ```bash
   crontab -l
   ```

### Formato Cron

```
* * * * * comando
│ │ │ │ │
│ │ │ │ └─── Dia da semana (0-7, 0 e 7 = domingo)
│ │ │ └───── Mês (1-12)
│ │ └─────── Dia do mês (1-31)
│ └───────── Hora (0-23)
└─────────── Minuto (0-59)
```

### Exemplos de Agendamento

| Cron | Descrição |
|------|-----------|
| `* * * * *` | A cada 1 minuto |
| `*/5 * * * *` | A cada 5 minutos (padrão) |
| `*/10 * * * *` | A cada 10 minutos |
| `0 * * * *` | A cada hora |
| `0 2 * * *` | Diariamente às 2h |
| `0 3 * * 0` | Semanalmente (domingo às 3h) |
| `0 0 1 * *` | Mensalmente (dia 1 às 0h) |

### Alterar Intervalo de Execução

Edite o crontab:
```bash
crontab -e
```

Altere o intervalo (exemplos):
- A cada 1 minuto: `* * * * * /caminho/automacao/cron.sh normal`
- A cada 5 minutos: `*/5 * * * * /caminho/automacao/cron.sh normal` (padrão)
- A cada 10 minutos: `*/10 * * * * /caminho/automacao/cron.sh normal`
- A cada hora: `0 * * * * /caminho/automacao/cron.sh normal`

---

## 🤖 Opções de Automação

### Comparação Rápida

| Solução | Complexidade | Interface Visual | Retry Automático | Logs Detalhados | Custo |
|---------|--------------|------------------|------------------|-----------------|-------|
| **Cron (Linux)** | ⭐ Simples | ❌ Não | ❌ Não | ⚠️ Básico | ✅ Grátis |
| **APScheduler** | ⭐⭐ Média | ❌ Não | ⚠️ Manual | ⚠️ Básico | ✅ Grátis |
| **Script Loop + Systemd** | ⭐⭐ Média | ❌ Não | ✅ Sim | ⚠️ Básico | ✅ Grátis |

---

### 1️⃣ CRON (Linux/Unix) - ⭐ RECOMENDADO PARA SIMPLICIDADE

#### ✅ Vantagens:
- **Muito simples**: Configuração em uma linha
- **Nativo do sistema**: Já vem instalado no Linux
- **Leve**: Não consome recursos extras
- **Confiável**: Usado há décadas em produção
- **Fácil manutenção**: Crontab é bem documentado

#### ❌ Desvantagens:
- **Sem retry automático**: Se falhar, precisa esperar próximo ciclo
- **Sem interface visual**: Tudo via linha de comando
- **Logs básicos**: Precisa configurar redirecionamento manual

#### 📝 Como Funciona:
```bash
# Adiciona ao crontab para executar a cada 5 minutos
*/5 * * * * /caminho/para/automacao/cron.sh normal
```

#### 💡 Quando Usar:
- Servidor Linux/Unix
- Precisa de solução simples e confiável
- Não precisa de interface visual ou retry automático

---

### 2️⃣ APSCHEDULER (Python) - ⭐ ALTERNATIVA PYTHON SIMPLES

#### ✅ Vantagens:
- **Python puro**: Tudo em Python, fácil de integrar
- **Flexível**: Muitas opções de agendamento
- **Leve**: Mais leve que Prefect
- **Sem servidor**: Roda como processo normal

#### ❌ Desvantagens:
- **Sem interface visual**: Tudo via código
- **Retry manual**: Precisa implementar você mesmo
- **Logs básicos**: Precisa configurar logging

#### 📝 Como Funciona:
```python
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler()
scheduler.add_job(sync, 'interval', minutes=5)
scheduler.start()
```

#### 💡 Quando Usar:
- Quer tudo em Python
- Não precisa de interface visual
- Solução intermediária entre cron e Prefect

---

### 3️⃣ SCRIPT LOOP + SYSTEMD (Linux) - ⭐ SERVIÇO DO SISTEMA

#### ✅ Vantagens:
- **Serviço do sistema**: Inicia automaticamente com o sistema
- **Gerenciado pelo sistema**: systemd cuida de restart
- **Logs do sistema**: Integrado com journald
- **Confiável**: Reinicia automaticamente se falhar

#### ❌ Desvantagens:
- **Mais complexo**: Precisa criar arquivo .service
- **Sem interface visual**: Tudo via linha de comando

#### 📝 Como Funciona:

Criar arquivo `/etc/systemd/system/sync-pluviometricos.service`:

```ini
[Unit]
Description=Sincronização Pluviométrica
After=network.target

[Service]
ExecStart=/usr/bin/python3 /caminho/sincronizar_pluviometricos_novos.py
Restart=always

[Install]
WantedBy=multi-user.target
```

Ativar serviço:
```bash
sudo systemctl enable sync-pluviometricos.service
sudo systemctl start sync-pluviometricos.service
```

#### 💡 Quando Usar:
- Servidor Linux em produção
- Precisa que inicie automaticamente
- Quer que o sistema gerencie o processo

---

## 🔍 Verificação e Monitoramento

### Verificar Logs (Linux)

Os logs são salvos em `logs/sincronizacao_YYYYMMDD_HHMMSS.log` ou `logs/cloudsql_YYYYMMDD_HHMMSS.log`:

```bash
# Ver último log criado
ls -lt logs/ | head -5

# Ver conteúdo do último log
tail -f logs/sincronizacao_*.log | tail -20

# Ver logs Cloud SQL
tail -f logs/cloudsql_*.log | tail -20
```

### Testar Execução Manual

Antes de confiar no cron, teste manualmente:

```bash
cd /caminho/do/projeto

# Teste sincronização normal
python3 scripts/servidor166/sincronizar_pluviometricos_novos.py --once

# Teste sincronização Cloud SQL
python3 scripts/cloudsql/sincronizar_para_cloudsql.py --once
```

### Verificar Última Sincronização

Execute no banco de destino:

```sql
SELECT MAX(dia) as ultima_sincronizacao, COUNT(*) as total_registros 
FROM pluviometricos;
```

A última sincronização deve estar próxima do momento atual (dentro de alguns minutos).

### Alertas

Considere configurar alertas se:
- A última sincronização estiver muito antiga (> 10 minutos)
- Nenhum log novo foi criado nas últimas horas
- Os logs mostram erros recorrentes

---

## 🛠️ Troubleshooting

### Cron não está executando

1. **Verifique se o cron está rodando:**
   ```bash
   # Linux
   sudo systemctl status cron
   # ou
   sudo service cron status
   ```

2. **Verifique os logs do sistema:**
   ```bash
   # Linux
   grep CRON /var/log/syslog | tail -20
   ```

3. **Verifique permissões:**
   ```bash
   chmod +x automacao/cron.sh
   ```

4. **Teste o script manualmente:**
   ```bash
   # Teste sincronização normal
   ./automacao/cron.sh normal
   
   # Teste sincronização Cloud SQL
   ./automacao/cron.sh cloudsql
   
   # Teste sincronização BigQuery
   ./automacao/cron.sh bigquery
   ```

### Script falha silenciosamente

1. **Verifique os logs em `logs/`:**
   ```bash
   ls -lt logs/
   cat logs/sincronizacao_*.log | tail -50
   ```

2. **Verifique variáveis de ambiente:**
   ```bash
   # O script precisa encontrar o arquivo .env
   # Certifique-se de estar no diretório raiz do projeto
   ```

3. **Teste conexões manualmente:**
   ```bash
   # Normal
   python3 scripts/servidor166/sincronizar_pluviometricos_novos.py --once
   
   # Cloud SQL
   python3 scripts/cloudsql/sincronizar_para_cloudsql.py --once
   
   # BigQuery
   python3 scripts/bigquery/sincronizar_pluviometricos_nimbus_bigquery.py --once
   ```

### Erro de caminho não encontrado

Certifique-se de usar **caminhos absolutos** no crontab:

```bash
# ❌ ERRADO (caminho relativo)
*/5 * * * * ./automacao/cron.sh normal

# ✅ CORRETO (caminho absoluto)
*/5 * * * * /home/usuario/repos/testarconexao/automacao/cron.sh normal
```

---

## 🔄 Remover Configuração do Cron

### Linux

1. **Edite o crontab:**
   ```bash
   crontab -e
   ```

2. **Remova a linha correspondente** e salve

3. **Ou remova todas as entradas:**
   ```bash
   crontab -r  # ⚠️ Remove TODAS as entradas do crontab
   ```

---

## 🎯 Recomendação

### Para Começar (Simples):
1. **Cron (Linux)**: Use Cron com `cron.sh` - Simples e confiável

### Para Produção (Avançado):
1. **Systemd**: Se quer serviço do sistema Linux com restart automático
2. **APScheduler**: Se prefere tudo em Python sem dependências externas

---

## ✅ Checklist Final

Antes de considerar a configuração completa:

- [ ] Carga inicial executada com sucesso
- [ ] Script de sincronização testado manualmente (`--once`)
- [ ] Cron/Agendador configurado
- [ ] Logs sendo gerados corretamente
- [ ] Última sincronização verificada no banco
- [ ] Monitoramento configurado (opcional)

---

## 💡 Dicas

1. **Sempre teste manualmente primeiro** antes de confiar no cron
2. **Monitore os logs regularmente** nas primeiras semanas
3. **Use caminhos absolutos** no crontab para evitar problemas
4. **Configure alertas** se possível para detectar falhas rapidamente
5. **Documente** onde está rodando o cron para facilitar manutenção futura

---

## 📚 Documentação Relacionada

- [README.md](../README.md) - Documentação principal
- [CLOUD_SQL_GUIA_COMPLETO.md](CLOUD_SQL_GUIA_COMPLETO.md) - Guia Cloud SQL
- [BIGQUERY_GUIA_COMPLETO.md](BIGQUERY_GUIA_COMPLETO.md) - Guia BigQuery

---

**Última atualização:** 2025

