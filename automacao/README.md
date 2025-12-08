# 🤖 Automação - Execução Automática da Sincronização

Este diretório contém scripts e configurações para executar automaticamente o script `sincronizar_pluviometricos_novos.py` em intervalos regulares (padrão: 5 minutos).

---

## 📋 Pré-requisitos

**⚠️ IMPORTANTE:** Antes de configurar a automação, execute PRIMEIRO a carga inicial:

```bash
python scripts/carregar_pluviometricos_historicos.py
```

Aguarde a conclusão antes de configurar a automação.

---

## 🚀 Opções de Automação

### 1. **Cron (Linux/Unix)** - Recomendado para servidores Linux

**Vantagens:**
- ✅ Nativo do sistema operacional
- ✅ Confiável e estável
- ✅ Fácil de configurar
- ✅ Logs automáticos

**Como usar:**

```bash
cd automacao
chmod +x configurar_cron_linux.sh cron_linux.sh
./configurar_cron_linux.sh
```

**Ou manualmente:**

```bash
# 1. Tornar executável
chmod +x automacao/cron_linux.sh

# 2. Adicionar ao crontab
crontab -e
# Adicione: */5 * * * * /caminho/completo/para/automacao/cron_linux.sh
```

**Verificar logs:**

```bash
ls -lt logs/ | head -5
tail -20 logs/sincronizacao_*.log
```

---

### 2. **Prefect** - Recomendado para orquestração avançada

**Vantagens:**
- ✅ UI web para monitoramento
- ✅ Retry automático em caso de falha
- ✅ Histórico de execuções
- ✅ Notificações e alertas

**Como usar:**

```bash
# 1. Instalar Prefect
pip install prefect

# 2. Iniciar servidor Prefect (opcional, para UI)
prefect server start

# 3. Executar o flow
python automacao/prefect_flow.py

# 4. Ou criar deployment para execução automática
python automacao/prefect_deployment.py
```

**Acessar UI:**

- Abra o navegador em: `http://localhost:4200`

---

## ⚙️ Configuração de Intervalo

O intervalo padrão é **5 minutos**. Para alterar:

### Cron (Linux)

Edite o crontab:

```bash
crontab -e
# Altere */5 para o intervalo desejado (em minutos)
# Exemplo: */10 = a cada 10 minutos
```

### Prefect

Configure a variável de ambiente:

```bash
# No arquivo .env
PREFECT_INTERVALO_MINUTOS=5
```

Ou edite diretamente em `automacao/prefect_flow.py`:

```python
INTERVALO_MINUTOS = 5  # Altere para o valor desejado
```

---

## 🧪 Testar Antes de Configurar

**Sempre teste manualmente primeiro:**

```bash
# Teste o script em modo único
python scripts/sincronizar_pluviometricos_novos.py --once

# Se funcionar, teste o script de automação
./automacao/cron_linux.sh
```

---

## 📊 Monitoramento

### Verificar se está funcionando

```bash
# Ver logs recentes
tail -f logs/sincronizacao_*.log

# Verificar última execução
ls -lt logs/ | head -1
```

### Verificar no banco de dados

```sql
-- Verificar último registro sincronizado
SELECT MAX(dia) as ultima_sincronizacao 
FROM pluviometricos;

-- Deve estar próximo do momento atual (diferença de até 5-10 minutos)
```

---

## 🔧 Solução de Problemas

### Script não executa

1. **Verifique permissões:**
   ```bash
   chmod +x automacao/cron_linux.sh
   ```

2. **Verifique caminhos:**
   - Certifique-se de que todos os caminhos estão corretos
   - Use caminhos absolutos no cron/agendador

3. **Verifique Python:**
   ```bash
   which python3
   ```

### Logs não são criados

1. **Verifique permissões de escrita:**
   ```bash
   mkdir -p logs
   chmod 755 logs
   ```

### Erro de conexão com banco

1. **Verifique variáveis de ambiente:**
   - Certifique-se de que o arquivo `.env` está configurado corretamente
   - Verifique se as credenciais estão corretas

2. **Teste conexão manualmente:**
   ```bash
   python scripts/sincronizar_pluviometricos_novos.py --once
   ```

---

## 📁 Estrutura de Arquivos

```
automacao/
├── README.md                    # Este arquivo
├── GUIA_RAPIDO_CRON.md          # Guia rápido de configuração
├── cron_linux.sh                # Script de cron para Linux
├── cron_cloudsql.sh            # Script de cron para Cloud SQL
├── configurar_cron_linux.sh     # Script de configuração automática
├── configurar_cron_cloudsql.sh  # Script de configuração Cloud SQL
├── prefect_flow.py              # Flow Prefect para orquestração
└── prefect_deployment.py        # Deployment Prefect
```

---

## 🆘 Suporte

Para mais informações, consulte:

- [Guia Rápido](./GUIA_RAPIDO_CRON.md)
- [Documentação Completa](../docs/CONFIGURAR_CRON.md)
- [README Principal](../README.md)

---

**Dica:** Sempre teste manualmente antes de confiar na automação! 🎯

