# ⏰ Configuração de Cron - Sincronização Automática em Tempo Real

Este guia explica como configurar o cron para executar automaticamente o script de sincronização incremental após a carga inicial dos dados históricos.

---

## 📋 Pré-requisitos

Antes de configurar o cron, certifique-se de que:

1. ✅ **Carga inicial concluída**: Executeu `carregar_pluviometricos_historicos.py` com sucesso
2. ✅ **Tabela populada**: A tabela `pluviometricos` contém dados históricos
3. ✅ **Arquivo .env configurado**: Todas as variáveis de ambiente estão corretas
4. ✅ **Script testado manualmente**: `sincronizar_pluviometricos_novos.py --once` funciona corretamente

---

## 🚀 Passo a Passo - Linux/Unix

### Opção 1: Usando o Script de Configuração Automática (Recomendado)

1. **Navegue até a pasta de automação:**
```bash
cd automacao
```

2. **Torne o script executável:**
```bash
chmod +x configurar_cron_linux.sh
chmod +x cron_linux.sh
```

3. **Execute o script de configuração:**
```bash
./configurar_cron_linux.sh
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
*/5 * * * * /caminho/completo/para/automacao/cron_linux.sh
```

**Exemplo:**
```bash
*/5 * * * * /home/usuario/repos/testarconexao/automacao/cron_linux.sh
```

3. **Salve e feche o editor** (no vim: `:wq`, no nano: `Ctrl+X` e depois `Y`)

4. **Verifique se foi adicionado:**
```bash
crontab -l
```

---

## 🔍 Verificação e Monitoramento

### Verificar Logs (Linux)

Os logs são salvos em `logs/sincronizacao_YYYYMMDD_HHMMSS.log`:

```bash
# Ver último log criado
ls -lt logs/ | head -5

# Ver conteúdo do último log
tail -f logs/sincronizacao_*.log | tail -20
```

### Testar Execução Manual

Antes de confiar no cron, teste manualmente:

```bash
cd /caminho/do/projeto
python3 scripts/sincronizar_pluviometricos_novos.py --once
```

---

## ⚙️ Configuração Avançada

### Alterar Intervalo de Execução

Edite o crontab:
```bash
crontab -e
```

Altere o intervalo (exemplos):
- A cada 1 minuto: `* * * * *`
- A cada 5 minutos: `*/5 * * * *` (padrão)
- A cada 10 minutos: `*/10 * * * *`
- A cada hora: `0 * * * *`

### Usar Variável de Ambiente para Intervalo

Você pode configurar o intervalo via arquivo `.env`:

```env
INTERVALO_VERIFICACAO=300  # 300 segundos = 5 minutos
```

**Nota:** Esta variável é usada apenas quando o script roda em modo contínuo (`python sincronizar_pluviometricos_novos.py`). No modo cron (com `--once`), o intervalo é controlado pelo cron/agendador.

---

## 🛠️ Solução de Problemas

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
chmod +x automacao/cron_linux.sh
```

4. **Teste o script manualmente:**
```bash
./automacao/cron_linux.sh
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
python3 scripts/sincronizar_pluviometricos_novos.py --once
```

### Erro de caminho não encontrado

Certifique-se de usar **caminhos absolutos** no crontab:

```bash
# ❌ ERRADO (caminho relativo)
*/5 * * * * ./automacao/cron_linux.sh

# ✅ CORRETO (caminho absoluto)
*/5 * * * * /home/usuario/repos/testarconexao/automacao/cron_linux.sh
```

---

## 📊 Monitoramento Recomendado

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

## ✅ Checklist Final

Antes de considerar a configuração completa:

- [ ] Carga inicial executada com sucesso
- [ ] Script de sincronização testado manualmente (`--once`)
- [ ] Cron/Agendador configurado
- [ ] Logs sendo gerados corretamente
- [ ] Última sincronização verificada no banco
- [ ] Monitoramento configurado (opcional)

---

## 📚 Documentação Relacionada

- [Opções de Automação](OPCOES_AUTOMACAO.md) - Comparação de diferentes soluções
- [Estrutura do Projeto](../ESTRUTURA_PROJETO.md) - Visão geral do projeto
- [Configuração](../CONFIGURACAO_EXEMPLO.md) - Exemplo de arquivo .env

---

## 💡 Dicas

1. **Sempre teste manualmente primeiro** antes de confiar no cron
2. **Monitore os logs regularmente** nas primeiras semanas
3. **Use caminhos absolutos** no crontab para evitar problemas
4. **Configure alertas** se possível para detectar falhas rapidamente
5. **Documente** onde está rodando o cron para facilitar manutenção futura

---

**Última atualização:** 2024

