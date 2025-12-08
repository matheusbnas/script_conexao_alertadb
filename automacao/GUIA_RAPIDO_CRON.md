# ⚡ Guia Rápido - Configurar Cron para Sincronização Automática

Este é um guia rápido para configurar o cron após executar a carga inicial.

---

## ✅ Pré-requisito Obrigatório

**⚠️ IMPORTANTE:** Execute PRIMEIRO a carga inicial:

```bash
python scripts/carregar_pluviometricos_historicos.py
```

Aguarde a conclusão antes de configurar o cron.

---

## 🚀 Configuração Rápida (Linux)

### Método 1: Script Automático (Recomendado)

```bash
cd automacao
chmod +x configurar_cron_linux.sh cron_linux.sh
./configurar_cron_linux.sh
```

Pronto! O cron está configurado para executar a cada 5 minutos.

### Método 2: Manual

```bash
# 1. Tornar executável
chmod +x automacao/cron_linux.sh

# 2. Obter caminho absoluto
cd automacao
CRON_PATH=$(pwd)/cron_linux.sh
echo $CRON_PATH

# 3. Adicionar ao crontab
crontab -e
# Adicione esta linha (substitua pelo caminho acima):
*/5 * * * * /caminho/completo/para/automacao/cron_linux.sh
```

---

## 🧪 Testar Antes de Configurar

```bash
# Teste manual primeiro
python scripts/sincronizar_pluviometricos_novos.py --once

# Se funcionar, teste o script de cron
./automacao/cron_linux.sh
```

---

## 📊 Verificar se Está Funcionando

```bash
# Ver logs
ls -lt logs/ | head -5
tail -20 logs/sincronizacao_*.log

# Verificar no banco
# A última sincronização deve estar próxima do momento atual
```

---

## 🔧 Remover Cron

```bash
crontab -e  # Remova a linha correspondente
```

---

## 📚 Documentação Completa

Para mais detalhes, consulte: [docs/CONFIGURAR_CRON.md](../docs/CONFIGURAR_CRON.md)

---

**Dica:** Sempre teste manualmente antes de confiar no cron! 🎯

