# 🚀 Instalação do Serviço Prefect - Execução Contínua

Este guia explica como configurar o serviço Prefect para executar automaticamente com:
- ✅ Ajuste automático de intervalo (se coleta demorar mais de 5 minutos)
- ✅ Reinício automático em caso de falha
- ✅ Execução contínua sem intervenção manual
- ✅ Suporte a Docker e systemd (Linux)

---

## 📋 Pré-requisitos

1. **Python 3.11+** instalado
2. **Dependências** instaladas: `pip install -r requirements.txt`
3. **Arquivo `.env`** configurado com credenciais
4. **Credenciais GCP** em `credentials/credentials.json`

---

## 🐳 OPÇÃO 1: Docker (Recomendado)

### Vantagens:
- ✅ Isolamento completo
- ✅ Fácil de gerenciar
- ✅ Reinício automático
- ✅ Funciona em qualquer sistema

### Passo 1: Configurar variáveis de ambiente

Crie ou edite `.env` na raiz do projeto com todas as variáveis necessárias.

### Passo 2: Construir e executar

```bash
# Construir imagem
docker-compose build

# Executar em background
docker-compose up -d

# Ver logs
docker-compose logs -f

# Parar
docker-compose down
```

### Passo 3: Verificar status

```bash
# Ver status do container
docker-compose ps

# Ver logs em tempo real
docker-compose logs -f prefect-service

# Verificar saúde do serviço
docker-compose exec prefect-service python -c "import os; print('OK' if os.path.exists('/app/scripts/prefect/.prefect_service_state.json') else 'ERRO')"
```

---

## 🖥️ OPÇÃO 2: systemd (Linux - Servidor 166)

### Vantagens:
- ✅ Integração nativa com sistema
- ✅ Inicia automaticamente com o sistema
- ✅ Gerenciado pelo systemd
- ✅ Logs integrados com journald

### Passo 1: Criar arquivo de serviço

Edite o arquivo `scripts/prefect/prefect.service` e ajuste os caminhos:

```ini
[Unit]
Description=Prefect Service - Sincronização BigQuery
After=network.target

[Service]
Type=simple
User=seu_usuario
WorkingDirectory=/caminho/completo/do/projeto
Environment="PATH=/usr/bin:/usr/local/bin"
ExecStart=/usr/bin/python3 /caminho/completo/do/projeto/scripts/prefect/prefect_service.py --workflow combinado --intervalo 5
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

### Passo 2: Instalar serviço

```bash
# Copiar arquivo de serviço
sudo cp scripts/prefect/prefect.service /etc/systemd/system/prefect-bigquery.service

# Recarregar systemd
sudo systemctl daemon-reload

# Habilitar para iniciar automaticamente
sudo systemctl enable prefect-bigquery.service

# Iniciar serviço
sudo systemctl start prefect-bigquery.service

# Verificar status
sudo systemctl status prefect-bigquery.service
```

### Passo 3: Gerenciar serviço

```bash
# Ver status
sudo systemctl status prefect-bigquery.service

# Ver logs
sudo journalctl -u prefect-bigquery.service -f

# Parar
sudo systemctl stop prefect-bigquery.service

# Reiniciar
sudo systemctl restart prefect-bigquery.service

# Desabilitar (não iniciar automaticamente)
sudo systemctl disable prefect-bigquery.service
```

---

## 🔄 OPÇÃO 3: Execução Manual (Desenvolvimento/Teste)

Para testes ou desenvolvimento:

```bash
# Executar workflow combinado
python scripts/prefect/prefect_service.py --workflow combinado --intervalo 5

# Executar apenas pluviométricos
python scripts/prefect/prefect_service.py --workflow pluviometricos --intervalo 5

# Executar apenas meteorológicos
python scripts/prefect/prefect_service.py --workflow meteorologicos --intervalo 5
```

---

## ⚙️ Como Funciona o Ajuste Automático

### 1. **Intervalo Padrão**: 5 minutos

### 2. **Ajuste Automático**:
   - Se execução demorar **mais de 5 minutos**:
     - Intervalo ajustado para `tempo_execucao * 1.5`
     - Arredondado para múltiplo de 5 minutos
     - Exemplo: Se demorar 8 minutos → intervalo vira 15 minutos
   
   - Se há **lacuna grande** (> 1 dia):
     - Intervalo temporário maior (15-60 minutos)
     - Volta ao normal quando lacuna for resolvida
   
   - Se execução ficar **muito rápida**:
     - Intervalo reduzido gradualmente até voltar ao padrão

### 3. **Estado Persistente**:
   - Estado salvo em `scripts/prefect/.prefect_service_state.json`
   - Intervalo atual mantido entre reinícios
   - Última execução registrada

---

## 📊 Monitoramento

### Ver logs (Docker):
```bash
docker-compose logs -f prefect-service
```

### Ver logs (systemd):
```bash
sudo journalctl -u prefect-bigquery.service -f
```

### Verificar estado:
```bash
cat scripts/prefect/.prefect_service_state.json
```

---

## 🔧 Configurações Avançadas

### Alterar intervalo inicial:
```bash
# Docker
docker-compose up -d -e PREFECT_INTERVALO=10

# systemd (editar arquivo .service)
ExecStart=... --intervalo 10
```

### Alterar workflow:
```bash
# Docker
docker-compose up -d -e PREFECT_WORKFLOW=pluviometricos

# systemd (editar arquivo .service)
ExecStart=... --workflow pluviometricos
```

---

## 🐛 Troubleshooting

### Serviço não inicia:
1. Verificar logs: `docker-compose logs` ou `journalctl -u prefect-bigquery.service`
2. Verificar arquivo `.env` existe e está correto
3. Verificar credenciais GCP em `credentials/credentials.json`
4. Verificar permissões de arquivos

### Execução demora muito:
- O serviço ajusta automaticamente o intervalo
- Verifique logs para ver o ajuste
- Se necessário, aumente timeout nos scripts de sincronização

### Reinício em loop:
- Verificar erros nos logs
- Verificar conexão com banco de dados
- Verificar credenciais GCP

---

## ✅ Verificação

Após instalar, verifique:

1. **Serviço rodando**:
   ```bash
   # Docker
   docker-compose ps
   
   # systemd
   sudo systemctl status prefect-bigquery.service
   ```

2. **Logs mostrando execuções**:
   ```bash
   # Docker
   docker-compose logs -f | grep "Executando workflow"
   
   # systemd
   sudo journalctl -u prefect-bigquery.service -f | grep "Executando workflow"
   ```

3. **Estado sendo atualizado**:
   ```bash
   cat scripts/prefect/.prefect_service_state.json
   ```

---

## 📝 Notas Importantes

- ⚠️ **Primeira execução**: Pode demorar muito se houver lacunas grandes
- ⚠️ **Intervalo mínimo**: 5 minutos (não reduz abaixo disso)
- ⚠️ **Reinício automático**: Serviço reinicia automaticamente em caso de falha
- ⚠️ **Estado persistente**: Intervalo ajustado é mantido entre reinícios
