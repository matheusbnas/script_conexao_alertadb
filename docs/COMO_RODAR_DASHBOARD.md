# 🚀 Como Rodar o Dashboard

O dashboard está integrado ao Flask e é servido automaticamente quando você inicia a API.

---

## 📋 Pré-requisitos

1. ✅ Python 3.7+ instalado
2. ✅ Dependências instaladas: `pip install -r requirements.txt`
3. ✅ Arquivo `.env` configurado com as credenciais do banco
4. ✅ Banco `alertadb_cor` populado com dados (execute `carregar_pluviometricos_historicos.py` primeiro)

---

## 🚀 Passo a Passo

### 1. Iniciar a API Flask

```bash
# Navegue até a raiz do projeto
cd /scripts/

# Inicie a API
python scripts/servidor166/app.py
```

Você verá uma saída como esta:

```
======================================================================
🔧 CONFIGURAÇÃO DO BANCO DE DADOS
======================================================================
📁 Arquivo .env: C:\Users\...\testarconexao\.env
🌐 Host: 10.50.30.166
🔌 Porta: 5432
💾 Banco: alertadb_cor
👤 Usuário: seu_usuario
🔑 Senha: **********
======================================================================

======================================================================
🌧️  API DADOS PLUVIOMÉTRICOS
======================================================================
🌐 Servidor: http://localhost:5000
📊 Dashboard: http://localhost:5000/dashboard
📚 Documentação: http://localhost:5000/api/docs
💚 Health Check: http://localhost:5000/api/health
🔧 Host: 0.0.0.0 | Porta: 5000 | Debug: False
💾 Banco de dados: alertadb_cor @ 10.50.30.166:5432
👤 Usuário: seu_usuario
======================================================================
```

### 2. Acessar o Dashboard

Abra seu navegador e acesse:

**Local:**
```
http://localhost:5000
ou
http://localhost:5000/dashboard
```

**Em produção (servidor):**
```
http://10.50.30.166:5000
ou
http://10.50.30.166:5000/dashboard
```

---

## 🌐 Acessar de Outros Dispositivos na Rede

Se você iniciou a API com `SERVER_HOST=0.0.0.0` (padrão), o dashboard estará acessível de qualquer dispositivo na mesma rede:

```
http://[IP_DO_SERVIDOR]:5000
```

**Exemplo:**
```
http://192.168.1.100:5000
```

---

## 🔧 Configurações Avançadas

### Alterar Porta

No arquivo `.env`:
```env
SERVER_PORT=8080
```

### Alterar Host

No arquivo `.env`:
```env
SERVER_HOST=0.0.0.0  # Permite acesso de qualquer interface
# ou
SERVER_HOST=127.0.0.1  # Apenas localhost
```

### Modo Debug (Desenvolvimento)

No arquivo `.env`:
```env
DEBUG=True
```

---

## 🏭 Produção com Gunicorn

Para produção, use Gunicorn em vez do servidor de desenvolvimento do Flask:

```bash
# Instalar Gunicorn
pip install gunicorn

# Rodar com Gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 scripts.app:app
```

**Com configurações:**
```bash
gunicorn -w 4 -b 0.0.0.0:5000 --timeout 120 --access-logfile - --error-logfile - scripts.app:app
```

---

## 🔄 Manter Rodando em Background (Linux)

### Usando nohup:
```bash
nohup python scripts/servidor166/app.py > logs/api.log 2>&1 &
```

### Usando screen:
```bash
screen -S api
python scripts/servidor166/app.py
# Pressione Ctrl+A depois D para desanexar
# Para reanexar: screen -r api
```

### Usando systemd (serviço):
Crie `/etc/systemd/system/pluviometricos-api.service`:

```ini
[Unit]
Description=API Dados Pluviométricos
After=network.target

[Service]
Type=simple
User=seu_usuario
WorkingDirectory=/caminho/do/projeto
Environment="PATH=/usr/bin:/usr/local/bin"
ExecStart=/usr/bin/python3 /caminho/do/projeto/scripts/servidor166/app.py
Restart=always

[Install]
WantedBy=multi-user.target
```

Depois:
```bash
sudo systemctl enable pluviometricos-api
sudo systemctl start pluviometricos-api
sudo systemctl status pluviometricos-api
```

---

## ✅ Verificar se Está Funcionando

1. **Health Check:**
   ```
   http://localhost:5000/api/health
   ```
   Deve retornar: `{"status": "ok", "banco": "conectado"}`

2. **Dashboard:**
   ```
   http://localhost:5000/dashboard
   ```
   Deve mostrar o dashboard com dados

3. **API Info:**
   ```
   http://localhost:5000/api
   ```
   Deve retornar informações da API

---

## 🐛 Troubleshooting

### Erro: "Port already in use"
```bash
# Encontrar processo usando a porta
lsof -i :5000

# Matar processo (substitua PID pelo número encontrado)
kill -9 [PID]
```

### Erro: "Arquivo .env não encontrado"
Certifique-se de que o arquivo `.env` está na raiz do projeto (mesmo nível que `scripts/`).

### Dashboard não carrega dados
1. Verifique se a API está rodando
2. Verifique se o banco tem dados: `python scripts/servidor166/carregar_pluviometricos_historicos.py`
3. Verifique os logs no console da API
4. Abra o console do navegador (F12) para ver erros JavaScript

### Erro de CORS
O Flask já está configurado com `CORS(app)`, então não deve haver problemas de CORS. Se houver, verifique se a API está rodando corretamente.

---

## 📊 Funcionalidades do Dashboard

- ✅ **Estatísticas em tempo real**: Total de registros, estações, período dos dados
- ✅ **Tabela de últimas leituras**: Dados mais recentes de todas as estações
- ✅ **Atualização automática**: A cada 5 minutos (mesmo intervalo do cron)
- ✅ **Controle de período**: Selecione quantas horas visualizar (1h, 6h, 12h, 24h, 48h, 72h)
- ✅ **Atualização manual**: Botão para atualizar imediatamente
- ✅ **Pausar/Retomar**: Controle da atualização automática
- ✅ **Indicador visual**: Mostra quando os dados foram atualizados pela última vez

---

## 🔗 Links Úteis

- **Dashboard:** `http://localhost:5000/dashboard`
- **API Info:** `http://localhost:5000/api`
- **Documentação:** `http://localhost:5000/api/docs`
- **Health Check:** `http://localhost:5000/api/health`

---

**Última atualização:** 2025

