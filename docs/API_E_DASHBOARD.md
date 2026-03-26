# 🌐 API REST e Dashboard — Guia Completo

A API Flask (`scripts/servidor166/app.py`) serve os dados pluviométricos do banco `alertadb_cor`
e disponibiliza um dashboard web integrado.

---

## 🚀 Iniciar a API

```bash
# Desenvolvimento
python scripts/servidor166/app.py
```

Saída esperada:

```
======================================================
🌧️  API DADOS PLUVIOMÉTRICOS
======================================================
🌐 Servidor: http://localhost:5000
📊 Dashboard: http://localhost:5000/dashboard
📚 Documentação: http://localhost:5000/api/docs
💚 Health Check: http://localhost:5000/api/health
🔧 Host: 0.0.0.0 | Porta: 5000 | Debug: False
💾 Banco: alertadb_cor @ 10.50.30.166:5432
======================================================
```

### Pré-requisitos

1. Python 3.7+
2. `pip install -r requirements.txt`
3. `.env` configurado com credenciais do banco (`alertadb_cor`)
4. Banco populado — execute `carregar_pluviometricos_historicos.py` primeiro

---

## 📊 Dashboard

Acesse no navegador:

**Local:**
```
http://localhost:5000/dashboard
```

**Servidor 166:**
```
http://10.50.30.166:5000/dashboard
```

**Funcionalidades:**
- Estatísticas em tempo real (total de registros, estações, período)
- Tabela de últimas leituras por estação
- Atualização automática a cada 5 minutos
- Controle de período: 1h, 6h, 12h, 24h, 48h, 72h
- Botão de atualização manual e controle de pausa

---

## 📋 Endpoints

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/` | Lista todos os endpoints |
| GET | `/api/docs` | Documentação da API |
| GET | `/api/health` | Status da API e banco |
| GET | `/api/estacoes` | Todas as estações |
| GET | `/api/estacoes/{id}` | Detalhes de uma estação |
| GET | `/api/pluviometricos` | Dados com filtros |
| GET | `/api/ultimos` | Dados recentes (últimas 24h) |
| GET | `/api/stats` | Estatísticas gerais |
| GET | `/api/periodo` | Dados agregados por período |

---

## 🔧 Exemplos Práticos

### Health check

```bash
curl http://localhost:5000/api/health
```

```json
{"status": "ok", "banco": "conectado", "timestamp": "2024-01-15T10:30:00"}
```

### Listar estações

```bash
curl http://localhost:5000/api/estacoes
```

```python
import requests
r = requests.get('http://localhost:5000/api/estacoes')
for est in r.json()['estacoes']:
    print(f"ID {est['estacao_id']}: {est['estacao']} ({est['total_registros']:,} registros)")
```

### Dados pluviométricos com filtros

```bash
# Por estação
curl "http://localhost:5000/api/pluviometricos?estacao_id=1&limit=10"

# Por período
curl "http://localhost:5000/api/pluviometricos?data_inicio=2024-01-01&data_fim=2024-01-31&limit=100"

# Por nome da estação (busca parcial)
curl "http://localhost:5000/api/pluviometricos?estacao_nome=Centro&limit=50"
```

**Parâmetros disponíveis:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `data_inicio` | YYYY-MM-DD | Data inicial |
| `data_fim` | YYYY-MM-DD | Data final |
| `estacao_id` | int | ID da estação |
| `estacao_nome` | string | Busca parcial, case-insensitive |
| `limit` | int | Máx. resultados (padrão: 1000, máx: 10000) |
| `offset` | int | Deslocamento para paginação |

### Últimos registros

```bash
# Últimas 24h (padrão)
curl http://localhost:5000/api/ultimos

# Últimas 48h
curl "http://localhost:5000/api/ultimos?horas=48"
```

### Estatísticas gerais

```bash
curl http://localhost:5000/api/stats
```

```python
r = requests.get('http://localhost:5000/api/stats')
s = r.json()['estatisticas_gerais']
print(f"Total: {s['total_registros']:,} | Estações: {s['total_estacoes']} | Média h24: {s['media_geral_h24']:.2f}mm")
```

### Dados agregados por período

```bash
# Últimos 30 dias (padrão)
curl http://localhost:5000/api/periodo

# Últimos 7 dias
curl "http://localhost:5000/api/periodo?dias=7"

# Período específico com agregação mensal
curl "http://localhost:5000/api/periodo?data_inicio=2024-01-01&data_fim=2024-12-31&agregacao=mes"

# Período com filtro de estação
curl "http://localhost:5000/api/periodo?data_inicio=2024-01-01&data_fim=2024-12-31&agregacao=semana&estacao_id=1"
```

**Parâmetros de `/api/periodo`:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `dias` | int | Número de dias a partir do mais recente |
| `data_inicio` | YYYY-MM-DD | Data inicial |
| `data_fim` | YYYY-MM-DD | Data final |
| `agregacao` | string | `dia`, `semana` ou `mes` (padrão: `dia`) |
| `estacao_id` | int | Filtrar por estação |

---

## 🔐 Autenticação (opcional)

Se `API_KEY` estiver configurada no `.env`, envie o header em todas as requisições:

```bash
curl -H "X-API-Key: sua_chave_aqui" http://localhost:5000/api/pluviometricos
```

```python
headers = {'X-API-Key': 'sua_chave_aqui'}
requests.get('http://localhost:5000/api/pluviometricos', headers=headers)
```

```javascript
fetch('http://localhost:5000/api/pluviometricos', {
    headers: {'X-API-Key': 'sua_chave_aqui'}
})
```

Se `API_KEY` não estiver configurada, a API é acessível sem autenticação.

---

## ⚙️ Configurações no `.env`

```env
SERVER_HOST=0.0.0.0     # Interface (0.0.0.0 = todas)
SERVER_PORT=5000         # Porta
DEBUG=False              # True apenas em desenvolvimento
API_KEY=                 # Deixe vazio para acesso livre
```

### Acesso de outros dispositivos na rede

Com `SERVER_HOST=0.0.0.0`, qualquer dispositivo na rede pode acessar:
```
http://[IP_DO_SERVIDOR]:5000
```

---

## 🏭 Produção com Gunicorn

```bash
pip install gunicorn

# Iniciar (execute a partir da raiz do projeto)
gunicorn -w 4 -b 0.0.0.0:5000 scripts.servidor166.app:app

# Com logs e timeout
gunicorn -w 4 -b 0.0.0.0:5000 --timeout 120 \
    --access-logfile logs/api_access.log \
    --error-logfile logs/api_error.log \
    scripts.servidor166.app:app
```

### Manter rodando em background (Linux)

**nohup:**
```bash
nohup python scripts/servidor166/app.py > logs/api.log 2>&1 &
```

**screen:**
```bash
screen -S api
python scripts/servidor166/app.py
# Ctrl+A, D para desanexar | screen -r api para reanexar
```

**systemd** — crie `/etc/systemd/system/pluviometricos-api.service`:
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

```bash
sudo systemctl enable pluviometricos-api
sudo systemctl start pluviometricos-api
sudo systemctl status pluviometricos-api
```

---

## 🛠️ Troubleshooting

### "no password supplied" / banco desconectado

```env
# Verifique o .env na raiz do projeto:
DB_DESTINO_PASSWORD=sua_senha_aqui
```

Reinicie a API após alterar o `.env`.

### Porta em uso

```bash
# Linux: encontrar e matar processo na porta
lsof -i :5000
kill -9 [PID]
```

### Dashboard não carrega dados

1. Verifique se a API está rodando (`/api/health`)
2. Verifique se o banco tem dados (execute `carregar_pluviometricos_historicos.py`)
3. Abra o console do navegador (F12) para erros JavaScript

### Erro de CORS

O Flask já está configurado com `CORS(app)`. Se ainda ocorrer, verifique se a API está respondendo corretamente.

### Erro 401 (não autorizado)

Verifique se `API_KEY` está configurada no `.env`. Se sim, envie no header `X-API-Key`.

### Nenhum dado retornado

```bash
# Verificar sincronização
crontab -l                     # cron ativo?
ls -lt logs/                   # logs recentes?

# Verificar tabela no banco
psql -h 10.50.30.166 -U postgres -d alertadb_cor \
    -c "SELECT COUNT(*) FROM pluviometricos;"
```

---

## 🔗 Links rápidos (com API rodando)

| URL | Descrição |
|-----|-----------|
| `http://localhost:5000/dashboard` | Dashboard visual |
| `http://localhost:5000/api/health` | Health check |
| `http://localhost:5000/api/docs` | Documentação inline |
| `http://localhost:5000/api/stats` | Estatísticas |
| `http://localhost:5000/api/estacoes` | Lista de estações |
| `http://localhost:5000/api/ultimos?horas=24` | Últimas 24h |
