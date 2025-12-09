# 🌐 Guia de Uso da API REST - Dados Pluviométricos

Este guia mostra como consultar os dados do banco `alertadb_cor` através da API REST.

---

## 🚀 Iniciar a API

Primeiro, certifique-se de que a API está rodando:

```bash
# Desenvolvimento
python scripts/servidor166/app.py

# Produção (com gunicorn)
gunicorn -w 4 -b 0.0.0.0:5000 scripts.app:app
```

A API estará disponível em: `http://localhost:5000` (ou `http://10.50.30.166:5000` em produção)

---

## 📋 Endpoints Disponíveis

### 1. **Página Inicial** - Lista todos os endpoints
```
GET http://localhost:5000/
```

### 2. **Documentação** - Documentação completa da API
```
GET http://localhost:5000/api/docs
```

### 3. **Health Check** - Status da API e banco
```
GET http://localhost:5000/api/health
```

### 4. **Listar Estações** - Todas as estações disponíveis
```
GET http://localhost:5000/api/estacoes
```

### 5. **Dados Pluviométricos** - Buscar dados com filtros
```
GET http://localhost:5000/api/pluviometricos
```

### 6. **Últimos Registros** - Dados recentes
```
GET http://localhost:5000/api/ultimos
```

### 7. **Estatísticas** - Estatísticas gerais
```
GET http://localhost:5000/api/stats
```

### 8. **Dados por Período** - Agregação por período
```
GET http://localhost:5000/api/periodo
```

---

## 🔧 Exemplos Práticos

### **1. Verificar se a API está funcionando**

#### No navegador:
```
http://localhost:5000/api/health
```

#### Com curl:
```bash
curl http://localhost:5000/api/health
```

#### Com Python:
```python
import requests

response = requests.get('http://localhost:5000/api/health')
print(response.json())
```

**Resposta esperada:**
```json
{
  "status": "ok",
  "banco": "conectado",
  "timestamp": "2024-01-15T10:30:00"
}
```

---

### **2. Listar todas as estações**

#### No navegador:
```
http://localhost:5000/api/estacoes
```

#### Com curl:
```bash
curl http://localhost:5000/api/estacoes
```

#### Com Python:
```python
import requests

response = requests.get('http://localhost:5000/api/estacoes')
data = response.json()
print(f"Total de estações: {data['total_estacoes']}")
for estacao in data['estacoes']:
    print(f"ID: {estacao['estacao_id']} - {estacao['estacao']}")
```

**Resposta esperada:**
```json
{
  "total_estacoes": 25,
  "estacoes": [
    {
      "estacao_id": 1,
      "estacao": "Estação Centro",
      "total_registros": 15000,
      "primeira_leitura": "1997-01-01T00:00:00",
      "ultima_leitura": "2024-01-15T10:00:00"
    },
    ...
  ]
}
```

---

### **3. Buscar dados pluviométricos com filtros**

#### Buscar dados de uma estação específica:
```bash
curl "http://localhost:5000/api/pluviometricos?estacao_id=1&limit=10"
```

#### Buscar dados por período:
```bash
curl "http://localhost:5000/api/pluviometricos?data_inicio=2024-01-01&data_fim=2024-01-31&limit=100"
```

#### Buscar por nome da estação:
```bash
curl "http://localhost:5000/api/pluviometricos?estacao_nome=Centro&limit=50"
```

#### Com Python:
```python
import requests

# Buscar últimos 100 registros da estação ID 1
params = {
    'estacao_id': 1,
    'limit': 100
}
response = requests.get('http://localhost:5000/api/pluviometricos', params=params)
data = response.json()

print(f"Total de registros: {data['total']}")
print(f"Resultados retornados: {data['resultados']}")

for registro in data['dados']:
    print(f"{registro['dia']} - Estação: {registro['estacao']} - h24: {registro['h24']}mm")
```

**Parâmetros disponíveis:**
- `data_inicio`: Data inicial (formato: YYYY-MM-DD)
- `data_fim`: Data final (formato: YYYY-MM-DD)
- `estacao_id`: ID da estação (número)
- `estacao_nome`: Nome da estação (busca parcial, case-insensitive)
- `limit`: Limite de resultados (padrão: 1000, máximo: 10000)
- `offset`: Deslocamento para paginação (padrão: 0)

---

### **4. Últimos registros (últimas 24 horas)**

#### No navegador:
```
http://localhost:5000/api/ultimos
```

#### Últimas 48 horas:
```bash
curl "http://localhost:5000/api/ultimos?horas=48"
```

#### Com Python:
```python
import requests

# Últimas 12 horas
response = requests.get('http://localhost:5000/api/ultimos', params={'horas': 12})
data = response.json()

print(f"Período: {data['periodo']}")
print(f"Total de registros: {data['total_registros']}")

for registro in data['dados'][:5]:  # Mostrar apenas os 5 primeiros
    print(f"{registro['dia']} - {registro['estacao']}: {registro['h24']}mm")
```

---

### **5. Estatísticas gerais**

#### No navegador:
```
http://localhost:5000/api/stats
```

#### Com curl:
```bash
curl http://localhost:5000/api/stats
```

#### Com Python:
```python
import requests

response = requests.get('http://localhost:5000/api/stats')
data = response.json()

stats = data['estatisticas_gerais']
print(f"Total de registros: {stats['total_registros']:,}")
print(f"Total de estações: {stats['total_estacoes']}")
print(f"Data mínima: {stats['data_minima']}")
print(f"Data máxima: {stats['data_maxima']}")
print(f"Média geral h24: {stats['media_geral_h24']:.2f}mm")
print(f"Máximo geral h24: {stats['max_geral_h24']}mm")

print("\nTop 5 estações:")
for estacao in data['top_5_estacoes']:
    print(f"  {estacao['estacao']}: {estacao['total']:,} registros")
```

---

### **6. Dados agregados por período**

O endpoint `/api/periodo` agora é **muito mais flexível**! Você pode:

#### **Opção 1: Sem parâmetros (usa últimos 30 dias automaticamente)**
```bash
curl "http://localhost:5000/api/periodo"
```

#### **Opção 2: Especificar número de dias**
```bash
# Últimos 7 dias
curl "http://localhost:5000/api/periodo?dias=7"

# Últimos 90 dias
curl "http://localhost:5000/api/periodo?dias=90"
```

#### **Opção 3: Especificar período completo**
```bash
# Agregação diária
curl "http://localhost:5000/api/periodo?data_inicio=2024-01-01&data_fim=2024-01-31&agregacao=dia"

# Agregação mensal
curl "http://localhost:5000/api/periodo?data_inicio=2024-01-01&data_fim=2024-12-31&agregacao=mes"

# Agregação semanal com filtro de estação
curl "http://localhost:5000/api/periodo?data_inicio=2024-01-01&data_fim=2024-12-31&agregacao=semana&estacao_id=1"
```

#### **Com Python:**
```python
import requests

# Exemplo 1: Últimos 30 dias (padrão)
response = requests.get('http://localhost:5000/api/periodo')
data = response.json()
print(f"Período usado: {data['periodo_usado']}")

# Exemplo 2: Últimos 7 dias
response = requests.get('http://localhost:5000/api/periodo', params={'dias': 7})
data = response.json()

# Exemplo 3: Período específico
params = {
    'data_inicio': '2024-01-01',
    'data_fim': '2024-12-31',
    'agregacao': 'mes',
    'estacao_id': 1  # Opcional
}
response = requests.get('http://localhost:5000/api/periodo', params=params)
data = response.json()

print(f"Agregação: {data['agregacao']}")
print(f"Período: {data['periodo_usado']}")
print(f"Total de registros: {data['total_registros']}")

for item in data['dados']:
    print(f"{item['periodo']} - {item['estacao']}: Média h24: {item['media_h24']:.2f}mm")
```

**Parâmetros disponíveis:**
- `dias` (opcional): Número de dias para buscar (ex: `dias=7` para últimos 7 dias)
- `data_inicio` (opcional): Data inicial no formato YYYY-MM-DD
- `data_fim` (opcional): Data final no formato YYYY-MM-DD
- `agregacao` (opcional): Tipo de agregação - `dia`, `semana`, `mes` (padrão: `dia`)
- `estacao_id` (opcional): Filtrar por estação específica

**Nota:** Se você não fornecer `data_inicio` e `data_fim`, o endpoint usa automaticamente os últimos 30 dias. Se fornecer apenas `dias`, calcula o período a partir da data mais recente no banco.

---

### **7. Detalhes de uma estação específica**

#### No navegador:
```
http://localhost:5000/api/estacoes/1
```

#### Com curl:
```bash
curl http://localhost:5000/api/estacoes/1
```

#### Com Python:
```python
import requests

estacao_id = 1
response = requests.get(f'http://localhost:5000/api/estacoes/{estacao_id}')
data = response.json()

info = data['informacoes']
print(f"Estacao: {info['estacao']}")
print(f"Total de registros: {info['total_registros']:,}")
print(f"Primeira leitura: {info['primeira_leitura']}")
print(f"Última leitura: {info['ultima_leitura']}")
print(f"Média h24: {info['media_h24']:.2f}mm")
print(f"Máximo h24: {info['max_h24']}mm")

print("\nÚltimas 10 leituras:")
for leitura in data['ultimas_leituras']:
    print(f"  {leitura['dia']}: h24={leitura['h24']}mm")
```

---

## 🔐 Autenticação (Opcional)

Se você configurou uma `API_KEY` no arquivo `.env`, será necessário enviá-la no header:

#### Com curl:
```bash
curl -H "X-API-Key: sua_chave_aqui" http://localhost:5000/api/pluviometricos
```

#### Com Python:
```python
import requests

headers = {
    'X-API-Key': 'sua_chave_aqui'
}
response = requests.get('http://localhost:5000/api/pluviometricos', headers=headers)
```

#### Com JavaScript (fetch):
```javascript
fetch('http://localhost:5000/api/pluviometricos', {
    headers: {
        'X-API-Key': 'sua_chave_aqui'
    }
})
.then(response => response.json())
.then(data => console.log(data));
```

**Nota:** Se `API_KEY` não estiver configurada no `.env`, a API será acessível sem autenticação.

---

## 📊 Exemplo Completo - Dashboard Simples

```python
import requests
from datetime import datetime, timedelta

BASE_URL = 'http://localhost:5000/api'

def dashboard():
    """Exibe um dashboard simples com informações principais"""
    
    # 1. Health check
    health = requests.get(f'{BASE_URL}/health').json()
    print(f"Status: {health['status']}")
    print(f"Banco: {health['banco']}")
    print()
    
    # 2. Estatísticas gerais
    stats = requests.get(f'{BASE_URL}/stats').json()
    stats_gerais = stats['estatisticas_gerais']
    print("=" * 60)
    print("ESTATÍSTICAS GERAIS")
    print("=" * 60)
    print(f"Total de registros: {stats_gerais['total_registros']:,}")
    print(f"Total de estações: {stats_gerais['total_estacoes']}")
    print(f"Período: {stats_gerais['data_minima']} até {stats_gerais['data_maxima']}")
    print(f"Média geral h24: {stats_gerais['media_geral_h24']:.2f}mm")
    print()
    
    # 3. Listar estações
    estacoes = requests.get(f'{BASE_URL}/estacoes').json()
    print("=" * 60)
    print(f"ESTAÇÕES DISPONÍVEIS ({estacoes['total_estacoes']})")
    print("=" * 60)
    for estacao in estacoes['estacoes'][:10]:  # Primeiras 10
        print(f"ID {estacao['estacao_id']:2d}: {estacao['estacao']:30s} "
              f"({estacao['total_registros']:,} registros)")
    print()
    
    # 4. Últimos registros (últimas 6 horas)
    ultimos = requests.get(f'{BASE_URL}/ultimos', params={'horas': 6}).json()
    print("=" * 60)
    print(f"ÚLTIMOS REGISTROS ({ultimos['periodo']})")
    print("=" * 60)
    for registro in ultimos['dados'][:10]:  # Primeiros 10
        print(f"{registro['dia']} | {registro['estacao']:30s} | "
              f"h24: {registro['h24'] or 0:.2f}mm")

if __name__ == '__main__':
    dashboard()
```

---

## 🌐 Testando no Navegador

Você pode testar diretamente no navegador acessando:

1. **Health Check:**
   ```
   http://localhost:5000/api/health
   ```

2. **Documentação:**
   ```
   http://localhost:5000/api/docs
   ```

3. **Listar Estações:**
   ```
   http://localhost:5000/api/estacoes
   ```

4. **Últimos Registros:**
   ```
   http://localhost:5000/api/ultimos?horas=24
   ```

5. **Estatísticas:**
   ```
   http://localhost:5000/api/stats
   ```

6. **Dados com Filtros:**
   ```
   http://localhost:5000/api/pluviometricos?estacao_id=1&limit=10
   ```

---

## 🛠️ Ferramentas Úteis

### **Postman / Insomnia**
Importe os endpoints e teste facilmente com interface gráfica.

### **HTTPie** (alternativa ao curl)
```bash
# Instalar: pip install httpie
http GET http://localhost:5000/api/estacoes
http GET http://localhost:5000/api/pluviometricos estacao_id==1 limit==10
```

### **jq** (para formatar JSON no terminal)
```bash
curl http://localhost:5000/api/stats | jq
```

---

## ⚠️ Troubleshooting

### **Erro: "no password supplied" / "banco: desconectado"**

Este é o erro mais comum! Significa que a senha do banco não está configurada.

**Solução:**

1. **Verifique a configuração:**
   ```bash
   python scripts/verificar_config_api.py
   ```

2. **Configure a senha no arquivo `.env`:**
   ```env
   # Adicione uma das seguintes variáveis:
   DB_DESTINO_PASSWORD=sua_senha_aqui
   # OU (retrocompatibilidade):
   DB_PASSWORD=sua_senha_aqui
   ```

3. **Verifique se o arquivo `.env` está na raiz do projeto:**
   ```bash
   ls -la .env  # Deve existir na raiz do projeto
   ```

4. **Reinicie a API após alterar o `.env`:**
   ```bash
   # Pare a API (Ctrl+C) e inicie novamente
   python scripts/servidor166/app.py
   ```

### **Erro de conexão**
- Verifique se a API está rodando: `python scripts/servidor166/app.py`
- Verifique se a porta está correta (padrão: 5000)
- Verifique se o firewall permite conexões
- Execute o script de verificação: `python scripts/verificar_config_api.py`

### **Erro 401 (Não autorizado)**
- Verifique se configurou `API_KEY` no `.env`
- Se configurou, envie no header: `X-API-Key: sua_chave`
- Se não configurou `API_KEY`, a API deve funcionar sem autenticação

### **Erro 500 (Erro interno)**
- Verifique os logs da API (mensagens no terminal)
- Execute o script de verificação: `python scripts/verificar_config_api.py`
- Verifique se o banco `alertadb_cor` existe e está acessível
- Verifique as credenciais no arquivo `.env`
- Teste a conexão manualmente:
  ```bash
  psql -h 10.50.30.166 -U seu_usuario -d alertadb_cor
  ```

### **Nenhum dado retornado**
- Verifique se os dados foram sincronizados: `python scripts/servidor166/carregar_pluviometricos_historicos.py`
- Verifique se o cron está rodando: `crontab -l`
- Verifique os logs de sincronização em `logs/`
- Verifique se a tabela `pluviometricos` tem dados:
  ```sql
  SELECT COUNT(*) FROM pluviometricos;
  ```

---

## 📚 Mais Informações

- **Documentação da API:** `http://localhost:5000/api/docs`
- **Código fonte:** `scripts/servidor166/app.py`
- **Configuração:** `CONFIGURACAO_EXEMPLO.md`

---

**Última atualização:** 2025