# 🌧️ Sistema de Sincronização de Dados Pluviométricos e Meteorológicos

Sistema para sincronizar dados pluviométricos e meteorológicos do banco **alertadb** para o banco **alertario_cor**.

---

## 📁 Estrutura do Projeto

```
projeto/
├── scripts/                          # Scripts principais de sincronização
│   ├── carregar_pluviometricos_historicos.py    # Carga inicial completa
│   └── sincronizar_pluviometricos_novos.py      # Sincronização incremental
│
├── setup/                            # Scripts de configuração/setup
│   └── criar_banco_alertario_cor.py             # Cria banco e tabelas
│
├── automacao/                        # Scripts de automação
│   ├── cron_linux.sh                 # Script cron para Linux
│   ├── cron_cloudsql.sh              # Script cron para Cloud SQL
│   ├── configurar_cron_linux.sh      # Helper para configurar cron
│   ├── configurar_cron_cloudsql.sh   # Helper para configurar cron Cloud SQL
│   ├── prefect_flow.py               # Flow Prefect para automação
│   └── prefect_deployment.py         # Deployment Prefect
│
├── docs/                             # Documentação
│   ├── OPCOES_AUTOMACAO.md           # Opções de automação disponíveis
│   ├── CONFIGURAR_CRON.md            # Guia completo de configuração do cron
│   └── GUIA_CRIACAO_USUARIO.md       # Guia para criar usuário no servidor
│
├── logs/                             # Logs de execução (criado automaticamente)
│
├── requirements.txt                   # Dependências Python
├── .env                              # Configurações (criar manualmente)
└── README.md                          # Este arquivo
```

---

## 🚀 Início Rápido

### 1. Configuração Inicial

```bash
# Instalar dependências
pip install -r requirements.txt

# Criar arquivo .env com as credenciais
# (veja exemplo abaixo)
```

### 2. Criar Usuário no Servidor (OBRIGATÓRIO)

⚠️ **IMPORTANTE:** O servidor `10.50.30.166` precisa de um usuário **diferente** do banco de origem.

Veja o guia completo: [`docs/GUIA_CRIACAO_USUARIO.md`](docs/GUIA_CRIACAO_USUARIO.md)

**Resumo rápido:**
1. Crie o usuário no servidor `10.50.30.166` usando o script `setup/criar_usuario_postgresql.sql`
2. Configure `DB_ALERTARIO_COR_USER` e `DB_ALERTARIO_COR_PASSWORD` no arquivo `.env`
3. Teste a conexão: `python setup/testar_conexao.py`

### 3. Criar Banco de Dados no Servidor

Conecte-se ao servidor via SSH e execute:

```bash
# Via SSH
ssh servicedesk@10.50.30.166

# No servidor, execute:
psql -U postgres -f setup/criar_banco_servidor.sql

# Ou use o script shell:
bash setup/criar_banco_servidor.sh
```

**Arquivos disponíveis:**
- `setup/criar_banco_servidor.sql` - Script SQL puro (recomendado)
- `setup/criar_banco_servidor.sh` - Script shell com interação

### 4. Carregar Dados Históricos (OBRIGATÓRIO)

```bash
python scripts/carregar_pluviometricos_historicos.py
```

⚠️ **IMPORTANTE:** Execute este script PRIMEIRO antes de configurar o cron. Ele faz a carga inicial completa de todos os dados históricos.

### 5. Configurar Sincronização Automática via Cron

Após a carga inicial, configure o cron para manter os dados atualizados automaticamente:

#### Linux/Unix (Recomendado)

```bash
cd automacao
chmod +x configurar_cron_linux.sh cron_linux.sh
./configurar_cron_linux.sh
```

Ou manualmente:
```bash
chmod +x automacao/cron_linux.sh
crontab -e
# Adicione: */5 * * * * /caminho/completo/para/automacao/cron_linux.sh
```

📚 **Documentação completa:** Veja [`docs/CONFIGURAR_CRON.md`](docs/CONFIGURAR_CRON.md) ou [`automacao/GUIA_RAPIDO_CRON.md`](automacao/GUIA_RAPIDO_CRON.md)

### 6. Testar Sincronização Manual (Opcional)

```bash
# Modo único (para testar antes de configurar cron)
python scripts/sincronizar_pluviometricos_novos.py --once

# Modo contínuo (não recomendado para produção - use cron)
python scripts/sincronizar_pluviometricos_novos.py
```

---

## ⚙️ Configuração (.env)

Crie um arquivo `.env` na raiz do projeto:

```env
# Banco de origem (alertadb)
DB_ORIGEM_HOST=seu_host
DB_ORIGEM_NAME=alertadb
DB_ORIGEM_USER=seu_usuario
DB_ORIGEM_PASSWORD=sua_senha
DB_ORIGEM_SSLMODE=disable
DB_ORIGEM_PORT=5432

# Banco de destino (alertario_cor)
DB_DESTINO_HOST=seu_host
DB_DESTINO_NAME=alertario_cor
DB_DESTINO_USER=seu_usuario
DB_DESTINO_PASSWORD=sua_senha
DB_DESTINO_PORT=5432

# Configurações opcionais
INTERVALO_VERIFICACAO=300  # Segundos (padrão: 300 = 5 minutos)
DB_ALERTARIO_COR_NAME=alertario_cor  # Nome do banco a criar

# ========================================
# API REST - Dados Pluviométricos
# ========================================
# ⚠️ A API (scripts/app.py) usa automaticamente as mesmas variáveis DB_DESTINO_*
#    configuradas acima. Não é necessário configurar variáveis separadas.
#    As variáveis abaixo são apenas para retrocompatibilidade:
# DB_HOST=10.50.30.166
# DB_PORT=5432
# DB_NAME=alertario_cor
# DB_USER=seu_usuario
# DB_PASSWORD=sua_senha_aqui

# API Key (opcional - se não configurada, a API será acessível sem autenticação)
API_KEY=sua_chave_secreta_aqui
```

---

## 📋 Scripts Disponíveis

### Scripts Principais (`scripts/`)

#### `carregar_pluviometricos_historicos.py`
- **Função:** Carrega TODOS os dados históricos do banco origem
- **Quando usar:** Primeira vez ou quando a tabela está vazia
- **Uso:** `python scripts/carregar_pluviometricos_historicos.py`

#### `sincronizar_pluviometricos_novos.py`
- **Função:** Sincroniza APENAS novos dados desde a última sincronização
- **Quando usar:** Após carga inicial, para manter dados atualizados
- **Uso:** 
  - Contínuo: `python scripts/sincronizar_pluviometricos_novos.py`
  - Único: `python scripts/sincronizar_pluviometricos_novos.py --once`

#### `app.py` - API REST
- **Função:** API REST para consultar dados pluviométricos sincronizados
- **Quando usar:** Para disponibilizar dados via HTTP/JSON para aplicações externas
- **Uso:** `python scripts/app.py` ou `gunicorn -w 4 -b 0.0.0.0:5000 scripts.app:app`
- **Endpoints:** `/api/pluviometricos`, `/api/estacoes`, `/api/stats`, `/api/health`, etc.
- **Documentação:** `http://localhost:5000/api/docs`
- **⚠️ IMPORTANTE:** A API usa os dados do banco `alertadb_cor` (mesmo banco sincronizado via cron)
- **🔧 Verificar configuração:** `python scripts/verificar_config_api.py` (diagnóstico de conexão)

### Scripts de Setup (`setup/`)

#### `criar_banco_alertario_cor.py`
- **Função:** Cria o banco de dados `alertario_cor` e as tabelas necessárias
- **Quando usar:** Primeira vez configurando o sistema
- **Uso:** `python setup/criar_banco_alertario_cor.py`

### Scripts de Automação (`automacao/`)

#### Linux
- `cron_linux.sh` - Script para cron
- `configurar_cron_linux.sh` - Helper para configurar cron automaticamente

#### Cloud SQL
- `cron_cloudsql.sh` - Script para cron Cloud SQL
- `configurar_cron_cloudsql.sh` - Helper para configurar cron Cloud SQL

#### Prefect
- `prefect_flow.py` - Flow Prefect para orquestração
- `prefect_deployment.py` - Deployment Prefect

---

## 🔄 Fluxo de Trabalho Recomendado

```
1. Setup
   └── python setup/criar_banco_alertario_cor.py

2. Carga Inicial (OBRIGATÓRIO)
   └── python scripts/carregar_pluviometricos_historicos.py
       ⚠️ Execute PRIMEIRO antes de configurar o cron!

3. Configurar Automação (escolha uma opção)
   ├── Cron Linux (Recomendado): 
   │   └── cd automacao && ./configurar_cron_linux.sh
   └── Prefect: 
       └── automacao/prefect_flow.py

4. Monitoramento
   └── Verificar logs em logs/ e última sincronização no banco

5. API REST (Opcional)
   └── python scripts/app.py
       A API usa automaticamente os dados sincronizados do banco alertadb_cor
```

---

## 📊 Estrutura do Banco de Dados

### Banco: `alertario_cor`

#### Tabela: `pluviometricos`
- **Chave primária:** (dia, estacao_id)
- **Campos:** dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id

#### Tabela: `meteorologicos`
- **Chave primária:** (dia, estacao_id)
- **Campos:** dia, estacao_id, temperatura, temperatura_minima, temperatura_maxima, umidade, pressao, velocidade_vento, direcao_vento, radiacao_solar, estacao

---

## 🔒 Proteções Implementadas

✅ **ON CONFLICT DO NOTHING** - Previne duplicatas  
✅ **Chave primária composta** - Garante unicidade  
✅ **Tratamento de horário de verão** - Ajuste automático (1997-2019)  
✅ **Validações** - Verifica tabelas e conexões antes de executar  
✅ **Processamento em lotes** - Otimiza uso de memória  

---

## 📚 Documentação Adicional

- [`docs/GUIA_USO_API.md`](docs/GUIA_USO_API.md) - **Guia completo de uso da API REST** (como consultar dados via API)
- [`docs/CONFIGURAR_CRON.md`](docs/CONFIGURAR_CRON.md) - Guia completo para configurar cron após carga inicial
- [`automacao/GUIA_RAPIDO_CRON.md`](automacao/GUIA_RAPIDO_CRON.md) - Guia rápido de configuração do cron
- [`docs/OPCOES_AUTOMACAO.md`](docs/OPCOES_AUTOMACAO.md) - Comparação de opções de automação

---

## ⚠️ Requisitos

- Python 3.7+
- PostgreSQL
- Bibliotecas Python (ver `requirements.txt`)

---

## 🆘 Suporte

Para problemas ou dúvidas, verifique:
1. Arquivo `.env` configurado corretamente
2. Conexões com os bancos de dados
3. Permissões do usuário do banco
4. Logs em `logs/` (se existirem)

