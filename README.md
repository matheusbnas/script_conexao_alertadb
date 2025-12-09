# 🌧️ Sistema de Sincronização de Dados Pluviométricos

Sincroniza dados pluviométricos do banco **alertadb** para **alertadb_cor** (servidor 166) e Cloud SQL GCP.

---

## 🚀 Início Rápido

### 1. Instalação

```bash
pip install -r requirements.txt
```

### 2. Configuração

Crie o arquivo `.env` com as credenciais (veja `docs/CONFIGURACAO_EXEMPLO.md`):

```env
# Banco origem (alertadb)
DB_ORIGEM_HOST=...
DB_ORIGEM_NAME=alertadb
DB_ORIGEM_USER=...
DB_ORIGEM_PASSWORD=...

# Banco destino (alertadb_cor - servidor 166)
DB_DESTINO_HOST=10.50.30.166
DB_DESTINO_NAME=alertadb_cor
DB_DESTINO_USER=...
DB_DESTINO_PASSWORD=...

# Cloud SQL (opcional)
DB_CLOUDSQL_HOST=...
DB_CLOUDSQL_NAME=...
DB_CLOUDSQL_USER=...
DB_CLOUDSQL_PASSWORD=...
```

### 3. Carga Inicial

```bash
# Servidor 166 (obrigatório)
python scripts/servidor166/carregar_pluviometricos_historicos.py

# Cloud SQL (opcional)
python scripts/cloudsql/carregar_para_cloudsql_inicial.py
```

### 4. Automação

```bash
cd automacao
chmod +x configurar_cron.sh cron.sh

# Sincronização normal (servidor 166)
./configurar_cron.sh normal

# Sincronização Cloud SQL
./configurar_cron.sh cloudsql
```

---

## 📁 Estrutura

```
scripts/
├── servidor166/          # Scripts para máquina virtual
│   ├── carregar_pluviometricos_historicos.py
│   ├── sincronizar_pluviometricos_novos.py
│   ├── app.py            # API REST
│   └── ...
│
└── cloudsql/            # Scripts para Cloud SQL
    ├── carregar_para_cloudsql_inicial.py
    └── sincronizar_para_cloudsql.py

automacao/
├── cron.sh               # Execução automática
└── configurar_cron.sh    # Configuração do cron

docs/                     # Documentação completa
```

---

## 📚 Documentação

- [Scripts](scripts/README.md) - Documentação dos scripts
- [Automação](automacao/README.md) - Configuração de cron
- [Configurar Cron](docs/CONFIGURAR_CRON.md) - Guia completo
- [Cloud SQL](docs/GUIA_RAPIDO_CLOUD_SQL.md) - Guia rápido Cloud SQL
- [API REST](docs/GUIA_USO_API.md) - Documentação da API

---

## 🔧 Scripts Principais

### Servidor 166

- **carregar_pluviometricos_historicos.py** - Carga inicial completa
- **sincronizar_pluviometricos_novos.py** - Sincronização incremental
- **app.py** - API REST Flask
- **validar_dados_pluviometricos.py** - Validação de dados
- **exportar_pluviometricos_parquet.py** - Exportação para Parquet

### Cloud SQL

- **carregar_para_cloudsql_inicial.py** - Carga inicial Cloud SQL
- **sincronizar_para_cloudsql.py** - Sincronização incremental Cloud SQL

---

## ⚙️ Variáveis de Ambiente

### Banco Origem (alertadb)
- `DB_ORIGEM_HOST` - Host do banco origem
- `DB_ORIGEM_PORT` - Porta (padrão: 5432)
- `DB_ORIGEM_NAME` - Nome do banco (alertadb)
- `DB_ORIGEM_USER` - Usuário
- `DB_ORIGEM_PASSWORD` - Senha
- `DB_ORIGEM_SSLMODE` - Modo SSL (disable/require)

### Banco Destino (alertadb_cor - servidor 166)
- `DB_DESTINO_HOST` - Host (10.50.30.166)
- `DB_DESTINO_PORT` - Porta (padrão: 5432)
- `DB_DESTINO_NAME` - Nome do banco (alertadb_cor)
- `DB_DESTINO_USER` - Usuário
- `DB_DESTINO_PASSWORD` - Senha

### Cloud SQL (opcional)
- `DB_CLOUDSQL_HOST` - Host Cloud SQL
- `DB_CLOUDSQL_PORT` - Porta (padrão: 5432)
- `DB_CLOUDSQL_NAME` - Nome do banco
- `DB_CLOUDSQL_USER` - Usuário
- `DB_CLOUDSQL_PASSWORD` - Senha

---

## 🆘 Suporte

Para problemas ou dúvidas, consulte:
- [Documentação Completa](docs/)
- [Estrutura do Projeto](ESTRUTURA_PROJETO.md)
