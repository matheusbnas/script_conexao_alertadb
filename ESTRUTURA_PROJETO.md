# 📁 Estrutura do Projeto

## Organização de Arquivos

```
projeto/
│
├── 📄 README.md                          # Documentação principal
├── 📄 requirements.txt                    # Dependências Python
├── 📄 .env                                # Configurações (criar manualmente)
│
├── 📂 scripts/                            # Scripts principais de sincronização
│   ├── carregar_pluviometricos_historicos.py
│   │   └── Carrega TODOS os dados históricos (primeira vez)
│   │
│   └── sincronizar_pluviometricos_novos.py
│       └── Sincroniza APENAS novos dados (após carga inicial)
│
├── 📂 setup/                              # Scripts de configuração/setup
│   ├── criar_usuario_postgresql.sql       # Cria usuário no PostgreSQL
│   ├── criar_banco_servidor.sql           # Cria banco via SQL (servidor)
│   ├── criar_banco_servidor.sh            # Cria banco via shell (servidor)
│   └── testar_conexao.py                  # Testa conexão com servidor
│
├── 📂 automacao/                          # Scripts de automação
│   ├── cron_linux.sh                      # Script cron para Linux
│   ├── cron_windows.bat                   # Script cron para Windows (batch)
│   ├── cron_windows.ps1                    # Script cron para Windows (PowerShell)
│   ├── configurar_cron_linux.sh            # Helper para configurar cron
│   ├── prefect_flow.py                     # Flow Prefect
│   └── prefect_deployment.py               # Deployment Prefect
│
├── 📂 docs/                               # Documentação
│   └── OPCOES_AUTOMACAO.md                 # Opções de automação
│
└── 📂 logs/                               # Logs (criado automaticamente)
    └── sincronizacao_YYYYMMDD_HHMMSS.log
```

---

## 📋 Descrição das Pastas

### `scripts/`
Scripts principais que fazem a sincronização de dados:
- **carregar_pluviometricos_historicos.py** - Carga inicial completa
- **sincronizar_pluviometricos_novos.py** - Sincronização incremental

### `setup/`
Scripts de configuração inicial do sistema:
- **criar_usuario_postgresql.sql** - Cria usuário no PostgreSQL
- **criar_banco_servidor.sql** - Script SQL para criar banco no servidor
- **criar_banco_servidor.sh** - Script shell para criar banco no servidor
- **testar_conexao.py** - Testa conexão com o servidor

### `automacao/`
Scripts para automatizar a execução:
- **cron_linux.sh** - Para usar com cron no Linux
- **cron_windows.bat/.ps1** - Para usar com Task Scheduler no Windows
- **prefect_flow.py** - Para usar com Prefect
- **configurar_cron_linux.sh** - Helper para configurar cron

### `docs/`
Documentação adicional do projeto

---

## 🔄 Fluxo de Uso

```
1. Setup (no servidor via SSH)
   └── ssh servicedesk@10.50.30.166
   └── psql -U postgres -f setup/criar_banco_servidor.sql

2. Carga Inicial
   └── python scripts/carregar_pluviometricos_historicos.py

3. Automação (escolha uma)
   ├── Linux: automacao/cron_linux.sh
   ├── Windows: automacao/cron_windows.bat ou .ps1
   └── Prefect: automacao/prefect_flow.py
```

---

## 📝 Notas Importantes

- Todos os scripts de automação estão configurados para usar caminhos relativos
- Os scripts principais estão em `scripts/` e podem ser executados de qualquer lugar
- Os logs são salvos em `logs/` na raiz do projeto
- O arquivo `.env` deve estar na raiz do projeto

