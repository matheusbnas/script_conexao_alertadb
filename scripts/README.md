# 📁 Scripts Principais

Esta pasta contém os scripts principais do sistema de sincronização de dados pluviométricos.

---

## 🚀 Scripts Essenciais

### `carregar_pluviometricos_historicos.py`
**Script principal de carga inicial**

- Faz a carga inicial completa de todos os dados históricos
- Deve ser executado **APENAS UMA VEZ** antes de usar o sincronizador
- Processa dados desde 1997 até a data atual
- Usa DISTINCT ON para garantir unicidade
- Preserva timezone (-02:00 e -03:00) corretamente

**Uso:**
```bash
python scripts/carregar_pluviometricos_historicos.py
```

---

### `sincronizar_pluviometricos_novos.py`
**Script de sincronização incremental em tempo real**

- Mantém os dados atualizados automaticamente
- Executa continuamente verificando novos dados a cada 5 minutos
- Deve ser executado **APÓS** a carga inicial
- Pode ser executado via cron/agendador de tarefas

**Uso:**
```bash
# Modo contínuo (padrão)
python scripts/sincronizar_pluviometricos_novos.py

# Modo único (para cron)
python scripts/sincronizar_pluviometricos_novos.py --once
```

---

### `app.py`
**API REST para consulta dos dados**

- Servidor Flask com endpoints REST
- Consulta dados do banco alertadb_cor
- Interface web para visualização

**Uso:**
```bash
python scripts/app.py
```

---

### `dashboard.html`
**Dashboard web para visualização**

- Interface HTML para visualizar dados
- Abre no navegador para análise visual

---

## 🔧 Scripts Utilitários

### `validar_dados_pluviometricos.py`
**Validação completa de dados**

- Compara dados entre origem e destino
- Útil para verificar integridade após carga/sincronização

**Uso:**
```bash
python scripts/validar_dados_pluviometricos.py
```

---

### `corrigir_dados_pluviometricos.py`
**Correção de dados para período específico**

- Corrige dados incorretos em um período específico
- Útil quando há divergências pontuais

**Uso:**
```bash
python scripts/corrigir_dados_pluviometricos.py
```

---

### `consultar_alertadb_cor.py`
**Consulta de dados do banco destino**

- Consulta dados da tabela pluviometricos
- Similar à query do banco origem, mas adaptado para alertadb_cor

**Uso:**
```bash
python scripts/consultar_alertadb_cor.py [data_inicial] [data_final] [estacao_id]
```

---

### `copiar_tabela_pluviometricos.py`
**Copia tabela pluviometricos entre bancos**

- Copia a tabela completa (estrutura e dados) entre bancos PostgreSQL
- Útil para migração ou sincronização entre ambientes
- Processa dados em lotes para otimizar memória
- Usa ON CONFLICT DO UPDATE para tratar duplicatas
- Configuração via arquivo .env

**Uso:**
```bash
python scripts/copiar_tabela_pluviometricos.py
```

**Configuração (.env):**
```env
# Banco ORIGEM para CÓPIA (alertadb_cor)
DB_COPIA_ORIGEM_HOST=10.50.30.166
DB_COPIA_ORIGEM_PORT=5432
DB_COPIA_ORIGEM_NAME=alertadb_cor
DB_COPIA_ORIGEM_USER=postgres
DB_COPIA_ORIGEM_PASSWORD=

# Banco DESTINO para CÓPIA (alertadb)
DB_COPIA_DESTINO_HOST=82.25.74.207
DB_COPIA_DESTINO_PORT=7077
DB_COPIA_DESTINO_NAME=alertadb
DB_COPIA_DESTINO_USER=postgres
DB_COPIA_DESTINO_PASSWORD=
```

**⚠️ IMPORTANTE:** Este script usa variáveis específicas com prefixo `DB_COPIA_*` 
para não conflitar com as variáveis `DB_ORIGEM_*` e `DB_DESTINO_*` usadas em 
outros scripts do projeto.

---

### `exportar_pluviometricos_parquet.py`
**Exporta tabela pluviometricos para arquivos Parquet**

- Exporta dados da tabela pluviometricos para formato Parquet (comprimido)
- Útil para backup, transferência de dados ou análise offline
- Pode dividir dados por ano ou exportar tudo em um arquivo
- Formato eficiente e comprimido (menor tamanho que CSV)
- Configuração via arquivo .env

**Uso:**
```bash
python scripts/exportar_pluviometricos_parquet.py
```

**Configuração (.env):**
```env
# Banco ORIGEM para EXPORTAÇÃO (alertadb_cor)
DB_COPIA_ORIGEM_HOST=10.50.30.166
DB_COPIA_ORIGEM_PORT=5432
DB_COPIA_ORIGEM_NAME=alertadb_cor
DB_COPIA_ORIGEM_USER=postgres
DB_COPIA_ORIGEM_PASSWORD=
```

**Dependências:**
```bash
pip install pandas pyarrow
```

**Arquivos gerados:**
- `exports/pluviometricos_YYYY.parquet` (se dividir por ano)
- `exports/pluviometricos_completo.parquet` (se exportar tudo)

---

### `zipar_exports_parquet.py`
**Compacta arquivos Parquet em ZIP**

- Compacta todos os arquivos .parquet da pasta exports/ em arquivo(s) ZIP
- Útil para backup, transferência ou compartilhamento dos dados
- Opção de compactar tudo em um ZIP ou dividir por década
- Mostra estatísticas de compressão (tamanho antes/depois)

**Uso:**
```bash
python scripts/zipar_exports_parquet.py
```

**Opções:**
1. Um único arquivo ZIP (todos os arquivos)
2. Dividir por década (1990s, 2000s, 2010s, 2020s)

**Arquivos gerados:**
- `exports/pluviometricos_export_YYYYMMDD_HHMMSS.zip` (opção 1)
- `exports/pluviometricos_1990s.zip`, `pluviometricos_2000s.zip`, etc. (opção 2)

---

## 📂 Estrutura

```
scripts/
├── README.md                              # Este arquivo
├── carregar_pluviometricos_historicos.py # ⭐ Carga inicial
├── sincronizar_pluviometricos_novos.py   # ⭐ Sincronização incremental
├── app.py                                 # ⭐ API REST
├── dashboard.html                         # ⭐ Dashboard web
├── validar_dados_pluviometricos.py       # 🔧 Validação
├── corrigir_dados_pluviometricos.py      # 🔧 Correção
├── consultar_alertadb_cor.py             # 🔧 Consulta
├── copiar_tabela_pluviometricos.py       # 🔧 Cópia entre bancos
├── exportar_pluviometricos_parquet.py    # 🔧 Exportação para Parquet
└── zipar_exports_parquet.py              # 🔧 Compactação ZIP
```

---

## 🧪 Scripts de Teste

Scripts de teste e diagnóstico foram movidos para `tests/`:
- `tests/diagnosticar_inconsistencias.py`
- `tests/investigar_divergencias.py`
- `tests/verificar_periodo_especifico.py`
- `tests/verificar_registro_especifico.py`

---

## 📚 Documentação

Para mais informações, consulte:
- `automacao/README.md` - Automação e cron
- `docs/` - Documentação completa
- `README.md` - Documentação principal do projeto

