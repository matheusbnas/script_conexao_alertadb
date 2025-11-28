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
└── consultar_alertadb_cor.py             # 🔧 Consulta
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

