# 📊 BigQuery - Guia Completo

Guia completo para integração com Google BigQuery, incluindo configuração, credenciais, exportação, visualização e automação.

---

## 📋 Índice

1. [Visão Geral](#visão-geral)
2. [Configuração Inicial](#configuração-inicial)
3. [Obter Credenciais](#obter-credenciais)
4. [Configurar Variáveis](#configurar-variáveis)
5. [Exportação de Dados](#exportação-de-dados)
6. [Formatos Suportados](#formatos-suportados)
7. [Visualizar Dados](#visualizar-dados)
8. [Formato dos Dados](#formato-dos-dados)
9. [Automação](#automação)
10. [Compartilhar Acesso](#compartilhar-acesso)

---

## 🎯 Visão Geral

### Opções de Integração

**Opção 1: NIMBUS → BigQuery (Direto)** ⭐ RECOMENDADO
- Exporta diretamente do NIMBUS para BigQuery
- Mais rápido (menos camadas)
- Script: `scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py`

**Opção 2: Cloud SQL → BigQuery (Federated Queries)**
- Consulta dados do Cloud SQL diretamente no BigQuery
- Sem necessidade de copiar dados
- Dados sempre atualizados

**Opção 3: Cloud SQL → BigQuery (Exportação)**
- Exporta dados do Cloud SQL para BigQuery
- Dados em BigQuery (mais rápido para consultas)
- Requer sincronização periódica

---

## ⚙️ Configuração Inicial

### Pré-requisitos

1. **Conta Google Cloud Platform** ativa
2. **Projeto GCP** criado
3. **Python 3.8+** instalado
4. **Bibliotecas Python** instaladas:
   ```bash
   pip install google-cloud-bigquery google-auth pyarrow pandas
   ```

### Script Automatizado

O script `scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py` faz automaticamente:
1. ✅ Conecta ao PostgreSQL (NIMBUS)
2. ✅ Busca dados usando DISTINCT ON (mesma lógica do script original)
3. ✅ Exporta para formato **Parquet** (mais eficiente que CSV/SQL)
4. ✅ Carrega automaticamente no BigQuery
5. ✅ Cria dataset/tabela se não existir

**⚠️ IMPORTANTE:** Não existe formato "SQL" para BigQuery!
- BigQuery **NÃO** aceita arquivos `.sql` com INSERT statements
- BigQuery **NÃO** aceita dumps PostgreSQL diretamente
- Você precisa **exportar** dados do PostgreSQL para CSV/Parquet/JSON primeiro
- O script faz isso **automaticamente**!

---

## 🔑 Obter Credenciais

### Método Recomendado: Service Account

#### Passo 1: Acessar o Console do GCP

1. Acesse: https://console.cloud.google.com
2. Selecione seu projeto (ou crie um novo se necessário)

#### Passo 2: Criar Service Account

1. No menu lateral, vá em **"IAM & Admin"** → **"Service Accounts"**
   - Ou acesse diretamente: https://console.cloud.google.com/iam-admin/serviceaccounts

2. Clique em **"Create Service Account"** ou **"Criar conta de serviço"**

3. Preencha os dados:
   - **Service account name:** `bigquery-exporter` (ou o nome que preferir)
   - **Service account ID:** Será gerado automaticamente
   - **Description:** `Service account para exportar dados para BigQuery`

4. Clique em **"Create and Continue"**

#### Passo 3: Conceder Permissões (Roles)

Adicione as seguintes roles (uma por vez):

**Role 1: BigQuery Data Editor**
- Procure por: `BigQuery Data Editor`
- Selecione: **"BigQuery Data Editor"**

**Role 2: BigQuery Job User**
- Procure por: `BigQuery Job User`
- Selecione: **"BigQuery Job User"**

**Role 3: BigQuery User** (opcional, mas recomendado)
- Procure por: `BigQuery User`
- Selecione: **"BigQuery User"**

#### Passo 4: Criar e Baixar a Chave JSON

1. Na lista de Service Accounts, clique na conta criada
2. Vá na aba **"Keys"** (Chaves)
3. Clique em **"Add Key"** → **"Create new key"**
4. Selecione o formato: **"JSON"**
5. Clique em **"Create"**
6. O arquivo JSON será baixado automaticamente!

**⚠️ IMPORTANTE:** 
- Guarde este arquivo com segurança
- Não compartilhe publicamente
- Não faça commit no Git (já está no .gitignore)

#### Passo 5: Salvar o Arquivo

1. Renomeie o arquivo baixado para: `credentials.json`
2. Coloque na pasta `credentials/` na raiz do projeto:

```
projeto/
└── credentials/
    └── credentials.json  ← Arquivo baixado aqui
```

### Método Alternativo: Credenciais Padrão do Ambiente

Se você não quiser usar Service Account, pode usar as credenciais padrão do ambiente:

```bash
# Instalar Google Cloud SDK
# https://cloud.google.com/sdk/docs/install

# Autenticar-se
gcloud auth application-default login

# Não precisa configurar BIGQUERY_CREDENTIALS_PATH no .env
```

**⚠️ Limitação:** Este método usa suas credenciais pessoais, não é ideal para produção.

---

## 🔧 Configurar Variáveis

### Variáveis Necessárias no .env

```env
# BigQuery
BIGQUERY_PROJECT_ID=seu-projeto-id
BIGQUERY_DATASET_ID=nome-do-dataset
BIGQUERY_TABLE_ID=nome-da-tabela
BIGQUERY_CREDENTIALS_PATH=/caminho/credentials.json  # Opcional
```

### 1. BIGQUERY_PROJECT_ID

**Onde encontrar:**

#### Via Console GCP (Mais Fácil)
1. Acesse: https://console.cloud.google.com
2. No topo da página, você verá o **ID do projeto** ao lado do nome do projeto
3. Exemplo: Se o nome é "Meu Projeto", o ID pode ser `1029418267270`

#### Via Linha de Comando
```bash
# Listar projetos
gcloud projects list

# Ver projeto atual
gcloud config get-value project
```

**💡 Dica:** Use o **ID numérico**, não o nome do projeto!

### 2. BIGQUERY_DATASET_ID

**Onde encontrar:**

1. Acesse: https://console.cloud.google.com/bigquery
2. No painel esquerdo, você verá os **datasets** do projeto
3. Se não existir, você precisa criar um:

**Criar Dataset:**
1. No BigQuery Console, clique em **"Create Dataset"** ou **"Criar conjunto de dados"**
2. Configure:
   - **Dataset ID:** `pluviometricos` (ou o nome que preferir)
   - **Location type:** `Multi-region` ou `Region` (ex: `us-west1`)
   - **Default table expiration:** Deixe em branco ou configure
3. Clique em **"Create dataset"**

**💡 Dica:** O dataset será criado automaticamente pelo script se não existir!

### 3. BIGQUERY_TABLE_ID

**Onde encontrar:**

1. Acesse: https://console.cloud.google.com/bigquery
2. Expanda o dataset no painel esquerdo
3. Você verá as tabelas dentro do dataset
4. Se não existir, o script criará automaticamente!

**💡 Dica:** O script criará a tabela automaticamente na primeira execução!

### 4. BIGQUERY_CREDENTIALS_PATH (Opcional)

**Quando usar:**
- ✅ Se você está rodando o script **localmente** (não no GCP)
- ✅ Se precisa de autenticação específica
- ✅ Se não quer usar `gcloud auth application-default login`

**💡 Dica:** O script detecta automaticamente `credentials/credentials.json` se não configurar no `.env`

---

## 📤 Exportação de Dados

### Processo Completo: PostgreSQL → Arquivo → BigQuery

**Não existe formato "SQL" para BigQuery!**

O processo é:
1. **PostgreSQL (NIMBUS)** → Dados em tabelas (formato interno)
2. **Exportar** → Converter para arquivo (CSV/Parquet/JSON)
3. **BigQuery** → Importar arquivo → Dados em tabelas

### Usar Script Automatizado (Recomendado)

```bash
# Configurar .env primeiro
BIGQUERY_PROJECT_ID=seu-projeto-gcp
BIGQUERY_DATASET_ID=pluviometricos
BIGQUERY_TABLE_ID=pluviometricos

# Executar
python scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py
```

**Pronto!** Os dados estarão no BigQuery! 🎉

### O que o Script Faz

1. ✅ **Conecta ao PostgreSQL (NIMBUS)**
2. ✅ **Busca dados usando SQL** com DISTINCT ON
3. ✅ **Converte para DataFrame (Pandas)**
4. ✅ **Exporta para Parquet** (formato otimizado)
5. ✅ **Carrega no BigQuery automaticamente**

**Você não precisa se preocupar com formatos!** O script faz tudo.

---

## 📊 Formatos Suportados

O BigQuery aceita os seguintes formatos de arquivo para importação:

### 1. CSV (Comma-Separated Values)
- ✅ **Suportado:** Sim
- ✅ **Comprimido:** Sim (GZIP)
- ⚡ **Performance:** Boa
- 💾 **Tamanho:** Médio

### 2. JSON (JavaScript Object Notation)
- ✅ **Suportado:** Sim
- ✅ **Comprimido:** Sim (GZIP)
- ⚡ **Performance:** Média
- 💾 **Tamanho:** Grande

### 3. Parquet ⭐ RECOMENDADO
- ✅ **Suportado:** Sim
- ✅ **Comprimido:** Sim (nativo)
- ⚡ **Performance:** **Excelente** (mais rápido)
- 💾 **Tamanho:** **Menor** (mais eficiente)
- 🎯 **Vantagens:**
  - Formato colunar (otimizado para análises)
  - Compressão automática
  - Preserva tipos de dados
  - Mais rápido para carregar

**Por que usar Parquet:**
- ✅ 5-10x mais rápido que CSV
- ✅ 50-80% menor que CSV
- ✅ Preserva tipos de dados (não precisa conversão)
- ✅ Ideal para BigQuery

### 4. Avro
- ✅ **Suportado:** Sim
- ✅ **Comprimido:** Sim (nativo)
- ⚡ **Performance:** Boa
- 💾 **Tamanho:** Médio

### 5. ORC (Optimized Row Columnar)
- ✅ **Suportado:** Sim
- ✅ **Comprimido:** Sim (nativo)
- ⚡ **Performance:** Excelente
- 💾 **Tamanho:** Menor

### Comparação de Formatos

| Formato | Velocidade | Tamanho | Compressão | Preserva Tipos | Recomendado |
|---------|------------|---------|------------|----------------|-------------|
| **Parquet** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ✅ Nativa | ✅ Sim | ✅ **SIM** |
| **ORC** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ✅ Nativa | ✅ Sim | ✅ Sim |
| **Avro** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ✅ Nativa | ✅ Sim | ⚠️ Médio |
| **CSV** | ⭐⭐⭐ | ⭐⭐ | ⚠️ GZIP | ❌ Não | ⚠️ Básico |
| **JSON** | ⭐⭐ | ⭐ | ⚠️ GZIP | ⚠️ Parcial | ❌ Não |

---

## 📍 Visualizar Dados

### Onde os Dados Estão Armazenados?

Os dados são armazenados **diretamente no BigQuery**, que é um data warehouse gerenciado pelo Google Cloud.

**Características:**
- ✅ Armazenamento **nativo** do BigQuery (não precisa configurar buckets)
- ✅ **Otimizado** para consultas SQL rápidas
- ✅ **Escalável** automaticamente
- ✅ **Seguro** e gerenciado pelo Google

**⚠️ Importante:** Os dados **NÃO** são salvos automaticamente no Google Cloud Storage. Eles ficam apenas no BigQuery.

### Como Acessar os Dados

#### Método 1: BigQuery Console (Web)

1. **Acesse o Console do BigQuery:**
   ```
   https://console.cloud.google.com/bigquery
   ```

2. **Selecione seu projeto:**
   - No topo da página, clique no dropdown de projetos
   - Selecione o projeto configurado

3. **Navegue até sua tabela:**
   - No painel esquerdo, expanda seu projeto
   - Expanda o dataset (ex: `alertadb_cor_raw`)
   - Clique na tabela (ex: `pluviometricos`)

4. **Visualize os dados:**
   - Clique na aba **"Preview"** para ver uma prévia dos dados
   - Ou clique em **"Query"** para escrever consultas SQL

#### Método 2: Consulta SQL Direta

1. **No BigQuery Console, clique em "Compose new query"** ou use o editor SQL

2. **Escreva sua consulta:**
   ```sql
   -- Ver primeiros 100 registros
   SELECT *
   FROM `seu-projeto.dataset.pluviometricos`
   LIMIT 100;
   ```

3. **Execute a query:**
   - Clique em **"Run"** ou pressione `Ctrl+Enter`
   - Os resultados aparecerão abaixo

### Exemplos de Consultas Úteis

#### Ver Total de Registros
```sql
SELECT COUNT(*) as total_registros
FROM `seu-projeto.dataset.pluviometricos`;
```

#### Ver Dados de uma Estação Específica
```sql
SELECT *
FROM `seu-projeto.dataset.pluviometricos`
WHERE estacao_id = 1
ORDER BY dia DESC
LIMIT 100;
```

#### Ver Dados por Período
```sql
SELECT *
FROM `seu-projeto.dataset.pluviometricos`
WHERE dia >= '2024-01-01'
  AND dia < '2024-02-01'
ORDER BY dia DESC;
```

#### Ver Estatísticas por Estação
```sql
SELECT 
  estacao,
  estacao_id,
  COUNT(*) as total_registros,
  MIN(dia) as primeira_data,
  MAX(dia) as ultima_data
FROM `seu-projeto.dataset.pluviometricos`
GROUP BY estacao, estacao_id
ORDER BY estacao_id;
```

---

## 📊 Formato dos Dados

### Comparação: NIMBUS vs BigQuery

**✅ Os dados são praticamente idênticos**, com pequenas diferenças técnicas de tipos de dados que **não afetam os valores** nem a visualização.

### Estrutura das Colunas

| Coluna | NIMBUS (Origem) | BigQuery (Destino) | Mudou? |
|--------|-----------------|---------------------|--------|
| `dia` | `horaLeitura` (TIMESTAMP WITH TIME ZONE) | `dia` (TIMESTAMP) | ✅ Nome padronizado |
| `dia_original` | - | `dia_original` (STRING) | ✅ Novo (formato original com timezone) |
| `m05` | NUMERIC(10,2) | FLOAT64 | ⚠️ Tipo diferente, mas valores iguais |
| `m10` | NUMERIC(10,2) | FLOAT64 | ⚠️ Tipo diferente, mas valores iguais |
| `m15` | NUMERIC(10,2) | FLOAT64 | ⚠️ Tipo diferente, mas valores iguais |
| `h01` | NUMERIC(10,2) | FLOAT64 | ⚠️ Tipo diferente, mas valores iguais |
| `h04` | NUMERIC(10,2) | FLOAT64 | ⚠️ Tipo diferente, mas valores iguais |
| `h24` | NUMERIC(10,2) | FLOAT64 | ⚠️ Tipo diferente, mas valores iguais |
| `h96` | NUMERIC(10,2) | FLOAT64 | ⚠️ Tipo diferente, mas valores iguais |
| `estacao` | VARCHAR(150) | STRING | ✅ Equivalente |
| `estacao_id` | INTEGER | INTEGER | ✅ Idêntico |

### Coluna `dia_original`

A coluna `dia_original` preserva o formato original do NIMBUS com timezone:
- Formato: `2009-02-18 00:57:20.000 -0300`
- Mostra claramente se é horário padrão (`-0300`) ou horário de verão (`-0200`)

### Garantias de Consistência

- ✅ **Mesma lógica** de DISTINCT ON
- ✅ **Mesmos valores** (precisão suficiente)
- ✅ **Mesma ordem** de processamento
- ✅ **Timezone preservado** na coluna `dia_original`

---

## ⏰ Automação

### Sincronização Incremental Automática (Recomendado)

Para manter os dados atualizados automaticamente a cada 5 minutos, use o script de sincronização incremental.

#### Pré-requisitos

1. **Carga inicial concluída**: Executeu `exportar_pluviometricos_nimbus_bigquery.py` com sucesso
2. **Servidor Linux** com acesso ao banco Nimbus
3. **Python 3.8+** instalado
4. **Credenciais do GCP** configuradas (`credentials/credentials.json`)
5. **Variáveis de ambiente** configuradas no `.env`

#### Configuração Rápida

1. **Testar o Script Manualmente**
   ```bash
   python scripts/bigquery/sincronizar_pluviometricos_nimbus_bigquery.py --once
   ```

2. **Configurar Cron Automaticamente**
   ```bash
   cd automacao
   ./configurar_cron.sh bigquery
   ```

   Isso configurará o cron para executar a cada 5 minutos automaticamente.

#### Configuração Manual

1. **Editar crontab**
   ```bash
   crontab -e
   ```

2. **Adicionar linha para executar a cada 5 minutos**
   ```bash
   # Sincronização incremental BigQuery a cada 5 minutos
   */5 * * * * /caminho/completo/para/automacao/cron.sh bigquery
   ```

### Exportação Completa Periódica (Opcional)

Se preferir fazer exportação completa periódica ao invés de incremental:

1. **Preparar o Ambiente**
   ```bash
   cd /caminho/para/script_conexao_alertadb
   source .venv/bin/activate  # Se usar ambiente virtual
   pip install -r requirements.txt
   ```

2. **Testar o Script Manualmente**
   ```bash
   python scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py
   ```

3. **Configurar Cron**
   ```bash
   crontab -e
   ```

4. **Adicionar Linha para Executar Diariamente às 2h**
   ```bash
   # Exportar dados NIMBUS → BigQuery diariamente às 2h (completo)
   0 2 * * * cd /caminho/para/script_conexao_alertadb && /caminho/para/python3 scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py >> /var/log/bigquery_export.log 2>&1
   ```

   **Ou executar a cada 6 horas:**
   ```bash
   # Exportar dados NIMBUS → BigQuery a cada 6 horas (completo)
   0 */6 * * * cd /caminho/para/script_conexao_alertadb && /caminho/para/python3 scripts/bigquery/exportar_pluviometricos_nimbus_bigquery.py >> /var/log/bigquery_export.log 2>&1
   ```

### Formato Cron

```
* * * * * comando
│ │ │ │ │
│ │ │ │ └─── Dia da semana (0-7, 0 e 7 = domingo)
│ │ │ └───── Mês (1-12)
│ │ └─────── Dia do mês (1-31)
│ └───────── Hora (0-23)
└─────────── Minuto (0-59)
```

### Exemplos de Agendamento

| Cron | Descrição |
|------|-----------|
| `0 2 * * *` | Diariamente às 2h |
| `0 */6 * * *` | A cada 6 horas |
| `0 3 * * 0` | Semanalmente (domingo às 3h) |
| `0 0 1 * *` | Mensalmente (dia 1 às 0h) |
| `*/30 * * * *` | A cada 30 minutos |

### Verificar Logs

```bash
# Ver logs em tempo real
tail -f /var/log/bigquery_export.log

# Ver últimas 100 linhas
tail -n 100 /var/log/bigquery_export.log

# Procurar erros
grep -i error /var/log/bigquery_export.log
```

---

## 🚨 Troubleshooting

### Erro: "Permission denied"
**Causa:** Service Account não tem permissões suficientes.

**Solução:**
1. Vá em **IAM & Admin** → **Service Accounts**
2. Clique na sua Service Account
3. Vá em **"Permissions"** ou **"IAM"**
4. Adicione as roles: `BigQuery Data Editor`, `BigQuery Job User`, `BigQuery User`

### Erro: "Invalid credentials"
**Causa:** Arquivo JSON corrompido ou caminho incorreto.

**Solução:**
1. Verifique se o arquivo está em `credentials/credentials.json`
2. Verifique se o arquivo não está corrompido
3. Tente baixar novamente a chave JSON

### Erro: "Project not found"
**Causa:** `project_id` no JSON não corresponde ao projeto atual.

**Solução:**
1. Verifique o `BIGQUERY_PROJECT_ID` no `.env`
2. Certifique-se de que corresponde ao `project_id` no JSON

### Erro de Memória
**Causa:** Processando muitos dados de uma vez.

**Solução:**
- O script já está otimizado para usar mínima memória
- Processa em chunks pequenos (25k registros)
- Escreve múltiplos arquivos Parquet (não acumula tudo)

---

## 🔐 Compartilhar Acesso

Para conceder acesso de **somente leitura** (consulta) no BigQuery para clientes usando Service Accounts, consulte o guia completo:

📚 **[Guia Completo: Compartilhar Acesso ao BigQuery](BIGQUERY_COMPARTILHAR_ACESSO.md)**

### Resumo Rápido

**Service Account do Cliente:**
- Email: `lncc-cefet@rj-cor.iam.gserviceaccount.com`
- Project ID: `rj-cor`

**Como Conceder Acesso:**

1. **Via Console GCP:**
   - BigQuery → Dataset → Share dataset
   - Adicionar: `lncc-cefet@rj-cor.iam.gserviceaccount.com`
   - Role: **BigQuery Data Viewer** (somente leitura)
   - Salvar

2. **Via CLI:**
   ```bash
   bq add-iam-member \
     --member="serviceAccount:lncc-cefet@rj-cor.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataViewer" \
     "alertadb-cor:alertadb_cor_raw"
   ```
---

## 📚 Links Úteis

- **BigQuery Console:** https://console.cloud.google.com/bigquery
- **Service Accounts:** https://console.cloud.google.com/iam-admin/serviceaccounts
- **BigQuery Connections:** https://console.cloud.google.com/bigquery/connections
- **GCP Projects:** https://console.cloud.google.com/cloud-resource-manager
- **Documentação BigQuery:** https://cloud.google.com/bigquery/docs
- **Compartilhar Acesso:** [BIGQUERY_COMPARTILHAR_ACESSO.md](BIGQUERY_COMPARTILHAR_ACESSO.md)

---

**Última atualização:** 2025

