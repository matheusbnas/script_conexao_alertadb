-- ============================================================================
-- 🌧️ SCRIPT SQL PARA IMPORTAR DADOS PLUVIOMÉTRICOS VIA DBLINK
-- ============================================================================
-- 
-- Este script substitui o Python, fazendo tudo via SQL
-- Execute no banco DESTINO: alertadb_cor (10.50.30.166)
-- ============================================================================

-- 1️⃣ INSTALAR EXTENSÃO DBLINK (se não existir)
CREATE EXTENSION IF NOT EXISTS dblink;

-- 2️⃣ TESTAR CONEXÃO COM O BANCO ORIGEM
DO $$
DECLARE
    teste INTEGER;
BEGIN
    -- Tenta conectar ao banco origem
    SELECT * INTO teste
    FROM dblink(
        'host=10.2.223.114 dbname=alertadb user=planejamento_cor password=$1u2ddi(X?D}339x',
        'SELECT 1'
    ) AS t(resultado INTEGER);
    
    RAISE NOTICE '✅ CONEXÃO COM BANCO ORIGEM: SUCESSO!';
    RAISE NOTICE '   Host: 10.2.223.114';
    RAISE NOTICE '   Banco: alertadb';
    
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '❌ ERRO DE CONEXÃO COM BANCO ORIGEM: %', SQLERRM
        USING HINT = 'Verifique: 1) Firewall, 2) pg_hba.conf no servidor origem, 3) Credenciais';
END $$;

-- 3️⃣ CRIAR TABELA DESTINO (se não existir)
CREATE TABLE IF NOT EXISTS pluviometricos (
    dia TIMESTAMPTZ NOT NULL,
    m05 NUMERIC(10,2),
    m10 NUMERIC(10,2),
    m15 NUMERIC(10,2),
    h01 NUMERIC(10,2),
    h04 NUMERIC(10,2),
    h24 NUMERIC(10,2),
    h96 NUMERIC(10,2),
    estacao VARCHAR(150),
    estacao_id INTEGER,
    PRIMARY KEY (dia, estacao_id)
);

-- 4️⃣ VERIFICAR QUANTOS DADOS EXISTEM NO BANCO ORIGEM
DO $$
DECLARE
    total_origem BIGINT;
    data_min TIMESTAMP;
    data_max TIMESTAMP;
BEGIN
    -- Consultar totais no banco origem
    SELECT * INTO total_origem
    FROM dblink(
        'host=10.2.223.114 dbname=alertadb user=planejamento_cor password=$1u2ddi(X?D}339x',
        'SELECT COUNT(*) FROM public.estacoes_leitura el
         JOIN public.estacoes_leiturachuva elc ON elc.leitura_id = el.id
         JOIN public.estacoes_estacao ee ON ee.id = el.estacao_id'
    ) AS t(total BIGINT);
    
    -- Consultar período disponível
    SELECT * INTO data_min, data_max
    FROM dblink(
        'host=10.2.223.114 dbname=alertadb user=planejamento_cor password=$1u2ddi(X?D}339x',
        'SELECT MIN(el."horaLeitura"), MAX(el."horaLeitura")
         FROM public.estacoes_leitura el
         JOIN public.estacoes_leiturachuva elc ON elc.leitura_id = el.id
         JOIN public.estacoes_estacao ee ON ee.id = el.estacao_id'
    ) AS t(data_min TIMESTAMP, data_max TIMESTAMP);
    
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE '📊 DIAGNÓSTICO DO BANCO ORIGEM';
    RAISE NOTICE '========================================';
    RAISE NOTICE '   Total de registros: %', total_origem;
    RAISE NOTICE '   Data mínima: %', data_min;
    RAISE NOTICE '   Data máxima: %', data_max;
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    
END $$;

-- 5️⃣ IMPORTAR DADOS (com ON CONFLICT para evitar duplicatas)
DO $$
DECLARE
    registros_inseridos BIGINT;
    inicio TIMESTAMP;
    fim TIMESTAMP;
BEGIN
    inicio := clock_timestamp();
    
    RAISE NOTICE '🔄 Iniciando importação de dados...';
    RAISE NOTICE '   Aguarde, isso pode levar alguns minutos...';
    RAISE NOTICE '';
    
    -- Inserir dados do banco origem
    INSERT INTO pluviometricos (
        dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
    )
    SELECT 
        dia, m05, m10, m15, h01, h04, h24, h96, estacao, estacao_id
    FROM dblink(
        'host=10.2.223.114 dbname=alertadb user=planejamento_cor password=$1u2ddi(X?D}339x',
        $query$
        SELECT 
            el."horaLeitura" AS dia,
            elc.m05,
            elc.m10,
            elc.m15,
            elc.h01,
            elc.h04,
            elc.h24,
            elc.h96,
            ee.nome AS estacao,
            el.estacao_id
        FROM public.estacoes_leitura AS el
        JOIN public.estacoes_leiturachuva AS elc
            ON elc.leitura_id = el.id
        JOIN public.estacoes_estacao AS ee
            ON ee.id = el.estacao_id
        ORDER BY el."horaLeitura" ASC
        $query$
    ) AS origem(
        dia TIMESTAMPTZ,
        m05 NUMERIC(10,2),
        m10 NUMERIC(10,2),
        m15 NUMERIC(10,2),
        h01 NUMERIC(10,2),
        h04 NUMERIC(10,2),
        h24 NUMERIC(10,2),
        h96 NUMERIC(10,2),
        estacao VARCHAR(150),
        estacao_id INTEGER
    )
    ON CONFLICT (dia, estacao_id) DO NOTHING;
    
    GET DIAGNOSTICS registros_inseridos = ROW_COUNT;
    
    fim := clock_timestamp();
    
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ IMPORTAÇÃO CONCLUÍDA!';
    RAISE NOTICE '========================================';
    RAISE NOTICE '   Registros inseridos: %', registros_inseridos;
    RAISE NOTICE '   Tempo decorrido: %', fim - inicio;
    RAISE NOTICE '========================================';
    
END $$;

-- 6️⃣ EXIBIR ESTATÍSTICAS FINAIS
DO $$
DECLARE
    total_tabela BIGINT;
    data_min TIMESTAMP;
    data_max TIMESTAMP;
BEGIN
    SELECT COUNT(*), MIN(dia), MAX(dia)
    INTO total_tabela, data_min, data_max
    FROM pluviometricos;
    
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE '📊 ESTATÍSTICAS DA TABELA DESTINO';
    RAISE NOTICE '========================================';
    RAISE NOTICE '   Total de registros: %', total_tabela;
    RAISE NOTICE '   Data mínima: %', data_min;
    RAISE NOTICE '   Data máxima: %', data_max;
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    
END $$;

-- 7️⃣ VERIFICAR DISTRIBUIÇÃO POR ANO
SELECT 
    EXTRACT(YEAR FROM dia)::INTEGER AS ano,
    COUNT(*) AS total_registros,
    MIN(dia)::DATE AS primeira_data,
    MAX(dia)::DATE AS ultima_data
FROM pluviometricos
GROUP BY EXTRACT(YEAR FROM dia)
ORDER BY ano DESC;