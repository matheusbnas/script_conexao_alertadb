#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌧️ API REST - DADOS PLUVIOMÉTRICOS
Servidor: 10.50.30.166
Porta: 5000
Banco: alertadb_cor (ou DB_DESTINO_NAME configurado no .env)

Esta API utiliza os dados sincronizados do banco de destino (alertadb_cor),
que são atualizados automaticamente via cron a cada 5 minutos.

Configuração:
- Usa as mesmas variáveis de ambiente dos scripts de sincronização (DB_DESTINO_*)
- Mantém compatibilidade com variáveis antigas (DB_HOST, DB_NAME, etc.)
- Banco padrão: alertadb_cor (mesmo usado pelos scripts de sincronização)
"""

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime, timedelta
from functools import wraps
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
env_path = project_root / '.env'

# Verificar se o arquivo .env existe
if not env_path.exists():
    print("=" * 70)
    print("❌ ERRO: Arquivo .env não encontrado!")
    print("=" * 70)
    print(f"Arquivo esperado em: {env_path}")
    print("\n💡 SOLUÇÃO:")
    print("   1. Crie o arquivo .env na raiz do projeto")
    print("   2. Copie o exemplo: CONFIGURACAO_EXEMPLO.md")
    print("   3. Configure as variáveis DB_DESTINO_*")
    print("=" * 70)
    raise FileNotFoundError(f"Arquivo .env não encontrado em: {env_path}")

load_dotenv(dotenv_path=env_path, override=True)

app = Flask(__name__)
CORS(app)  # Permitir requisições de qualquer origem

# Configurar Flask para retornar JSON em caso de erro
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.config['JSON_SORT_KEYS'] = False

# ========================================
# CONFIGURAÇÕES
# ========================================

def obter_variavel(nome, obrigatoria=True, padrao=None):
    """Obtém variável de ambiente, lança erro se obrigatória e não encontrada."""
    valor = os.getenv(nome)
    if not valor or (isinstance(valor, str) and valor.strip() == ''):
        if obrigatoria:
            raise ValueError(f"❌ Variável de ambiente obrigatória não encontrada: {nome}")
        return padrao
    return valor.strip() if isinstance(valor, str) else valor

# Carregar configurações usando a mesma lógica dos scripts de sincronização
try:
    # Tentar DB_DESTINO_* primeiro, depois DB_* (retrocompatibilidade), depois padrões
    host_destino = obter_variavel('DB_DESTINO_HOST', obrigatoria=False)
    host_old = obter_variavel('DB_HOST', obrigatoria=False)
    host = host_destino or host_old or '10.50.30.166'
    
    port_destino = obter_variavel('DB_DESTINO_PORT', obrigatoria=False)
    port_old = obter_variavel('DB_PORT', obrigatoria=False)
    port = port_destino or port_old or '5432'
    
    dbname_destino = obter_variavel('DB_DESTINO_NAME', obrigatoria=False)
    dbname_old = obter_variavel('DB_NAME', obrigatoria=False)
    dbname = dbname_destino or dbname_old or 'alertadb_cor'
    
    user_destino = obter_variavel('DB_DESTINO_USER', obrigatoria=False)
    user_old = obter_variavel('DB_USER', obrigatoria=False)
    user = user_destino or user_old or 'postgres'
    
    # Senha é obrigatória - tentar DB_DESTINO_PASSWORD primeiro, depois DB_PASSWORD
    password = None
    try:
        password = obter_variavel('DB_DESTINO_PASSWORD', obrigatoria=True)
    except ValueError:
        try:
            password = obter_variavel('DB_PASSWORD', obrigatoria=True)
        except ValueError:
            raise ValueError("❌ Variável de ambiente obrigatória não encontrada: DB_DESTINO_PASSWORD ou DB_PASSWORD")
    
    DB_CONFIG = {
        'host': host,
        'port': port,
        'dbname': dbname,
        'user': user,
        'password': password
    }
except ValueError as e:
    print("=" * 70)
    print("❌ ERRO DE CONFIGURAÇÃO")
    print("=" * 70)
    print(str(e))
    print(f"\n📝 Arquivo .env: {env_path}")
    print("\n💡 Configure uma das seguintes variáveis no arquivo .env:")
    print("   DB_DESTINO_PASSWORD=sua_senha")
    print("   OU")
    print("   DB_PASSWORD=sua_senha (retrocompatibilidade)")
    print("\n📋 Exemplo de configuração completa:")
    print("   DB_DESTINO_HOST=10.50.30.166")
    print("   DB_DESTINO_PORT=5432")
    print("   DB_DESTINO_NAME=alertadb_cor")
    print("   DB_DESTINO_USER=seu_usuario")
    print("   DB_DESTINO_PASSWORD=sua_senha_aqui")
    print("=" * 70)
    raise

# Debug: Mostrar configuração (sem mostrar senha completa)
print("=" * 70)
print("🔧 CONFIGURAÇÃO DO BANCO DE DADOS")
print("=" * 70)
print(f"📁 Arquivo .env: {env_path}")
print(f"🌐 Host: {DB_CONFIG['host']}")
print(f"🔌 Porta: {DB_CONFIG['port']}")
print(f"💾 Banco: {DB_CONFIG['dbname']}")
print(f"👤 Usuário: {DB_CONFIG['user']}")
print(f"🔑 Senha: {'*' * 10 if DB_CONFIG['password'] else 'NÃO CONFIGURADA'}")
print("=" * 70)
print()

# API Key simples (opcional, para proteger a API)
API_KEY = os.getenv('API_KEY')

# ========================================
# DECORADORES
# ========================================

def require_api_key(f):
    """Decorator para validar API key (opcional)"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Se não há API_KEY configurada, permite acesso livre
        if not API_KEY:
            return f(*args, **kwargs)
        
        # Se há API_KEY configurada, valida
        api_key = request.headers.get('X-API-Key')
        if api_key and api_key == API_KEY:
            return f(*args, **kwargs)
        else:
            return jsonify({'erro': 'API Key inválida ou não fornecida'}), 401
    return decorated_function

def get_db_connection():
    """Cria conexão com o banco"""
    return psycopg2.connect(**DB_CONFIG)

def get_base_url():
    """Retorna a URL base da API baseada no request atual"""
    from flask import request
    scheme = request.scheme  # http ou https
    host = request.host  # host:porta do request
    return f"{scheme}://{host}"

# ========================================
# ROTAS DA API
# ========================================

@app.route('/', methods=['GET'])
def home():
    """Página inicial - serve o dashboard"""
    try:
        # Obter caminho absoluto do dashboard
        script_dir = os.path.dirname(os.path.abspath(__file__))
        dashboard_path = os.path.join(script_dir, 'dashboard.html')
        
        # Tentar caminho alternativo se não encontrar
        if not os.path.exists(dashboard_path):
            alt_path = os.path.join(project_root, 'scripts', 'dashboard.html')
            if os.path.exists(alt_path):
                dashboard_path = alt_path
        
        if os.path.exists(dashboard_path):
            # Ler e retornar arquivo como HTML
            with open(dashboard_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            return html_content, 200, {'Content-Type': 'text/html; charset=utf-8'}
        else:
            # Se não encontrar, retornar JSON com informações da API
            return jsonify({
                'api': 'API Dados Pluviométricos',
                'versao': '1.0',
                'erro': f'Dashboard não encontrado',
                'caminhos_tentados': [
                    os.path.join(script_dir, 'dashboard.html'),
                    os.path.join(project_root, 'scripts', 'dashboard.html')
                ],
                'endpoints': {
                    '/api': 'Informações da API',
                    '/api/docs': 'Documentação completa',
                    '/api/pluviometricos': 'Buscar dados pluviométricos',
                    '/api/estacoes': 'Listar todas as estações',
                    '/api/stats': 'Estatísticas gerais',
                    '/api/ultimos': 'Últimos registros',
                    '/api/periodo': 'Dados por período',
                    '/api/health': 'Status da API'
                }
            }), 404
    except Exception as e:
        import traceback
        return jsonify({
            'erro': f'Erro ao carregar dashboard: {str(e)}',
            'traceback': traceback.format_exc()
        }), 500

@app.route('/dashboard', methods=['GET'])
def dashboard():
    """Dashboard de dados pluviométricos"""
    try:
        # Obter caminho absoluto do dashboard
        script_dir = os.path.dirname(os.path.abspath(__file__))
        dashboard_path = os.path.join(script_dir, 'dashboard.html')
        
        # Debug: verificar se arquivo existe
        if not os.path.exists(dashboard_path):
            # Tentar caminho alternativo
            alt_path = os.path.join(project_root, 'scripts', 'dashboard.html')
            if os.path.exists(alt_path):
                dashboard_path = alt_path
            else:
                return jsonify({
                    'erro': 'Dashboard não encontrado',
                    'caminho_procurado_1': dashboard_path,
                    'caminho_procurado_2': alt_path,
                    'diretorio_script': script_dir,
                    'project_root': str(project_root),
                    'arquivos_no_diretorio': os.listdir(script_dir) if os.path.exists(script_dir) else 'diretorio_nao_existe'
                }), 404
        
        # Ler e retornar arquivo como HTML
        with open(dashboard_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        return html_content, 200, {'Content-Type': 'text/html; charset=utf-8'}
    except Exception as e:
        import traceback
        return jsonify({
            'erro': f'Erro ao carregar dashboard: {str(e)}',
            'traceback': traceback.format_exc()
        }), 500

@app.route('/api', methods=['GET'])
def api_info():
    """Informações da API"""
    base_url = get_base_url()
    return jsonify({
        'api': 'API Dados Pluviométricos',
        'versao': '1.0',
        'servidor': request.host.split(':')[0] if ':' in request.host else request.host,
        'documentacao': f'{base_url}/api/docs',
        'dashboard': f'{base_url}/dashboard',
        'endpoints': {
            'GET /api/pluviometricos': 'Buscar dados pluviométricos',
            'GET /api/estacoes': 'Listar todas as estações',
            'GET /api/estacoes/<id>': 'Dados de uma estação específica',
            'GET /api/stats': 'Estatísticas gerais',
            'GET /api/ultimos': 'Últimos registros',
            'GET /api/periodo': 'Dados por período',
            'GET /api/health': 'Status da API'
        }
    })

@app.route('/api/docs', methods=['GET'])
def docs():
    """Documentação completa da API"""
    base_url = get_base_url()
    return jsonify({
        'titulo': 'Documentação API Pluviométricos',
        'base_url': base_url,
        'endpoints': [
            {
                'rota': '/api/pluviometricos',
                'metodo': 'GET',
                'descricao': 'Buscar dados pluviométricos com filtros',
                'parametros': {
                    'data_inicio': 'Data inicial (formato: YYYY-MM-DD)',
                    'data_fim': 'Data final (formato: YYYY-MM-DD)',
                    'estacao_id': 'ID da estação',
                    'estacao_nome': 'Nome da estação (busca parcial)',
                    'limit': 'Limite de resultados (padrão: 1000, máximo: 10000)',
                    'offset': 'Deslocamento para paginação'
                },
                'exemplos': [
                    f'{base_url}/api/pluviometricos',
                    f'{base_url}/api/pluviometricos?data_inicio=2024-01-01&data_fim=2024-12-31',
                    f'{base_url}/api/pluviometricos?estacao_id=1&limit=100',
                    f'{base_url}/api/pluviometricos?estacao_nome=Campinas&limit=500',
                    f'{base_url}/api/pluviometricos?data_inicio=2024-01-01&estacao_id=1&limit=100&offset=0'
                ]
            },
            {
                'rota': '/api/estacoes',
                'metodo': 'GET',
                'descricao': 'Listar todas as estações disponíveis',
                'parametros': {},
                'exemplo': f'{base_url}/api/estacoes'
            },
            {
                'rota': '/api/estacoes/<id>',
                'metodo': 'GET',
                'descricao': 'Dados detalhados de uma estação específica',
                'parametros': {},
                'exemplo': f'{base_url}/api/estacoes/1'
            },
            {
                'rota': '/api/ultimos',
                'metodo': 'GET',
                'descricao': 'Últimos registros de todas as estações',
                'parametros': {
                    'horas': 'Últimas X horas (padrão: 24)'
                },
                'exemplo': f'{base_url}/api/ultimos?horas=48'
            },
            {
                'rota': '/api/periodo',
                'metodo': 'GET',
                'descricao': 'Agregação de dados por período',
                'parametros': {
                    'data_inicio': 'Data inicial (YYYY-MM-DD, opcional - padrão: últimos 30 dias)',
                    'data_fim': 'Data final (YYYY-MM-DD, opcional - padrão: data atual)',
                    'dias': 'Número de dias para buscar (opcional, usado se data_inicio/data_fim não fornecidos)',
                    'estacao_id': 'ID da estação (opcional)',
                    'agregacao': 'Tipo: dia, semana, mes (padrão: dia)'
                },
                'exemplos': [
                    f'{base_url}/api/periodo (usa últimos 30 dias automaticamente)',
                    f'{base_url}/api/periodo?dias=7 (últimos 7 dias)',
                    f'{base_url}/api/periodo?data_inicio=2024-01-01&data_fim=2024-12-31&agregacao=mes',
                    f'{base_url}/api/periodo?dias=90&agregacao=semana&estacao_id=1'
                ]
            },
            {
                'rota': '/api/stats',
                'metodo': 'GET',
                'descricao': 'Estatísticas gerais do banco de dados',
                'parametros': {},
                'exemplo': f'{base_url}/api/stats'
            },
            {
                'rota': '/api/health',
                'metodo': 'GET',
                'descricao': 'Status de saúde da API e banco de dados',
                'parametros': {},
                'exemplo': f'{base_url}/api/health'
            }
        ]
    })

@app.route('/api/pluviometricos', methods=['GET'])
@require_api_key
def get_pluviometricos():
    """
    Buscar dados pluviométricos com filtros
    
    Parâmetros:
    - data_inicio: YYYY-MM-DD
    - data_fim: YYYY-MM-DD
    - estacao_id: int
    - estacao_nome: string
    - limit: int (padrão: 1000)
    - offset: int (padrão: 0)
    """
    try:
        # Parâmetros
        data_inicio = request.args.get('data_inicio')
        data_fim = request.args.get('data_fim')
        estacao_id = request.args.get('estacao_id')
        estacao_nome = request.args.get('estacao_nome')
        limit = min(int(request.args.get('limit', 1000)), 10000)
        offset = int(request.args.get('offset', 0))
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Construir query dinâmica
        query = "SELECT * FROM pluviometricos WHERE 1=1"
        params = []
        
        if data_inicio:
            query += " AND dia >= %s"
            params.append(data_inicio)
        
        if data_fim:
            query += " AND dia <= %s"
            params.append(data_fim)
        
        if estacao_id:
            query += " AND estacao_id = %s"
            params.append(estacao_id)
        
        if estacao_nome:
            query += " AND estacao ILIKE %s"
            params.append(f'%{estacao_nome}%')
        
        query += " ORDER BY dia DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cur.execute(query, params)
        resultados = cur.fetchall()
        
        # Contar total (para paginação)
        count_query = "SELECT COUNT(*) FROM pluviometricos WHERE 1=1"
        count_params = []
        
        if data_inicio:
            count_query += " AND dia >= %s"
            count_params.append(data_inicio)
        
        if data_fim:
            count_query += " AND dia <= %s"
            count_params.append(data_fim)
        
        if estacao_id:
            count_query += " AND estacao_id = %s"
            count_params.append(estacao_id)
        
        if estacao_nome:
            count_query += " AND estacao ILIKE %s"
            count_params.append(f'%{estacao_nome}%')
        
        cur.execute(count_query, count_params)
        total = cur.fetchone()['count']
        
        cur.close()
        conn.close()
        
        return jsonify({
            'total': total,
            'limit': limit,
            'offset': offset,
            'resultados': len(resultados),
            'dados': resultados
        })
        
    except psycopg2.OperationalError as e:
        # Erro de conexão ou banco não disponível
        erro_msg = str(e)
        if 'not yet accepting connections' in erro_msg or 'recovery' in erro_msg.lower():
            return jsonify({
                'erro': 'Banco de dados não está disponível no momento',
                'detalhes': 'O servidor PostgreSQL está em processo de recuperação ou inicialização',
                'sugestao': 'Aguarde alguns instantes e tente novamente. O banco de dados está sendo inicializado.',
                'host': DB_CONFIG['host'],
                'porta': DB_CONFIG['port'],
                'banco': DB_CONFIG['dbname']
            }), 503  # Service Unavailable
        else:
            return jsonify({
                'erro': 'Erro de conexão com o banco de dados',
                'detalhes': erro_msg,
                'host': DB_CONFIG['host'],
                'porta': DB_CONFIG['port'],
                'banco': DB_CONFIG['dbname']
            }), 503
    except psycopg2.Error as e:
        return jsonify({
            'erro': 'Erro no banco de dados',
            'detalhes': str(e),
            'tipo': type(e).__name__
        }), 500
    except Exception as e:
        import traceback
        return jsonify({
            'erro': str(e),
            'tipo': type(e).__name__,
            'traceback': traceback.format_exc()
        }), 500

@app.route('/api/estacoes', methods=['GET'])
@require_api_key
def get_estacoes():
    """Lista todas as estações disponíveis"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                estacao_id,
                estacao,
                COUNT(*) as total_registros,
                MIN(dia) as primeira_leitura,
                MAX(dia) as ultima_leitura
            FROM pluviometricos
            GROUP BY estacao_id, estacao
            ORDER BY estacao;
        """)
        
        resultados = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'total_estacoes': len(resultados),
            'estacoes': resultados
        })
        
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/api/estacoes/<int:estacao_id>', methods=['GET'])
@require_api_key
def get_estacao_detalhes(estacao_id):
    """Detalhes de uma estação específica"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Informações gerais
        cur.execute("""
            SELECT 
                estacao_id,
                estacao,
                COUNT(*) as total_registros,
                MIN(dia) as primeira_leitura,
                MAX(dia) as ultima_leitura,
                ROUND(COALESCE(AVG(h24), 0)::numeric, 2) as media_h24,
                ROUND(COALESCE(MAX(h24), 0)::numeric, 2) as max_h24
            FROM pluviometricos
            WHERE estacao_id = %s
            GROUP BY estacao_id, estacao;
        """, (estacao_id,))
        
        info = cur.fetchone()
        
        if not info:
            return jsonify({'erro': 'Estação não encontrada'}), 404
        
        # Processar valores numéricos
        info_dict = dict(info)
        for campo in ['media_h24', 'max_h24']:
            if campo in info_dict and info_dict[campo] is not None:
                try:
                    valor = float(info_dict[campo])
                    if abs(valor) < 0.001:
                        info_dict[campo] = 0.00
                    else:
                        info_dict[campo] = round(valor, 2)
                except (ValueError, TypeError):
                    info_dict[campo] = 0.00
            else:
                info_dict[campo] = 0.00
        info = info_dict
        
        # Últimas 10 leituras
        cur.execute("""
            SELECT * FROM pluviometricos
            WHERE estacao_id = %s
            ORDER BY dia DESC
            LIMIT 10;
        """, (estacao_id,))
        
        ultimas_leituras = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify({
            'informacoes': info,
            'ultimas_leituras': ultimas_leituras
        })
        
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/api/ultimos', methods=['GET'])
@require_api_key
def get_ultimos():
    """Últimos registros de todas as estações"""
    try:
        horas = int(request.args.get('horas', 24))
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT * FROM pluviometricos
            WHERE dia >= NOW() - INTERVAL '%s hours'
            ORDER BY dia DESC;
        """, (horas,))
        
        resultados = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'periodo': f'Últimas {horas} horas',
            'total_registros': len(resultados),
            'dados': resultados
        })
        
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/api/periodo', methods=['GET'])
@require_api_key
def get_periodo():
    """
    Agregação de dados por período
    
    Parâmetros opcionais:
    - data_inicio: Data inicial (YYYY-MM-DD). Se não fornecido, usa últimos 30 dias ou início dos dados
    - data_fim: Data final (YYYY-MM-DD). Se não fornecido, usa data atual ou fim dos dados
    - dias: Número de dias para buscar (padrão: 30). Usado apenas se data_inicio/data_fim não fornecidos
    - estacao_id: ID da estação (opcional)
    - agregacao: Tipo de agregação - dia, semana, mes (padrão: dia)
    """
    conn = None
    cur = None
    try:
        data_inicio = request.args.get('data_inicio')
        data_fim = request.args.get('data_fim')
        dias = request.args.get('dias', type=int)  # Número de dias (opcional)
        estacao_id = request.args.get('estacao_id')
        agregacao = request.args.get('agregacao', 'dia')  # dia, semana, mes
        
        # Validar agregacao
        if agregacao not in ['dia', 'semana', 'mes']:
            return jsonify({'erro': 'agregacao deve ser: dia, semana ou mes'}), 400
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Verificar se a tabela existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'pluviometricos'
            );
        """)
        tabela_existe = cur.fetchone()['exists']
        
        if not tabela_existe:
            return jsonify({
                'erro': 'Tabela pluviometricos não encontrada',
                'sugestao': 'Execute primeiro: python scripts/carregar_pluviometricos_historicos.py'
            }), 404
        
        # Se não forneceu datas, determinar período automaticamente
        if not data_inicio or not data_fim:
            # Buscar data mínima e máxima do banco
            cur.execute("SELECT MIN(dia) as min_dia, MAX(dia) as max_dia FROM pluviometricos;")
            periodo_banco = cur.fetchone()
            
            if not periodo_banco or not periodo_banco['min_dia']:
                return jsonify({
                    'erro': 'Nenhum dado encontrado no banco',
                    'sugestao': 'Execute: python scripts/carregar_pluviometricos_historicos.py'
                }), 404
            
            max_dia_banco = periodo_banco['max_dia']
            min_dia_banco = periodo_banco['min_dia']
            
            # Se forneceu apenas dias, calcular período
            if dias:
                data_fim = max_dia_banco.strftime('%Y-%m-%d') if isinstance(max_dia_banco, datetime) else str(max_dia_banco)[:10]
                data_inicio_obj = datetime.strptime(data_fim, '%Y-%m-%d') - timedelta(days=dias)
                data_inicio = data_inicio_obj.strftime('%Y-%m-%d')
            else:
                # Padrão: últimos 30 dias ou período completo se menos de 30 dias disponíveis
                if isinstance(max_dia_banco, datetime):
                    data_fim = max_dia_banco.strftime('%Y-%m-%d')
                    data_inicio_obj = max_dia_banco - timedelta(days=30)
                    data_inicio = max(data_inicio_obj.strftime('%Y-%m-%d'), 
                                     min_dia_banco.strftime('%Y-%m-%d') if isinstance(min_dia_banco, datetime) else str(min_dia_banco)[:10])
                else:
                    data_fim = str(max_dia_banco)[:10]
                    data_inicio_obj = datetime.strptime(data_fim, '%Y-%m-%d') - timedelta(days=30)
                    data_inicio = max(data_inicio_obj.strftime('%Y-%m-%d'), str(min_dia_banco)[:10])
        
        # Validar formato das datas
        try:
            datetime.strptime(data_inicio, '%Y-%m-%d')
            datetime.strptime(data_fim, '%Y-%m-%d')
        except ValueError as e:
            return jsonify({
                'erro': 'Formato de data inválido. Use YYYY-MM-DD',
                'data_inicio': data_inicio,
                'data_fim': data_fim,
                'detalhes': str(e)
            }), 400
        
        # Validar que data_inicio <= data_fim
        if data_inicio > data_fim:
            return jsonify({
                'erro': 'data_inicio deve ser anterior ou igual a data_fim',
                'data_inicio': data_inicio,
                'data_fim': data_fim
            }), 400
        
        # Definir formato de agregação
        if agregacao == 'semana':
            date_trunc = "DATE_TRUNC('week', dia)"
        elif agregacao == 'mes':
            date_trunc = "DATE_TRUNC('month', dia)"
        else:
            date_trunc = "DATE_TRUNC('day', dia)"
        
        query = f"""
            SELECT 
                {date_trunc} as periodo,
                estacao_id,
                estacao,
                ROUND(COALESCE(AVG(m05), 0)::numeric, 2) as media_m05,
                ROUND(COALESCE(AVG(m15), 0)::numeric, 2) as media_m15,
                ROUND(COALESCE(AVG(h01), 0)::numeric, 2) as media_h01,
                ROUND(COALESCE(AVG(h04), 0)::numeric, 2) as media_h04,
                ROUND(COALESCE(AVG(h24), 0)::numeric, 2) as media_h24,
                ROUND(COALESCE(AVG(h96), 0)::numeric, 2) as media_h96,
                ROUND(COALESCE(MAX(h24), 0)::numeric, 2) as max_h24,
                COUNT(*) as total_leituras
            FROM pluviometricos
            WHERE dia >= %s AND dia <= %s
        """
        
        params = [data_inicio, data_fim]
        
        if estacao_id:
            try:
                estacao_id_int = int(estacao_id)
                query += " AND estacao_id = %s"
                params.append(estacao_id_int)
            except ValueError:
                return jsonify({'erro': 'estacao_id deve ser um número inteiro'}), 400
        
        query += f"""
            GROUP BY {date_trunc}, estacao_id, estacao
            ORDER BY periodo DESC;
        """
        
        cur.execute(query, params)
        resultados = cur.fetchall()
        
        # Processar resultados para formatar valores numéricos
        dados_formatados = []
        for row in resultados:
            row_dict = dict(row)
            # Converter valores numéricos para float e formatar
            campos_numericos = ['media_m05', 'media_m15', 'media_h01', 'media_h04', 
                              'media_h24', 'media_h96', 'max_h24']
            for campo in campos_numericos:
                if campo in row_dict and row_dict[campo] is not None:
                    try:
                        valor = float(row_dict[campo])
                        # Se o valor for muito pequeno (praticamente zero), usar 0.00
                        if abs(valor) < 0.001:
                            row_dict[campo] = 0.00
                        else:
                            row_dict[campo] = round(valor, 2)
                    except (ValueError, TypeError):
                        row_dict[campo] = 0.00
                else:
                    row_dict[campo] = 0.00
            
            # Formatar período se for datetime
            if 'periodo' in row_dict and row_dict['periodo']:
                if isinstance(row_dict['periodo'], datetime):
                    row_dict['periodo'] = row_dict['periodo'].isoformat()
            
            dados_formatados.append(row_dict)
        
        return jsonify({
            'agregacao': agregacao,
            'data_inicio': data_inicio,
            'data_fim': data_fim,
            'periodo_usado': f'{data_inicio} até {data_fim}',
            'total_registros': len(dados_formatados),
            'dados': dados_formatados
        })
        
    except psycopg2.Error as e:
        return jsonify({
            'erro': 'Erro no banco de dados',
            'detalhes': str(e),
            'tipo': type(e).__name__
        }), 500
    except Exception as e:
        import traceback
        return jsonify({
            'erro': str(e),
            'tipo': type(e).__name__,
            'traceback': traceback.format_exc()
        }), 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@app.route('/api/stats', methods=['GET'])
@require_api_key
def get_stats():
    """Estatísticas gerais do banco"""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Verificar se a tabela existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'pluviometricos'
            );
        """)
        tabela_existe = cur.fetchone()['exists']
        
        if not tabela_existe:
            return jsonify({
                'erro': 'Tabela pluviometricos não encontrada',
                'sugestao': 'Execute primeiro: python scripts/carregar_pluviometricos_historicos.py'
            }), 404
        
        cur.execute("""
            SELECT 
                COUNT(*) as total_registros,
                MIN(dia) as data_minima,
                MAX(dia) as data_maxima,
                COUNT(DISTINCT estacao_id) as total_estacoes,
                ROUND(COALESCE(AVG(h24), 0)::numeric, 2) as media_geral_h24,
                ROUND(COALESCE(MAX(h24), 0)::numeric, 2) as max_geral_h24
            FROM pluviometricos;
        """)
        
        stats = cur.fetchone()
        
        # Processar valores numéricos para evitar notação científica
        if stats:
            stats_dict = dict(stats)
            for campo in ['media_geral_h24', 'max_geral_h24']:
                if campo in stats_dict and stats_dict[campo] is not None:
                    try:
                        valor = float(stats_dict[campo])
                        if abs(valor) < 0.001:
                            stats_dict[campo] = 0.00
                        else:
                            stats_dict[campo] = round(valor, 2)
                    except (ValueError, TypeError):
                        stats_dict[campo] = 0.00
                else:
                    stats_dict[campo] = 0.00
            stats = stats_dict
        
        # Se não houver dados, retornar valores padrão
        if not stats or stats['total_registros'] == 0:
            return jsonify({
                'estatisticas_gerais': {
                    'total_registros': 0,
                    'data_minima': None,
                    'data_maxima': None,
                    'total_estacoes': 0,
                    'media_geral_h24': None,
                    'max_geral_h24': None
                },
                'top_5_estacoes': [],
                'aviso': 'Nenhum dado encontrado na tabela. Execute: python scripts/carregar_pluviometricos_historicos.py'
            })
        
        # Top 5 estações com mais registros
        cur.execute("""
            SELECT estacao, COUNT(*) as total
            FROM pluviometricos
            GROUP BY estacao
            ORDER BY total DESC
            LIMIT 5;
        """)
        
        top_estacoes = cur.fetchall()
        
        return jsonify({
            'estatisticas_gerais': stats,
            'top_5_estacoes': top_estacoes
        })
        
    except psycopg2.Error as e:
        return jsonify({
            'erro': 'Erro no banco de dados',
            'detalhes': str(e),
            'tipo': type(e).__name__
        }), 500
    except Exception as e:
        import traceback
        return jsonify({
            'erro': str(e),
            'tipo': type(e).__name__,
            'traceback': traceback.format_exc()
        }), 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Handler global de erros para garantir que sempre retorne JSON
@app.errorhandler(404)
def not_found(error):
    return jsonify({'erro': 'Endpoint não encontrado'}), 404

@app.errorhandler(500)
def internal_error(error):
    import traceback
    return jsonify({
        'erro': 'Erro interno do servidor',
        'detalhes': str(error),
        'tipo': type(error).__name__
    }), 500

@app.errorhandler(Exception)
def handle_exception(e):
    import traceback
    return jsonify({
        'erro': str(e),
        'tipo': type(e).__name__,
        'traceback': traceback.format_exc()
    }), 500

@app.route('/api/health', methods=['GET'])
def health():
    """Status de saúde da API"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        
        return jsonify({
            'status': 'ok',
            'banco': 'conectado',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'erro',
            'banco': 'desconectado',
            'erro': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# ========================================
# MAIN
# ========================================

if __name__ == '__main__':
    # Configurações do servidor (via .env ou padrões)
    SERVER_HOST = os.getenv('SERVER_HOST', '0.0.0.0')  # 0.0.0.0 permite acesso de qualquer interface
    SERVER_PORT = int(os.getenv('SERVER_PORT', '5000'))
    DEBUG_MODE = os.getenv('DEBUG', 'False').lower() == 'true'
    
    # URL base para exibição (usa localhost se host for 0.0.0.0)
    display_host = 'localhost' if SERVER_HOST == '0.0.0.0' else SERVER_HOST
    base_url = f"http://{display_host}:{SERVER_PORT}"
    
    print("="*70)
    print("🌧️  API DADOS PLUVIOMÉTRICOS")
    print("="*70)
    print(f"🌐 Servidor: {base_url}")
    print(f"📊 Dashboard: {base_url}/dashboard")
    print(f"📚 Documentação: {base_url}/api/docs")
    print(f"💚 Health Check: {base_url}/api/health")
    print(f"🔧 Host: {SERVER_HOST} | Porta: {SERVER_PORT} | Debug: {DEBUG_MODE}")
    print(f"💾 Banco de dados: {DB_CONFIG['dbname']} @ {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"👤 Usuário: {DB_CONFIG['user']}")
    print("="*70)
    print()
    
    # Rodar em produção com WSGI (ex: gunicorn)
    # gunicorn -w 4 -b 0.0.0.0:5000 scripts.app:app
    
    # Desenvolvimento
    app.run(host=SERVER_HOST, port=SERVER_PORT, debug=DEBUG_MODE)