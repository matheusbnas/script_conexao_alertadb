#!/bin/bash
set -e

# Função para aguardar serviços
wait_for_service() {
    local host=$1
    local port=$2
    local timeout=${3:-30}
    
    echo "⏳ Aguardando serviço $host:$port..."
    for i in $(seq 1 $timeout); do
        # Tentar conectar usando Python (mais portável)
        if python -c "import socket; s = socket.socket(); s.settimeout(1); result = s.connect_ex(('$host', $port)); s.close(); exit(0 if result == 0 else 1)" 2>/dev/null; then
            echo "✅ Serviço $host:$port disponível"
            return 0
        fi
        sleep 1
    done
    
    echo "⚠️  Timeout aguardando $host:$port (continuando mesmo assim)"
    return 1
}

# Executar comando
case "$1" in
    prefect-service)
        # Verificar variáveis de ambiente obrigatórias para o serviço de sincronização
        if [ -z "$DB_ORIGEM_HOST" ]; then
            echo "❌ ERRO: DB_ORIGEM_HOST não definido"
            exit 1
        fi

        # Aguardar banco de dados de origem (se necessário)
        wait_for_service "$DB_ORIGEM_HOST" "${DB_ORIGEM_PORT:-5432}" 30 || true

        # API do Prefect (container prefect-server na rede Docker). Só porta aberta não garante HTTP
        # pronto; aguardamos TCP e depois a rota de health.
        if echo "${PREFECT_API_URL:-}" | grep -qE 'prefect-server|127\.0\.0\.1|localhost'; then
            echo "⏳ Aguardando Prefect API (prefect-server:4200)..."
            wait_for_service prefect-server 4200 120 || true
            PREFECT_HEALTH_OK=0
            for i in $(seq 1 90); do
                if python -c "
import urllib.request
for path in ('/health', '/api/health'):
    try:
        urllib.request.urlopen('http://prefect-server:4200' + path, timeout=3)
        raise SystemExit(0)
    except Exception:
        continue
raise SystemExit(1)
" 2>/dev/null; then
                    echo "✅ Prefect API (health) respondendo"
                    PREFECT_HEALTH_OK=1
                    break
                fi
                sleep 1
            done

            # Se a API do Prefect não responder, não aborta o container.
            # O service.py aplica fallback para execução direta dos scripts de sync,
            # mantendo atualização do BigQuery mesmo sem API do Prefect.
            if [ "$PREFECT_HEALTH_OK" -ne 1 ]; then
                echo "⚠️  Prefect API indisponível após timeout."
                echo "⚠️  Continuando sem API; service.py aplicará fallback de sincronização."
            fi
        fi

        echo "🚀 Iniciando serviço Prefect..."
        exec python scripts/prefect/service.py --workflow "${PREFECT_WORKFLOW:-combinado}" --intervalo "${PREFECT_INTERVALO:-5}"
        ;;
    prefect-server)
        echo "🚀 Iniciando servidor Prefect..."
        exec prefect server start --host 0.0.0.0
        ;;
    *)
        exec "$@"
        ;;
esac
