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

        # API do Prefect: usa host/porta da PREFECT_API_URL para funcionar
        # tanto em rede Docker bridge quanto em network_mode: host.
        PREFECT_API_HOST=$(python - <<'PY'
from urllib.parse import urlparse
import os
u = (os.getenv("PREFECT_API_URL") or "").strip()
if not u:
    print("")
else:
    p = urlparse(u)
    print(p.hostname or "")
PY
)
            PREFECT_API_PORT=$(python - <<'PY'
from urllib.parse import urlparse
import os
u = (os.getenv("PREFECT_API_URL") or "").strip()
if not u:
    print("4200")
else:
    p = urlparse(u)
    if p.port:
        print(p.port)
    else:
        print(443 if p.scheme == "https" else 80)
PY
)

        if [ -n "${PREFECT_API_HOST:-}" ]; then
            echo "⏳ Aguardando Prefect API (${PREFECT_API_HOST}:${PREFECT_API_PORT})..."
            wait_for_service "${PREFECT_API_HOST}" "${PREFECT_API_PORT}" 120 || true
            PREFECT_HEALTH_OK=0
            for i in $(seq 1 90); do
                if python -c "
import urllib.request
base = 'http://${PREFECT_API_HOST}:${PREFECT_API_PORT}'
for path in ('/health', '/api/health'):
    try:
        urllib.request.urlopen(base + path, timeout=3)
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
