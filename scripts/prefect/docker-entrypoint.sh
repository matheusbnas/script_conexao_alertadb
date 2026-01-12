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

# Verificar variáveis de ambiente obrigatórias
if [ -z "$DB_ORIGEM_HOST" ]; then
    echo "❌ ERRO: DB_ORIGEM_HOST não definido"
    exit 1
fi

# Aguardar banco de dados (se necessário)
if [ -n "$DB_ORIGEM_HOST" ]; then
    wait_for_service "$DB_ORIGEM_HOST" "${DB_ORIGEM_PORT:-5432}" 30 || true
fi

# Executar comando
case "$1" in
    prefect-service)
        echo "🚀 Iniciando serviço Prefect..."
        exec python scripts/prefect/prefect_service.py --workflow "${PREFECT_WORKFLOW:-combinado}" --intervalo "${PREFECT_INTERVALO:-5}"
        ;;
    prefect-server)
        echo "🚀 Iniciando servidor Prefect..."
        exec prefect server start --host 0.0.0.0
        ;;
    *)
        exec "$@"
        ;;
esac
