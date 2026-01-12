FROM python:3.11-slim

# Metadados
LABEL maintainer="COR - Centro de Operações de Riscos"
LABEL description="Serviço Prefect para sincronização BigQuery"

# Variáveis de ambiente
ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    PREFECT_HOME=/app/.prefect

# Criar diretório de trabalho
WORKDIR /app

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código do projeto
COPY . .

# Criar diretórios necessários
RUN mkdir -p /app/logs /app/exports /app/.prefect

# Script de inicialização
COPY scripts/prefect/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Healthcheck
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:4200/api/health', timeout=5)" || exit 1

# Expor porta do Prefect (se usar servidor local)
EXPOSE 4200

# Comando padrão
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["prefect-service"]
