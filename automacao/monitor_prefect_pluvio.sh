#!/bin/bash
set -u

# Monitor do container pluviometrico com alerta por e-mail.
# Executar via cron a cada 5 minutos.

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
STATE_DIR="$PROJECT_ROOT/automacao/.monitor_state"
STATE_FILE="$STATE_DIR/prefect_pluvio_last_status.txt"
LOG_FILE="$PROJECT_ROOT/logs/monitor_prefect_pluvio.log"
CONTAINER_NAME="prefect-bigquery-pluviometricos"

mkdir -p "$STATE_DIR" "$PROJECT_ROOT/logs"

now() {
  date '+%Y-%m-%d %H:%M:%S'
}

log() {
  echo "[$(now)] $1" >> "$LOG_FILE"
}

# Carregar variaveis do .env sem executar comandos arbitrarios
if [ -f "$ENV_FILE" ]; then
  while IFS='=' read -r key value; do
    case "$key" in
      ''|\#*) continue ;;
    esac
    value="${value%%$'\r'}"
    export "$key=$value"
  done < "$ENV_FILE"
fi

ALERT_TO="${PREFECT_ALERT_EMAIL_TO:-matheusbnas@gmail.com}"
SMTP_HOST="${SMTP_HOST:-smtp.gmail.com}"
SMTP_PORT="${SMTP_PORT:-587}"
SMTP_USER="${SMTP_USER:-}"
SMTP_APP_PASSWORD="${SMTP_APP_PASSWORD:-}"
ALERT_FROM="${ALERT_FROM:-$SMTP_USER}"

status="unknown"
details=""

if ! docker ps --format '{{.Names}}' | rg -x "$CONTAINER_NAME" >/dev/null 2>&1; then
  status="down"
  details="Container $CONTAINER_NAME nao esta em execucao."
else
  health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$CONTAINER_NAME" 2>/dev/null || echo "unknown")"
  running="$(docker inspect --format '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null || echo "false")"

  if [ "$running" != "true" ]; then
    status="down"
    details="Container $CONTAINER_NAME nao esta rodando."
  elif [ "$health" = "healthy" ] || [ "$health" = "none" ]; then
    status="ok"
    details="Container $CONTAINER_NAME rodando com health=$health."
  else
    status="degraded"
    details="Container $CONTAINER_NAME com health=$health."
  fi
fi

last_status=""
if [ -f "$STATE_FILE" ]; then
  last_status="$(cat "$STATE_FILE" 2>/dev/null)"
fi

echo "$status" > "$STATE_FILE"
log "$details"

# Se nao estiver ok, tenta reiniciar uma vez
if [ "$status" != "ok" ]; then
  log "Tentando restart automatico do servico prefect-pluviometricos..."
  if docker compose -f "$PROJECT_ROOT/docker-compose.yml" restart prefect-pluviometricos >> "$LOG_FILE" 2>&1; then
    sleep 5
    new_running="$(docker inspect --format '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null || echo "false")"
    new_health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$CONTAINER_NAME" 2>/dev/null || echo "unknown")"
    log "Resultado restart: running=$new_running health=$new_health"
    if [ "$new_running" = "true" ] && { [ "$new_health" = "healthy" ] || [ "$new_health" = "none" ]; }; then
      status="ok"
      details="$details Restart automatico recuperou o servico."
      echo "$status" > "$STATE_FILE"
    fi
  else
    log "Falha ao reiniciar serviço prefect-pluviometricos."
  fi
fi

# Envia e-mail apenas na mudanca para problema (evita spam)
if [ "$status" != "ok" ] && [ "$last_status" != "$status" ]; then
  if [ -n "$SMTP_USER" ] && [ -n "$SMTP_APP_PASSWORD" ]; then
    python3 - <<PY >> "$LOG_FILE" 2>&1
import smtplib
from email.mime.text import MIMEText

smtp_host = "${SMTP_HOST}"
smtp_port = int("${SMTP_PORT}")
smtp_user = "${SMTP_USER}"
smtp_password = "${SMTP_APP_PASSWORD}"
sender = "${ALERT_FROM}" or smtp_user
to = "${ALERT_TO}"
subject = "[ALERTA] Prefect Pluviometricos parado/degradado"
body = """Foi detectado problema no monitor do Prefect Pluviometricos.

Status atual: ${status}
Detalhes: ${details}
Host: $(hostname)
Data: $(now)
"""

msg = MIMEText(body, "plain", "utf-8")
msg["Subject"] = subject
msg["From"] = sender
msg["To"] = to

with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
    server.starttls()
    server.login(smtp_user, smtp_password)
    server.send_message(msg)
print("Email de alerta enviado para", to)
PY
    log "Alerta por e-mail enviado para $ALERT_TO."
  else
    log "SMTP_USER/SMTP_APP_PASSWORD nao configurados; e-mail nao enviado."
  fi
fi

exit 0
