#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

YES=0
NO_INSTALL=0

for arg in "$@"; do
  case "$arg" in
    --yes|-y)
      YES=1
      ;;
    --no-install)
      NO_INSTALL=1
      ;;
    -h|--help)
      cat <<'EOF'
Usage: ./scripts/reset-dev-db.sh [--yes] [--no-install]

Resets the local dev Postgres database by deleting the docker volume, then re-running migrations.

WARNING: This permanently deletes local dev data (docker volume).

Options:
  --yes, -y       Skip confirmation prompt
  --no-install    Skip pip install -e '.[dev]' (faster if deps are already installed)
EOF
      exit 0
      ;;
    *)
      echo "Unknown arg: $arg" >&2
      exit 2
      ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "[reset] docker not found; install Docker Desktop" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "[reset] Docker daemon not running. Start Docker Desktop and retry." >&2
  exit 1
fi

if [[ "$YES" -ne 1 ]]; then
  cat <<'EOF'
[reset] This will run: docker compose down -v
[reset] That deletes the Postgres volume (all local dev DB data).
EOF
  read -r -p "[reset] Continue? (y/N): " ans
  if [[ "${ans:-}" != "y" && "${ans:-}" != "Y" ]]; then
    echo "[reset] Aborted."
    exit 0
  fi
fi

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "[reset] Created .env from .env.example"
fi

DATABASE_URL_LINE="$(grep -E '^DATABASE_URL=' .env || true)"
if [[ -n "$DATABASE_URL_LINE" ]] && [[ "$DATABASE_URL_LINE" == *":5432/"* ]]; then
  echo "[reset] WARNING: .env DATABASE_URL points at port 5432, but docker-compose maps Postgres to 5433 by default." >&2
  echo "[reset]          Consider setting DATABASE_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:5433/crypto_momentum" >&2
fi

echo "[reset] Stopping containers and deleting volumes..."
docker compose down -v

echo "[reset] Starting Postgres..."
docker compose up -d db

echo "[reset] Waiting for Postgres to be ready..."
for _ in {1..60}; do
  if docker compose exec -T db pg_isready -U postgres -d crypto_momentum >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! docker compose exec -T db pg_isready -U postgres -d crypto_momentum >/dev/null 2>&1; then
  echo "[reset] Postgres did not become ready in time." >&2
  exit 1
fi

VENV_PY="$ROOT_DIR/.venv/bin/python"
if [[ ! -x "$VENV_PY" ]]; then
  echo "[reset] Missing .venv. Run ./scripts/dev.sh once to set up the environment." >&2
  exit 1
fi

if [[ "$NO_INSTALL" -eq 0 ]]; then
  echo "[reset] Ensuring deps are installed..."
  "$VENV_PY" -m pip install -U pip
  "$VENV_PY" -m pip install -e '.[dev]'
else
  echo "[reset] Skipping dependency install (--no-install)"
fi

echo "[reset] Running migrations..."
exec "$VENV_PY" -m alembic upgrade head

