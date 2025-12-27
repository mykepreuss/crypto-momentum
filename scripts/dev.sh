#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

NO_INSTALL=0
for arg in "$@"; do
  case "$arg" in
    --no-install)
      NO_INSTALL=1
      ;;
    -h|--help)
      cat <<'EOF'
Usage: ./scripts/dev.sh [--no-install]

Starts Crypto Momentum Scout for local development:
- Ensures .env exists (copies from .env.example if missing)
- Starts Postgres via docker-compose (host port 5433 by default)
- Runs Alembic migrations
- Runs uvicorn with --reload

Options:
  --no-install   Skip pip install -e '.[dev]' step
EOF
      exit 0
      ;;
    *)
      echo "Unknown arg: $arg" >&2
      exit 2
      ;;
  esac
done

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "[dev] Created .env from .env.example"
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "[dev] docker not found; install Docker Desktop" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "[dev] Docker daemon not running. Start Docker Desktop and retry." >&2
  exit 1
fi

echo "[dev] Starting Postgres (docker compose)..."
docker compose up -d db

echo "[dev] Waiting for Postgres to be ready..."
for _ in {1..60}; do
  if docker compose exec -T db pg_isready -U postgres -d crypto_momentum >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! docker compose exec -T db pg_isready -U postgres -d crypto_momentum >/dev/null 2>&1; then
  echo "[dev] Postgres did not become ready in time." >&2
  exit 1
fi

DATABASE_URL_LINE="$(grep -E '^DATABASE_URL=' .env || true)"
if [[ -n "$DATABASE_URL_LINE" ]] && [[ "$DATABASE_URL_LINE" == *":5432/"* ]]; then
  echo "[dev] WARNING: .env DATABASE_URL points at port 5432, but docker-compose maps Postgres to 5433 by default." >&2
  echo "[dev]          Consider setting DATABASE_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:5433/crypto_momentum" >&2
fi

VENV_PY="$ROOT_DIR/.venv/bin/python"
if [[ ! -x "$VENV_PY" ]]; then
  PY_BIN=""
  for candidate in python3.12 python3.11 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then
      PY_BIN="$candidate"
      break
    fi
  done
  if [[ -z "$PY_BIN" ]]; then
    echo "[dev] No suitable python found. Install Python 3.11+ (recommended: python3.12 via Homebrew)." >&2
    exit 1
  fi

  echo "[dev] Creating virtualenv (.venv) using $PY_BIN"
  "$PY_BIN" -m venv .venv
fi

VENV_PY="$ROOT_DIR/.venv/bin/python"
VENV_VER="$("$VENV_PY" -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"
VENV_MAJOR_MINOR="$("$VENV_PY" -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
if [[ "$VENV_MAJOR_MINOR" != "3.11" && "$VENV_MAJOR_MINOR" != "3.12" && "$VENV_MAJOR_MINOR" != "3.13" ]]; then
  echo "[dev] .venv python is $VENV_VER, but this project requires Python >= 3.11." >&2
  echo "[dev] Recreate venv with: rm -rf .venv && python3.12 -m venv .venv" >&2
  exit 1
fi

if [[ "$NO_INSTALL" -eq 0 ]]; then
  echo "[dev] Installing deps into .venv..."
  "$VENV_PY" -m pip install -U pip
  "$VENV_PY" -m pip install -e '.[dev]'
else
  echo "[dev] Skipping dependency install (--no-install)"
fi

echo "[dev] Running migrations..."
set +e
"$VENV_PY" -m alembic upgrade head
ALEMBIC_RC=$?
set -e
if [[ "$ALEMBIC_RC" -ne 0 ]]; then
  echo "[dev] Alembic migration failed." >&2
  echo "[dev] If you see 'role \"postgres\" does not exist', ensure DATABASE_URL points at the docker-mapped port (5433 by default)." >&2
  exit "$ALEMBIC_RC"
fi

echo "[dev] Starting API on http://127.0.0.1:8000"
exec "$VENV_PY" -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000

