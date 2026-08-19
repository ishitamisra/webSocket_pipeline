#!/usr/bin/env bash
# Runs the pipeline: ingests the live Binance trade stream, aggregates it,
# and serves the dashboard at http://localhost:8000
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
source .venv/bin/activate
pip install -q -r requirements.txt

cd backend
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}" --reload
