#!/usr/bin/env bash
# Runs the naive-vs-batched throughput benchmark (see backend/benchmark/load_test.py)
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
source .venv/bin/activate
pip install -q -r backend/requirements-dev.txt

cd backend
exec python -m benchmark.load_test "$@"
