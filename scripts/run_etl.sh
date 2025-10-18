#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/home/raghu/Documents/projects/nelo_data_engg"
PYTHON_BIN="${PYTHON_BIN:-$(which python)}"

# Ensure required dirs exist
mkdir -p "$BASE_DIR/logs" "$BASE_DIR/dead_letter" "$BASE_DIR/staging" "$BASE_DIR/processed" "$BASE_DIR/archive"

export PYTHONUNBUFFERED=1
cd "$BASE_DIR"

exec "$PYTHON_BIN" etl.py
