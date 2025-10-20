#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/home/raghu/Documents/projects/nelo_data_engg"
# Use the RUN_TS from the environment (passed by cron_job_wrapper.sh)
# If it's not set, generate one for manual runs.
RUN_TS="${RUN_TS:-$(date +"%Y%m%d_%H%M%S")}"

PYTHON_BIN="${PYTHON_BIN:-/home/raghu/Documents/projects/nelo_data_engg/.venv/bin/python}"

# Prevent overlapping runs with a simple lock (compatible with cron)
LOCK_FILE="/tmp/nelo_etl.lock"

# Ensure required dirs exist
mkdir -p "$BASE_DIR/logs/runs" "$BASE_DIR/dead_letter" "$BASE_DIR/staging" "$BASE_DIR/processed" "$BASE_DIR/archive"

export PYTHONUNBUFFERED=1
export RUN_TS # Export it for the python scripts
cd "$BASE_DIR"

(
    flock -n 200 || { echo "Another ETL run is already in progress. Exiting."; exit 1; }

    SUCCESS=1

    # Initialize or migrate database schema
    if ! "$PYTHON_BIN" setup_database.py; then
        echo "setup_database.py failed with exit code $?" >&2
        SUCCESS=0
    fi

    # Run the main python script (creates per-component logs tagged with RUN_TS)
    if ! "$PYTHON_BIN" etl.py; then
        echo "etl.py failed with exit code $?" >&2
        SUCCESS=0
    fi

    # Consolidate logs after the run, regardless of success or failure
    echo "Consolidating logs for run ${RUN_TS}..."
    UNIFIED_LOG_FILE="$BASE_DIR/logs/runs/run_${RUN_TS}.log"
    touch "$UNIFIED_LOG_FILE"

    # Find all log files for this run and append them to the unified log.
    # Use a loop for clarity and robustness.
    find "$BASE_DIR/logs" -maxdepth 1 -type f -name "*_${RUN_TS}.log" | while read -r logfile; do
        echo -e "\n--- Log file: $(basename "$logfile") ---\n" >> "$UNIFIED_LOG_FILE"
        cat "$logfile" >> "$UNIFIED_LOG_FILE"
        # Optional: uncomment to clean up individual files after consolidation
        # rm "$logfile"
    done

    echo "Unified log created at ${UNIFIED_LOG_FILE}"

    # Update heartbeat only on overall success
    if [ "$SUCCESS" -eq 1 ]; then
        HEARTBEAT_DIR="$BASE_DIR/logs/heartbeat"
        mkdir -p "$HEARTBEAT_DIR"
        touch "$HEARTBEAT_DIR/successful_run"
        echo "Heartbeat file updated."
    fi

    # Exit with appropriate status so cron can detect failures
    if [ "$SUCCESS" -eq 1 ]; then
        exit 0
    else
        exit 1
    fi

) 200>"$LOCK_FILE"
