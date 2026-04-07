#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/run_scheduled_pipeline.sh <incremental|standard|full> [extra run_pipeline args...]"
  exit 2
fi

MODE="$1"
shift || true

if [[ "$MODE" != "incremental" && "$MODE" != "standard" && "$MODE" != "full" ]]; then
  echo "Invalid mode: $MODE"
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"
mkdir -p data/processed/scheduler_logs data/processed/api_logs data/processed/alerts data/processed/ops

RETENTION_DAYS="${BREATHWISE_PIPELINE_RETENTION_DAYS:-30}"
ROTATE_MAX_BYTES="${BREATHWISE_LOG_ROTATE_MAX_BYTES:-5000000}"
ROTATE_RETAIN_ARCHIVES="${BREATHWISE_LOG_ROTATE_RETAIN_ARCHIVES:-14}"
PYTHON_BIN="${BREATHWISE_PYTHON_BIN:-python3}"
SCHED_HEALTH_FAIL_LEVEL="${BREATHWISE_SCHED_HEALTH_FAIL_LEVEL:-critical}"
SCHED_HEALTH_ENFORCE="${BREATHWISE_SCHED_ENFORCE_HEALTH:-false}"

PIPELINE_EXIT=0
"${PYTHON_BIN}" scripts/run_pipeline.py \
  --mode "${MODE}" \
  --quiet \
  --prune-old-runs \
  --retention-days "${RETENTION_DAYS}" \
  "$@" || PIPELINE_EXIT=$?

# Always refresh lightweight ops status and log hygiene artifacts.
"${PYTHON_BIN}" scripts/ops_status.py --format text --write-file || true
"${PYTHON_BIN}" scripts/check_scheduler_health.py --format text --fail-level "${SCHED_HEALTH_FAIL_LEVEL}" || SCHED_HEALTH_EXIT=$?
"${PYTHON_BIN}" scripts/rotate_ops_logs.py --max-bytes "${ROTATE_MAX_BYTES}" --retain-archives "${ROTATE_RETAIN_ARCHIVES}" || true

if [[ "${SCHED_HEALTH_ENFORCE}" == "true" && "${SCHED_HEALTH_EXIT:-0}" -ne 0 && "${PIPELINE_EXIT}" -eq 0 ]]; then
  PIPELINE_EXIT="${SCHED_HEALTH_EXIT}"
fi

exit "${PIPELINE_EXIT}"
