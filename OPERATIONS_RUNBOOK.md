# Operations Runbook

## Purpose
This runbook defines repeatable local-first operations for Breathwise TR:
- scheduled pipeline execution
- failure/success alerting
- log hygiene and retention
- lightweight operational visibility

Canonical release/share sequence is defined in:
- `RELEASE_WORKFLOW.md`

## Core Entrypoints
- Primary pipeline:
  - `python3 scripts/run_pipeline.py --mode standard`
- Scheduler wrapper:
  - `bash scripts/run_scheduled_pipeline.sh <incremental|standard|full>`
- Ops summary:
  - `python3 scripts/ops_status.py --format text --write-file`
- Localization metadata:
  - `curl -H "X-API-Key: $BREATHWISE_API_KEY" "http://127.0.0.1:8000/v1/meta/localization"`

## Pipeline Modes
- `incremental`
  - Marts incremental refresh, validation, contracts, API smoke
- `standard`
  - Historical forecast extended + CAMS + marts + validation + contracts + API smoke
- `full`
  - Standard + pytest

## Safe Operations Defaults
- Lock guard enabled by default to prevent overlapping runs.
- Strict validation remains required.
- API smoke remains part of mode flows.
- Optional run retention cleanup:
  - `--prune-old-runs --retention-days 30`

## Resumable Runs
Resume command:
```bash
python3 scripts/run_pipeline.py --mode standard --resume-from-latest-failed
```

Rule:
- Resume is allowed only when `latest_run_manifest.json` has `status=failed`.

## Run Artifacts
Per run:
- `data/processed/pipeline_runs/<run_id>/run_manifest.json`
- `data/processed/pipeline_runs/<run_id>/events.jsonl`
- `data/processed/pipeline_runs/<run_id>/<step>.log`

Global:
- `data/processed/pipeline_runs/latest_run_manifest.json`
- `data/processed/pipeline_runs/latest_success_run_manifest.json`
- `data/processed/pipeline_runs/history.jsonl`
- `data/processed/pipeline_runs/pipeline.lock` (transient)

Run manifest includes:
- step-level status/timing/exit codes
- artifact summary
- artifact delta vs latest successful run
- run-management metadata (lock/prune/ops status path)
- alerting summary

## Scheduler Integration
Templates:
- `ops/scheduler/cron/crontab.example`
- `ops/scheduler/launchd/*.plist`
- `ops/scheduler/systemd/*`
- `ops/scheduler/README.md`

Recommended schedule policy:
- Every 6 hours: incremental
- Daily 02:15: standard
- Weekly Sunday 03:30: full

### Cron Quickstart (Mac/Linux)
1. Replace `__PROJECT_ROOT__` placeholders in `ops/scheduler/cron/crontab.example`.
2. Install:
```bash
crontab ops/scheduler/cron/crontab.example
```
3. Verify:
```bash
crontab -l
tail -n 200 /absolute/project/path/data/processed/scheduler_logs/incremental.log
```

Optional:
- set `BREATHWISE_PYTHON_BIN` if scheduler environment cannot resolve `python3`.

### launchd Quickstart (macOS)
1. Replace `__PROJECT_ROOT__` placeholders in `ops/scheduler/launchd/*.plist`.
2. Copy files to `~/Library/LaunchAgents/`.
3. Load each:
```bash
launchctl load ~/Library/LaunchAgents/com.breathwise.pipeline.incremental.plist
launchctl load ~/Library/LaunchAgents/com.breathwise.pipeline.standard.plist
launchctl load ~/Library/LaunchAgents/com.breathwise.pipeline.full.plist
```

### systemd Quickstart (Linux)
1. Replace `__PROJECT_ROOT__` placeholders in `ops/scheduler/systemd/*`.
2. Copy to `/etc/systemd/system/`.
3. Enable timers:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now breathwise-pipeline-incremental.timer
sudo systemctl enable --now breathwise-pipeline-standard.timer
sudo systemctl enable --now breathwise-pipeline-full.timer
```

## Alerting (Local-First Hooks)
Alert hooks are evaluated at pipeline run finalization.

Environment variables:
- `BREATHWISE_ALERTS_ENABLED` (`true|false`, default `false`)
- `BREATHWISE_ALERT_ON_FAILURE` (`true|false`, default `true`)
- `BREATHWISE_ALERT_ON_SUCCESS` (`true|false`, default `false`)
- `BREATHWISE_ALERTS_DIR` (default `data/processed/alerts`)
- `BREATHWISE_ALERT_WEBHOOK_URL` (optional)
- `BREATHWISE_ALERT_WEBHOOK_TIMEOUT_SECONDS` (default `5`)
- `BREATHWISE_ALERT_WEBHOOK_RETRIES` (default `2`)
- `BREATHWISE_ALERT_WEBHOOK_BACKOFF_SECONDS` (default `2`, exponential backoff)
- `BREATHWISE_ALERT_DEDUP_WINDOW_MINUTES` (default `60`)
- `BREATHWISE_ALERT_REPEAT_EVERY_FAILURES` (default `5`)
- `BREATHWISE_ALERT_STATE_PATH` (default `data/processed/alerts/alert_state.json`)
- `BREATHWISE_ALERT_MAC_NOTIFY` (`true|false`, default `false`)

Behavior:
- If alerting is enabled and condition matches:
  - writes `data/processed/alerts/alerts_history.jsonl`
  - updates `data/processed/alerts/latest_alert.json`
  - optionally sends webhook (with retry/backoff)
  - optionally sends macOS notification (`osascript`)
- repeated identical failures are deduplicated inside dedup window.
- repeated-failure escalation re-sends after threshold is reached within window.

## Scheduler Health Checks
Health checks evaluate expected run cadence for `incremental`, `standard`, and `full` modes from pipeline history.

CLI:
```bash
python3 scripts/check_scheduler_health.py --format text
python3 scripts/check_scheduler_health.py --format json --fail-level warning
```

Policy environment variables:
- `BREATHWISE_SCHED_INCREMENTAL_EXPECTED_HOURS` (default `6`)
- `BREATHWISE_SCHED_INCREMENTAL_MAX_STALE_HOURS` (default `12`)
- `BREATHWISE_SCHED_INCREMENTAL_REQUIRED` (default `true`)
- `BREATHWISE_SCHED_STANDARD_EXPECTED_HOURS` (default `24`)
- `BREATHWISE_SCHED_STANDARD_MAX_STALE_HOURS` (default `36`)
- `BREATHWISE_SCHED_STANDARD_REQUIRED` (default `true`)
- `BREATHWISE_SCHED_FULL_EXPECTED_HOURS` (default `168`)
- `BREATHWISE_SCHED_FULL_MAX_STALE_HOURS` (default `240`)
- `BREATHWISE_SCHED_FULL_REQUIRED` (default `false`)

Status semantics:
- `ok`: cadence healthy.
- `late`: success exists but expected cadence exceeded.
- `stale`: max stale threshold exceeded.
- `missing_required`: no success found for required mode.
- `missing_optional`: no success found for optional mode.

## Log Hygiene and Retention
Pipeline run logs:
- bounded via run-directory pruning:
  - `--prune-old-runs --retention-days N`
- same retention pass also prunes old rows in `data/processed/pipeline_runs/history.jsonl`.

Operational log rotation:
- script:
```bash
python3 scripts/rotate_ops_logs.py --max-bytes 5000000 --retain-archives 14
```
- dry-run:
```bash
python3 scripts/rotate_ops_logs.py --dry-run
```
- default targets:
  - `data/processed/api_logs/api_access.jsonl`
  - `data/processed/alerts/alerts_history.jsonl`

Optional Linux logrotate template:
- `ops/logrotate/breathwise-ops.conf`

## Lightweight Ops Visibility
Generated status file:
- `data/processed/ops/ops_status_latest.json`

CLI status summary:
```bash
python3 scripts/ops_status.py --format text --write-file
python3 scripts/ops_status.py --format json
```

API visibility endpoint (auth-protected):
- `GET /v1/meta/ops-status`
- `GET /v1/meta/ops-status?compact=true`
- `GET /v1/meta/ops-status?publish_safe=true` (redacts absolute local filesystem paths)

Status includes:
- latest run summary
- latest successful run summary
- recent failures summary
- last validation summary
- pipeline lock visibility
- scheduler cadence health and freshness summaries
- latest alert and alert-state summaries

## API Runtime Hardening
Security:
- `BREATHWISE_API_AUTH_ENABLED` (`true|false`, default `false`)
- `BREATHWISE_API_KEY`
- `BREATHWISE_API_KEY_HEADER_NAME` (default `X-API-Key`)

Rate limiting:
- `BREATHWISE_RATE_LIMIT_ENABLED`
- `BREATHWISE_RATE_LIMIT_REQUESTS`
- `BREATHWISE_RATE_LIMIT_WINDOW_SECONDS`

Caching:
- `BREATHWISE_API_CACHE_ENABLED`
- `BREATHWISE_API_CACHE_TTL_SECONDS`
- `BREATHWISE_API_CACHE_MAX_ENTRIES`

Access logs:
- `BREATHWISE_API_ACCESS_LOG_ENABLED`
- `BREATHWISE_API_ACCESS_LOG_PATH`

CORS (for Expo web / browser access):
- `BREATHWISE_API_CORS_ENABLED` (default `true`)
- `BREATHWISE_API_CORS_ALLOWED_ORIGINS` (comma-separated explicit origins)
- `BREATHWISE_API_CORS_ALLOW_PRIVATE_NETWORK_ORIGINS` (default `true`)
- `BREATHWISE_API_CORS_ALLOWED_METHODS` (default `GET,OPTIONS`)
- `BREATHWISE_API_CORS_ALLOWED_HEADERS` (includes `X-API-Key` and `Authorization`)
- `BREATHWISE_API_CORS_EXPOSE_HEADERS`
- `BREATHWISE_API_CORS_ALLOW_CREDENTIALS` (default `false`)
- `BREATHWISE_API_CORS_MAX_AGE_SECONDS` (default `600`)

Path behavior:
- Relative env paths resolve against project root.

## Health, Readiness, and Metadata
- `GET /health`
- `GET /health?publish_safe=true` (redacts absolute local filesystem path fields)
- `GET /ready`
- `GET /v1/meta/datasets`
- `GET /v1/meta/ops-status`
- `GET /v1/meta/localization`

Publish-safe metadata mode:
- `GET /v1/meta/datasets?publish_safe=true`
- `GET /v1/meta/localization?publish_safe=true`
- `GET /v1/meta/ops-status?publish_safe=true`
- behavior: absolute machine paths are redacted or converted to project-relative paths for shareable outputs.

Operational response headers:
- `X-Request-Id`
- `X-Process-Time-Ms`
- `X-RateLimit-*` (when enabled)

## Localization Readiness (TR/EN)
Exported contract:
- `data/contracts/i18n_contract.json`

Notes:
- supported locales are `tr-TR` and `en-US`.
- deliver user-facing labels as stable translation keys when possible.
- client layers resolve keys to localized strings using this contract.

## Troubleshooting
1. Check latest run pointer:
```bash
cat data/processed/pipeline_runs/latest_run_manifest.json
```
2. Open failing run manifest:
```bash
cat data/processed/pipeline_runs/<run_id>/run_manifest.json
```
3. Inspect step logs:
```bash
ls data/processed/pipeline_runs/<run_id>/
```
4. Re-run failed flow from failure point:
```bash
python3 scripts/run_pipeline.py --mode standard --resume-from-latest-failed
```
5. Validate outputs:
```bash
python3 scripts/validate_analytics_outputs.py
```
6. Check ops snapshot:
```bash
python3 scripts/ops_status.py --format text
```

## Shared-Mode Safety
- Keep auth enabled for shared usage:
  - `BREATHWISE_API_AUTH_ENABLED=true`
  - `BREATHWISE_API_KEY=<strong-random-value>`
- Keep external service credentials environment-based only:
  - `OPENAI_API_KEY`
- Never commit secrets to tracked files.
- Rotate API keys via environment/secret manager only.
- Keep rate limiting enabled in shared mode to avoid accidental abuse.

## Public/Sanitized Export
Use the standardized bundle generator before external sharing:
```bash
python3 scripts/export_publish_bundle.py --tag release_candidate --overwrite
```
Output directory:
- `data/processed/publish_exports/<tag>/`

Bundle includes:
- public-safe docs
- sanitized contracts/metadata/manifests
- checksum index (`bundle_manifest.json`)

Share policy:
- share from `data/processed/publish_exports/<tag>/` only
- do not share unsanitized `data/raw/**` or `data/processed/pipeline_runs/**` directly
