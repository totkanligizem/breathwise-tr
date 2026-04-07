# Analytics Layer

## Scope
This document defines the local-first analytics and API layer for Breathwise TR.

Stack:
- DuckDB for transformations and query execution
- Parquet for marts and lightweight app views
- FastAPI for backend-ready serving

## Build Order
From project root:

```bash
python3 scripts/run_pipeline.py --mode standard
```

Fallback manual sequence:

```bash
python3 scripts/build_historical_forecast_extended.py --year 2024
python3 scripts/extract_cams_city_hourly_tr.py
python3 scripts/build_analytics_marts.py --refresh-mode auto
python3 scripts/validate_analytics_outputs.py
python3 scripts/export_data_contracts.py
```

Optional explicit full rebuild:

```bash
python3 scripts/run_pipeline.py --mode full
```

Optional explicit incremental rebuild window:

```bash
python3 scripts/run_pipeline.py --mode incremental --incremental-lookback-hours 96
```

Scheduler wrapper (recommended for cron/launchd/systemd):

```bash
bash scripts/run_scheduled_pipeline.sh incremental
```

Optional local API run:

```bash
uvicorn backend.main:app --reload
```

## Produced Marts
Output directory: `data/processed/marts/`

- `city_hourly_environment_tr.parquet`
  - Unified city-hourly environmental table.
  - Source prefixes preserve semantics:
    - `hw_*`: historical weather (actual)
    - `hf_*`: historical forecast (archived forecast)
    - `fw_*`: weather forecast hourly
    - `aq_*`: air quality hourly
    - `cams_*`: CAMS reanalysis

- `city_forecast_vs_actual_tr.parquet`
  - Forecast vs actual comparison table at city-hourly grain.
  - Includes signed and absolute error columns.
  - `forecast_validation_window` stores the historical forecast source label (for example `2024_extended`).

- `city_current_snapshot_tr.parquet`
  - City-level current snapshot for dashboard/app serving.

- `province_map_metrics_tr.parquet`
  - Province-level aggregates ready for ADM1 joins on `province_name`.

Build metadata:
- `marts_build_manifest.json`
- `validation_report.json`

Pipeline run metadata:
- `data/processed/pipeline_runs/<run_id>/run_manifest.json`
- `data/processed/pipeline_runs/<run_id>/events.jsonl`
- `data/processed/pipeline_runs/history.jsonl`
- `data/processed/pipeline_runs/latest_run_manifest.json`
- `data/processed/pipeline_runs/latest_success_run_manifest.json`
- `data/processed/ops/ops_status_latest.json`

Operational run management notes:
- runner uses a lock guard (`data/processed/pipeline_runs/pipeline.lock`) to avoid overlapping runs by default.
- optional cleanup: `--prune-old-runs --retention-days <N>`.
- alert hooks (local file/webhook/mac notification optional) are emitted at run finalization when enabled.

## Mobile Views
Output directory: `data/processed/views/`

- `mobile_city_current_snapshot_tr_light.parquet`
- `mobile_city_hourly_timeline_tr_light.parquet`
- `mobile_province_map_metrics_tr_light.parquet`

## Historical Forecast and CAMS Intermediate Outputs
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_extended_full.csv`
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_extended_manifest.json`
- `data/processed/cams/cams_city_hourly_tr_all_available.csv`
- `data/processed/cams/cams_city_hourly_manifest.json`

## Data Contracts
Output directory: `data/contracts/`

- API JSON schemas (Pydantic-derived)
- `parquet_contracts.json` (column-level parquet contracts)
- `i18n_contract.json` (TR/EN localization-ready translation key contract)
- `product_shell_view_models.json` (screen-level product-facing mapping contract)
- localization strategy reference: `LOCALIZATION_STRATEGY.md`
- product shell integration reference: `PRODUCT_SHELL_INTEGRATION.md`

## FastAPI Endpoints
- `GET /health`
- `GET /ready`
- `GET /v1/meta/datasets`
- `GET /v1/meta/ops-status`
- `GET /v1/meta/localization`
- `GET /v1/cities/current`
- `GET /v1/cities/{city_name}/hourly`
- `GET /v1/provinces/map-metrics`
- `GET /v1/mobile/cities/current`
- `GET /v1/mobile/cities/{city_name}/timeline`
- `GET /v1/mobile/provinces/map-metrics`

Localization-aware endpoint behavior:
- `locale` query parameter supported on:
  - `/v1/cities/current`
  - `/v1/mobile/cities/current`
  - `/v1/provinces/map-metrics`
  - `/v1/mobile/provinces/map-metrics`
- response header `Content-Language` reflects resolved locale.

## Environment Configuration
No secrets are hardcoded.

Supported environment variables:
- `BREATHWISE_PROJECT_ROOT`
- `BREATHWISE_MARTS_DIR`
- `BREATHWISE_VIEWS_DIR`
- `BREATHWISE_DUCKDB_PATH`
- `BREATHWISE_API_AUTH_ENABLED`
- `BREATHWISE_API_KEY`
- `BREATHWISE_API_KEY_HEADER_NAME`
- `BREATHWISE_RATE_LIMIT_ENABLED`
- `BREATHWISE_RATE_LIMIT_REQUESTS`
- `BREATHWISE_RATE_LIMIT_WINDOW_SECONDS`
- `BREATHWISE_API_CACHE_ENABLED`
- `BREATHWISE_API_CACHE_TTL_SECONDS`
- `BREATHWISE_API_CACHE_MAX_ENTRIES`
- `BREATHWISE_API_ACCESS_LOG_ENABLED`
- `BREATHWISE_API_ACCESS_LOG_PATH`
- `BREATHWISE_ALERTS_ENABLED`
- `BREATHWISE_ALERT_ON_FAILURE`
- `BREATHWISE_ALERT_ON_SUCCESS`
- `BREATHWISE_ALERTS_DIR`
- `BREATHWISE_ALERT_WEBHOOK_URL`
- `BREATHWISE_ALERT_WEBHOOK_TIMEOUT_SECONDS`
- `BREATHWISE_ALERT_WEBHOOK_RETRIES`
- `BREATHWISE_ALERT_WEBHOOK_BACKOFF_SECONDS`
- `BREATHWISE_ALERT_DEDUP_WINDOW_MINUTES`
- `BREATHWISE_ALERT_REPEAT_EVERY_FAILURES`
- `BREATHWISE_ALERT_STATE_PATH`
- `BREATHWISE_ALERT_MAC_NOTIFY`
- `BREATHWISE_SCHED_INCREMENTAL_EXPECTED_HOURS`
- `BREATHWISE_SCHED_INCREMENTAL_MAX_STALE_HOURS`
- `BREATHWISE_SCHED_INCREMENTAL_REQUIRED`
- `BREATHWISE_SCHED_STANDARD_EXPECTED_HOURS`
- `BREATHWISE_SCHED_STANDARD_MAX_STALE_HOURS`
- `BREATHWISE_SCHED_STANDARD_REQUIRED`
- `BREATHWISE_SCHED_FULL_EXPECTED_HOURS`
- `BREATHWISE_SCHED_FULL_MAX_STALE_HOURS`
- `BREATHWISE_SCHED_FULL_REQUIRED`
- `OPENAI_API_KEY` (future agent/model workflows; keep env-only)

Path resolution rule:
- If these path env vars are provided as relative paths, they are resolved against project root.

API protection notes:
- `/v1/*` endpoints can be protected with API key auth.
- Accepted formats: `X-API-Key` header or `Authorization: Bearer <key>`.

Localization notes:
- user-facing product contracts should carry stable translation keys.
- supported locales: `tr-TR` and `en-US`.
- reference contract: `data/contracts/i18n_contract.json`.
- localization endpoint for runtime inspection: `/v1/meta/localization`.
- do not store any API key or secret in repo files; use environment variables only.

## Grain and Join Rules
- Hourly analytical joins: `city_name + time`
- Current snapshot joins: `city_name`
- Province geometry joins: `province_name`

Do not:
- merge forecast and actual values without explicit prefixes
- join raw city-hourly fact tables directly to geometry
- mix hourly and daily grains without explicit aggregation
