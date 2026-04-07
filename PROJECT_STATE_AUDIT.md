# Project State Audit

Audit date (UTC): 2026-04-06
Project: Breathwise TR

## 1. Scope of Audit
The repository was inspected in-place and extended without resetting existing structure.

Reviewed areas:
- source data layers under `data/raw/`
- processed geography and CAMS outputs
- historical forecast canonical/validated/experimental layout
- existing ingestion/processing scripts
- core project documentation

## 2. Current Data Foundation Status
Confirmed available and usable:
- Open-Meteo geocoding outputs (`81` cities)
- Open-Meteo forecast current/hourly/daily tidy outputs
- Open-Meteo air quality current/hourly tidy outputs
- Open-Meteo historical weather hourly output (`2024`)
- Open-Meteo historical forecast canonical short-range full/light outputs (`2024-01-15` to `2024-01-21`)
- Open-Meteo historical forecast extended consolidated output (`2024-01-01` to `2024-12-31`)
- geoBoundaries ADM0/ADM1/ADM2 raw snapshots + processed ADM1 layer
- CAMS 2024 monthly city-hourly processed outputs (`2024_01` to `2024_12`) + combined output (`cams_city_hourly_tr_all_available.csv`)

## 3. Hardening Status
Implemented:
- portable project-root discovery (`discover_project_root`)
- standardized ASCII-safe slug generation (`slugify_ascii`)
- historical forecast tiered folder strategy (`experimental/validated/canonical`)
- canonical short-range manifest + inventory manifest
- historical forecast extended consolidation pipeline + manifests
- mart input fingerprints with content hash (`sha256`) for safer incremental/full mode decisions
- geoBoundaries source metadata and checksum pinning notes
- documentation cleanup for path consistency and current canonical references

## 4. New Analytical Layer (Implemented)
Built with DuckDB + Parquet:
- `data/processed/marts/city_hourly_environment_tr.parquet`
- `data/processed/marts/city_forecast_vs_actual_tr.parquet`
- `data/processed/marts/city_current_snapshot_tr.parquet`
- `data/processed/marts/province_map_metrics_tr.parquet`

Mobile-ready views:
- `data/processed/views/mobile_city_current_snapshot_tr_light.parquet`
- `data/processed/views/mobile_city_hourly_timeline_tr_light.parquet`
- `data/processed/views/mobile_province_map_metrics_tr_light.parquet`

Build metadata:
- `data/processed/marts/marts_build_manifest.json`
- `data/processed/marts/validation_report.json`

## 5. Contract and Backend Readiness
Implemented:
- FastAPI backend under `backend/`
- env-based configuration only (no hardcoded secrets)
- API and parquet contracts exported to `data/contracts/`
- TR/EN localization contract exported to `data/contracts/i18n_contract.json`
- optional API key auth and rate-limiting controls (env-driven)
- endpoint response cache with TTL controls (env-driven)
- readiness endpoint (`/ready`) and structured error responses

Key API endpoints:
- `/health`
- `/v1/meta/datasets`
- `/v1/meta/localization`
- `/v1/cities/current`
- `/v1/cities/{city_name}/hourly`
- `/v1/provinces/map-metrics`
- `/v1/mobile/...` endpoints for light views

## 6. Validation Results
From generated reports and checks:
- mart build completed successfully
- validation checks passed (`41/41`)
- pytest passed (`33` tests)
- API smoke checks returned `200` for main endpoints

Key output stats:
- `city_hourly_environment_tr`: `756216` rows, `81` cities, `0` duplicate `city_name + time`
- `city_forecast_vs_actual_tr`: `711504` rows, `81` cities, `2024-01-01` to `2024-12-31`
- `city_current_snapshot_tr`: `81` rows (one per city)
- `province_map_metrics_tr`: `81` rows (one per province)

## 7. Open Expansion Tracks
Not blockers for current local-first v1:
- CAMS years beyond `2024`
- historical forecast years beyond `2024`
- deployment-grade external monitoring/alerts beyond local-first hooks
- full React Native UI implementation (shell contract and starter now available)

## 8. Operational Maturity Layer (Current)
Implemented in this phase:
- `scripts/run_pipeline.py` orchestration entrypoint (`full/standard/incremental`)
- step-level run logs + structured manifests under `data/processed/pipeline_runs/`
- latest-run and latest-success pointers for operational traceability
- API smoke script (`scripts/smoke_test_api.py`) for quick reliability checks
- scheduler wrapper (`scripts/run_scheduled_pipeline.sh`) and cron/launchd/systemd templates under `ops/scheduler/`
- run alert hooks (local file sink + optional webhook/mac notification)
- scheduler cadence health checks (`scripts/check_scheduler_health.py`)
- operational log rotation tooling (`scripts/rotate_ops_logs.py`) + logrotate template
- lightweight ops visibility snapshots (`scripts/ops_status.py`, `data/processed/ops/ops_status_latest.json`)
- API metadata visibility endpoint (`/v1/meta/ops-status`)
- localization-ready TR/EN contract export (`data/contracts/i18n_contract.json`)
- locale-aware app-facing payload enrichment (`aq_category_key/label`, alert key/label fields)
- product shell integration guide and starter structure (`PRODUCT_SHELL_INTEGRATION.md`, `frontend/mobile_shell_starter/`)
- runnable Expo mobile shell scaffold under `frontend/mobile_shell_starter/`
