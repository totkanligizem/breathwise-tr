# Scripts

Core project automation lives here: ingestion, processing, marts, validation, pipeline orchestration, and ops checks.

## Main Pipelines
- `run_pipeline.py`: orchestration entrypoint (incremental/standard/full, lock, manifests, alerts, retention)
- `build_analytics_marts.py`: marts + light views build
- `validate_analytics_outputs.py`: strict validation checks
- `build_historical_forecast_extended.py`: extended canonical historical forecast build
- `extract_cams_city_hourly_tr.py`: CAMS monthly extraction to city-hourly format

## Fetch/Acquisition
- `fetch_open_meteo_forecast_tr.py`
- `fetch_open_meteo_air_quality_tr.py`
- `fetch_open_meteo_historical_weather_tr.py`
- `fetch_open_meteo_historical_forecast_tr.py`
- `fetch_open_meteo_historical_forecast_backfill_tr.py`

## Ops/Health Utilities
- `check_scheduler_health.py`
- `ops_status.py`
- `rotate_ops_logs.py`
- `smoke_test_api.py`
- `run_scheduled_pipeline.sh`
- `export_publish_bundle.py` (public-safe export bundle generator)

## Typical Commands
```bash
python3 scripts/run_pipeline.py --mode incremental
python3 scripts/validate_analytics_outputs.py
python3 scripts/check_scheduler_health.py --format text
python3 scripts/smoke_test_api.py
python3 scripts/export_publish_bundle.py --tag release_candidate --overwrite
```

## Canonical Release Gate
Before share/export:
```bash
pytest -q
python3 scripts/validate_analytics_outputs.py
python3 scripts/check_scheduler_health.py --format text
cd frontend/mobile_shell_starter && npm run -s typecheck && cd ../..
```

Then export:
```bash
python3 scripts/export_publish_bundle.py --tag release_candidate --overwrite
```

Share path:
- `data/processed/publish_exports/<tag>/` only

Reference:
- `RELEASE_WORKFLOW.md`

## Principles
- Local-first by default
- Deterministic outputs
- Strict validation before publish/use
- No hardcoded secrets
