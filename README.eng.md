# Breathwise TR

Turkey-wide environmental intelligence platform (local-first): analytics data layer, FastAPI services, and bilingual (TR/EN) mobile-web product shell.

Turkish README: [README.md](README.md)

## Executive Summary
- **What:** A production-minded data+API system that unifies weather, air quality, historical actuals, archived forecasts, CAMS reanalysis, and province geometry.
- **Delivers:** City/province marts, app-ready lightweight views, auth-protected APIs, schedulable pipelines, and operational visibility.
- **State:** Validation/test baseline is green; local-first architecture is stable and disciplined.
- **Principle:** Forecast vs actual semantics are explicitly separated; joins are deterministic and reproducible.
- **Security:** Environment-only secrets, publish-safe surfaces, and sanitized export as canonical sharing path.

## Scope, Purpose, Goals
- **Scope:** 81 provinces of Turkey; city-level and province-level analytics.
- **Purpose:** Reliable data contracts for dashboards, mobile/web product surfaces, and model/agent workflows.
- **Goals:**
  - Unified city-hourly environmental intelligence
  - Forecast-vs-actual reliability analytics
  - Province map metrics
  - Repeatable operational refresh

## Integrated Data Sources
- Open-Meteo Geocoding (81 provinces)
- Open-Meteo Forecast
- Open-Meteo Air Quality
- Open-Meteo Historical Weather
- Open-Meteo Historical Forecast (2024 extended canonical)
- geoBoundaries Turkey ADM0/ADM1/ADM2
- CAMS reanalysis (monthly 2024 coverage)

## Technology Stack
- **Languages:** Python, TypeScript
- **Analytics:** DuckDB, Parquet, Pandas, PyArrow
- **Geospatial:** GeoPandas, Shapely, PyProj, Fiona
- **Atmospheric files:** NetCDF4, Xarray
- **Backend:** FastAPI, Uvicorn
- **Frontend/Mobile:** Expo, React Native, React Native Web, React Native SVG
- **Quality:** Pytest, TypeScript typecheck
- **Ops:** cron/launchd/systemd templates, logrotate, run manifests

## Repository Structure
```text
breathwise-tr/
  backend/                        # FastAPI service, config, schemas
  frontend/mobile_shell_starter/  # Runnable Expo app shell (TR/EN)
  scripts/                        # ETL, mart build, orchestrator, validation
  tests/                          # Backend/pipeline/ops regression tests
  data/
    raw/                          # Immutable source artifacts
    processed/                    # Marts, views, run/ops outputs
    contracts/                    # API/parquet/i18n/product contracts
  ops/                            # Scheduler and logrotate templates
  sql/                            # SQL helpers
  *.md                            # Project documentation
```

## Key Outputs
- `data/processed/marts/`
  - `city_hourly_environment_tr.parquet`
  - `city_forecast_vs_actual_tr.parquet`
  - `city_current_snapshot_tr.parquet`
  - `province_map_metrics_tr.parquet`
- `data/processed/views/`
  - `mobile_city_current_snapshot_tr_light.parquet`
  - `mobile_city_hourly_timeline_tr_light.parquet`
  - `mobile_province_map_metrics_tr_light.parquet`

## Quick Start
1. Python environment:
   - `python3 -m venv .venv && source .venv/bin/activate`
   - `pip install -r requirements.txt`
2. Validate analytics outputs:
   - `python3 scripts/validate_analytics_outputs.py`
3. Run API:
   - `uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000`
4. Run mobile/web shell:
   - `cd frontend/mobile_shell_starter`
   - `npm install && npm run start`

## Security and Share Discipline
- Never hardcode secrets.
- Use environment variables only (`OPENAI_API_KEY`, `BREATHWISE_API_KEY`, etc.).
- Do not commit `.env`; keep only `.env.example`.
- Use `publish_safe=true` for external metadata surfaces.
- Keep raw artifacts immutable; sanitize at presentation/export boundaries.

Standard sanitized export:
```bash
python3 scripts/export_publish_bundle.py --tag release_candidate --overwrite
```
Output: `data/processed/publish_exports/<tag>/`

**Canonical rule:** public/portfolio sharing must come from the sanitized publish bundle only.

## Documentation Map
- [DATA_PIPELINE_OVERVIEW.md](DATA_PIPELINE_OVERVIEW.md)
- [DATASET_CATALOG.md](DATASET_CATALOG.md)
- [DATA_DICTIONARY.md](DATA_DICTIONARY.md)
- [JOIN_STRATEGY.md](JOIN_STRATEGY.md)
- [ANALYTICS_LAYER.md](ANALYTICS_LAYER.md)
- [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md)
- [LOCALIZATION_STRATEGY.md](LOCALIZATION_STRATEGY.md)
- [PRODUCT_SHELL_INTEGRATION.md](PRODUCT_SHELL_INTEGRATION.md)
- [SECURITY.md](SECURITY.md)
- [THIRD_PARTY_SOURCES.md](THIRD_PARTY_SOURCES.md)
- [RELEASE_READINESS_CHECKLIST.md](RELEASE_READINESS_CHECKLIST.md)
- [RELEASE_WORKFLOW.md](RELEASE_WORKFLOW.md)
- [ROADMAP_2025_PLUS.md](ROADMAP_2025_PLUS.md)

## Short Next Plan
1. Keep sanitized export standardized in release workflow.
2. Extend 2025+ data coverage with same validation strictness.
3. Continue UI polish + product release checklist (store/share readiness).
