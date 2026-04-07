# Breathwise TR

Turkey-wide environmental intelligence platform (local-first) for analytics, API delivery, and bilingual (TR/EN) mobile/web product surfaces.

## Executive Summary
- **What it is:** A production-minded local data+API stack that unifies weather, air quality, historical actuals, archived forecasts, CAMS reanalysis, and province geometry.
- **What it delivers:** City/province analytical marts, app-ready lightweight views, FastAPI endpoints, scheduling and ops run management.
- **Current state:** Validated and stable baseline (tests, validations, API smoke, scheduler health).
- **Design principle:** Forecast and actual semantics are strictly separated; joins are deterministic and reproducible.
- **Security stance:** Environment-only secrets, auth-protected API mode, publish-safe metadata surfaces.

## Scope, Purpose, Goals
- **Project name:** Breathwise TR
- **Scope:** 81 Turkish provinces, city-level + province-level environmental intelligence.
- **Purpose:** Reliable data products for dashboards, mobile app shell, future web app, and model/agent workflows.
- **Primary goals:**
  - Unified hourly city intelligence
  - Forecast-vs-actual reliability analytics
  - Province map metrics for geospatial insights
  - Operationally repeatable local-first pipeline

## Core Datasets (Integrated)
- Open-Meteo Geocoding (81 provinces)
- Open-Meteo Forecast (current/hourly/daily)
- Open-Meteo Air Quality (current/hourly)
- Open-Meteo Historical Weather (hourly actuals)
- Open-Meteo Historical Forecast (extended 2024 canonical coverage)
- geoBoundaries Turkey ADM0/ADM1/ADM2
- CAMS interim reanalysis (monthly 2024 coverage)

## Technology Stack
- **Languages:** Python, TypeScript
- **Data/Analytics:** DuckDB, Parquet, Pandas, PyArrow
- **Geospatial:** GeoPandas, Shapely, PyProj, Fiona
- **Atmospheric files:** NetCDF4, Xarray
- **Backend:** FastAPI, Uvicorn
- **Frontend/Mobile:** Expo, React Native, React Native Web, React Native SVG
- **Validation/QA:** Pytest, TypeScript typecheck
- **Ops:** cron/launchd/systemd templates, logrotate template, run manifests

### Package Manifests (Exact)
- Python pinned dependencies: `requirements.txt`
- Frontend dependencies: `frontend/mobile_shell_starter/package.json`

## Repository Structure
```text
breathwise-tr/
  backend/                      # FastAPI service, config, schemas
  frontend/mobile_shell_starter/# Runnable Expo app shell (TR/EN)
  scripts/                      # Data ingestion, marts, pipeline orchestration, checks
  tests/                        # Backend/pipeline/ops regression tests
  data/
    raw/                        # Immutable source artifacts
    processed/                  # Marts, views, run outputs, ops snapshots
    contracts/                  # API/parquet/i18n/product contracts
  ops/                          # Scheduler + logrotate templates
  sql/                          # SQL helpers (if needed)
  *.md                          # System documentation
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
1. **Python env**
   - `python3 -m venv .venv && source .venv/bin/activate`
   - `pip install -r requirements.txt`
2. **Run validation + marts check**
   - `python3 scripts/validate_analytics_outputs.py`
3. **Run API**
   - `uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000`
4. **Run mobile/web shell (optional)**
   - `cd frontend/mobile_shell_starter`
   - `npm install && npm run start`

## Security and Publish Hygiene
- Never hardcode secrets (API keys, tokens, credentials).
- Use environment variables only (`OPENAI_API_KEY`, `BREATHWISE_API_KEY`, etc.).
- `.env` files are ignored; only `.env.example` templates are committed.
- Use `publish_safe=true` on metadata endpoints when sharing outputs externally.
- Raw source artifacts are immutable by policy; sanitize at presentation/export boundaries.

### Standard Sanitized Export
```bash
python3 scripts/export_publish_bundle.py --tag release_candidate --overwrite
```
Default output: `data/processed/publish_exports/<tag>/`

Canonical policy:
- Public/portfolio sharing is done from sanitized bundle outputs only.
- Raw repo folders are not the default public-share surface.

## Documentation Map
- Pipeline and refresh flow: [DATA_PIPELINE_OVERVIEW.md](DATA_PIPELINE_OVERVIEW.md)
- Dataset inventory: [DATASET_CATALOG.md](DATASET_CATALOG.md)
- Data dictionary: [DATA_DICTIONARY.md](DATA_DICTIONARY.md)
- Join semantics: [JOIN_STRATEGY.md](JOIN_STRATEGY.md)
- Analytics/API/ops layer: [ANALYTICS_LAYER.md](ANALYTICS_LAYER.md)
- Operations runbook: [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md)
- Localization strategy (TR/EN): [LOCALIZATION_STRATEGY.md](LOCALIZATION_STRATEGY.md)
- Product shell integration: [PRODUCT_SHELL_INTEGRATION.md](PRODUCT_SHELL_INTEGRATION.md)
- Security checklist: [SECURITY.md](SECURITY.md)
- Third-party source attribution: [THIRD_PARTY_SOURCES.md](THIRD_PARTY_SOURCES.md)
- Release checklist: [RELEASE_READINESS_CHECKLIST.md](RELEASE_READINESS_CHECKLIST.md)
- Canonical release workflow: [RELEASE_WORKFLOW.md](RELEASE_WORKFLOW.md)
- 2025+ expansion plan: [ROADMAP_2025_PLUS.md](ROADMAP_2025_PLUS.md)

## Short Roadmap (Next)
1. Province map interaction depth (kept lightweight and stable).
2. Additional multi-year partitions (2025+).
3. Extended CAMS horizon and operational monitoring polish.
4. Release packaging and deployment hardening (local-first baseline preserved).
