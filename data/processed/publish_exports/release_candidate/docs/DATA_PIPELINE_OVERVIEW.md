# Data Pipeline Overview

## Project
**Breathwise TR**

This document explains how raw files move through the project, how datasets are transformed, what each stage produces, and how the different sources are connected into a unified environmental intelligence pipeline.

---

# 1. Pipeline Purpose

The project uses a multi-source data pipeline to build a city-level environmental intelligence layer for Turkey.

The pipeline is designed to support:

- current and forecast weather analytics
- current and short-term air quality intelligence
- historical weather analysis
- historical forecast reliability evaluation
- historical air quality reanalysis
- province-level map integration
- dashboard, app, and modeling layers

---

# 2. High-Level Pipeline Flow

The data pipeline follows this structure:

    External Sources
        ↓
    Raw Acquisition
        ↓
    Tidy / Extracted City-Level Outputs
        ↓
    Processed Geography and Reanalysis Layers
        ↓
    Joinable City-Time Analytical Layer
        ↓
    Dashboards / App / Modeling / Agents

---

# 3. Source Systems

The pipeline is built from the following source systems:

- Open-Meteo Geocoding API
- Open-Meteo Weather Forecast API
- Open-Meteo Air Quality API
- Open-Meteo Historical Weather API
- Open-Meteo Historical Forecast API
- geoBoundaries Turkey
- CAMS European Air Quality Reanalyses

---

# 4. Directory Structure

## Raw Layer

    data/raw/open_meteo/geocoding/
    data/raw/open_meteo/forecast/
    data/raw/open_meteo/air_quality/
    data/raw/open_meteo/historical_weather/
    data/raw/open_meteo/historical_forecast/
    data/raw/open_meteo/historical_forecast/raw_json/experimental/
    data/raw/open_meteo/historical_forecast/raw_json/validated/
    data/raw/open_meteo/historical_forecast/raw_json/canonical/
    data/raw/open_meteo/historical_forecast/tidy/experimental/
    data/raw/open_meteo/historical_forecast/tidy/validated/
    data/raw/open_meteo/historical_forecast/tidy/canonical/
    data/raw/open_meteo/historical_forecast/manifests/
    data/raw/geoboundaries/
    data/raw/cams/

## Processed Layer

    data/processed/geography/
    data/processed/cams/
    data/processed/marts/
    data/processed/views/
    data/contracts/
    data/db/breathwise_tr.duckdb
    backend/

---

# 5. Raw Acquisition Layer

## 5.1 Open-Meteo Geocoding

### Input
- province names

### Process
- geocode Turkish provinces
- cache location metadata
- write city dimension

### Outputs
- cached geocoding JSON files
- `dim_city_*.csv`

### Role
Provides the canonical city coordinate layer for all API requests.

---

## 5.2 Open-Meteo Forecast

### Input
- province coordinates from geocoding

### Process
- request current weather
- request hourly forecast
- request daily forecast
- save raw JSON
- save tidy CSV outputs

### Outputs
- `forecast_current_tr_*.csv`
- `forecast_hourly_tr_*.csv`
- `forecast_daily_tr_*.csv`

### Role
Provides current and near-term forecast weather layer.

---

## 5.3 Open-Meteo Air Quality

### Input
- province coordinates from geocoding

### Process
- request current AQ
- request hourly AQ
- include European AQI fields
- save raw JSON
- save tidy CSV outputs

### Outputs
- `air_quality_current_tr_*.csv`
- `air_quality_hourly_tr_*.csv`

### Role
Provides short-term pollution and AQI layer.

---

## 5.4 Open-Meteo Historical Weather

### Input
- province coordinates from geocoding

### Process
- request 2024 historical hourly weather
- use chunked request strategy for stability
- save raw JSON chunks
- assemble tidy hourly historical weather output

### Outputs
- `historical_weather_hourly_tr_2024_*.csv`

### Role
Provides actual historical weather context.

---

## 5.5 Open-Meteo Historical Forecast

### Input
- province coordinates from geocoding

### Process
- staged validation using:
  - 1 city × 1 day
  - 3 cities × 3 days
  - 10 cities × 7 days
  - 81 cities × 7 days light batch
  - 81 cities × 7 days full batch
- separate outputs by reliability tier:
  - `raw_json/experimental/`
  - `raw_json/validated/`
  - `raw_json/canonical/`
  - `tidy/experimental/`
  - `tidy/validated/`
  - `tidy/canonical/`

### Outputs
Canonical short-range validated set:
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_01_15_2024_01_21_light.csv`
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_01_15_2024_01_21_full.csv`

Extended consolidated set:
- `data/raw/open_meteo/historical_forecast/tidy/validated/historical_forecast_hourly_tr_2024_validated_monthly_full.csv`
- `data/raw/open_meteo/historical_forecast/tidy/validated/historical_forecast_hourly_tr_2024_validated_monthly_light.csv`
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_extended_full.csv`
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_extended_light.csv`

Supporting metadata:
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_canonical_short_range_manifest.csv`
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_folder_inventory.csv`
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_extended_manifest.json`
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_extended_monthly_coverage.csv`
- `data/raw/open_meteo/historical_forecast/README.md`

### Role
Provides archived forecast layer for forecast-vs-actual analysis.

---

## 5.6 geoBoundaries Turkey

### Input
- manually downloaded GeoJSON administrative boundaries

### Process
- download ADM0 / ADM1 / ADM2 raw files
- inspect feature counts and property structure
- standardize ADM1 province names
- build province dimension and processed ADM1 geometry

### Outputs
Raw:
- `turkey_adm0_country.geojson`
- `turkey_adm1_provinces.geojson`
- `turkey_adm2_districts.geojson`

Processed:
- `dim_province_tr.csv`
- `adm1_provinces_tr.geojson`

Snapshot metadata:
- `data/raw/geoboundaries/SOURCE_METADATA.md`

### Role
Provides spatial geometry for maps and province joins.

---

## 5.7 CAMS Reanalysis

### Input
- manually requested and downloaded NetCDF files from ADS

### Process
- retrieve available month-level interim ensemble surface fields
- keep six pollutant variables in raw NetCDF
- sample the nearest CAMS grid cell for each Turkish city coordinate
- assemble month-level and combined city-hourly tidy outputs

### Outputs
Raw:
- NetCDF files under `data/raw/cams/YYYY_MM_interim_surface_ensemble/` (`2024_01` to `2024_12` currently present)

Processed:
- `cams_city_hourly_tr_YYYY_MM.csv` (one file per available month)
- `cams_city_hourly_tr_all_available.csv`
- `cams_city_hourly_manifest.json`

### Role
Provides historical reanalysis-grade pollution layer.

---

# 6. Processed Layers

## 6.1 Geography Processed Layer

### Files
- `data/processed/geography/dim_province_tr.csv`
- `data/processed/geography/adm1_provinces_tr.geojson`

### Purpose
This layer standardizes geography and prepares ADM1 boundaries for joins and mapping.

### Key Standardization
Province names were normalized to align with Open-Meteo city naming:
- `Canakkale -> Çanakkale`
- `Istanbul -> İstanbul`
- `Izmir -> İzmir`

---

## 6.2 CAMS Processed Layer

### File
- `data/processed/cams/cams_city_hourly_tr_2024_01.csv`
- `data/processed/cams/cams_city_hourly_tr_all_available.csv`
- `data/processed/cams/cams_city_hourly_manifest.json`

### Purpose
Transforms large gridded NetCDF air quality reanalysis files into a city-level hourly analytical table.

### Why This Matters
Flattening the full CAMS grid would create a very large dataset.  
Sampling the nearest grid point for each city creates a tractable city-level environmental layer aligned with the rest of the project.

---

## 6.3 Analytics Marts and Views

### Marts
- `data/processed/marts/city_hourly_environment_tr.parquet`
- `data/processed/marts/city_forecast_vs_actual_tr.parquet`
- `data/processed/marts/city_current_snapshot_tr.parquet`
- `data/processed/marts/province_map_metrics_tr.parquet`

### Mobile Views
- `data/processed/views/mobile_city_current_snapshot_tr_light.parquet`
- `data/processed/views/mobile_city_hourly_timeline_tr_light.parquet`
- `data/processed/views/mobile_province_map_metrics_tr_light.parquet`

### Build and Validation Metadata
- `data/processed/marts/marts_build_manifest.json`
- `data/processed/marts/validation_report.json`

### Data Contracts
- `data/contracts/parquet_contracts.json`
- `data/contracts/api_*.schema.json`

---

# 7. Join Strategy

## 7.1 Primary Analytical Grain

The main analytical grain is:

`city_name + time`

This is the intended join key for:

- forecast hourly
- air quality hourly
- historical weather hourly
- historical forecast hourly
- CAMS city-hourly output

---

## 7.2 Geographic Join Layer

Province map joins are handled through:

- `province_name`
- standardized naming
- `shape_iso` if needed for province shape joins

Primary geometry file:
- `adm1_provinces_tr.geojson`

---

## 7.3 City Dimension

The city dimension comes from geocoding outputs and should serve as the canonical city key source.

Expected fields include:
- `city_name`
- `latitude`
- `longitude`
- `timezone`
- `admin1`
- `country_code`

---

# 8. Transformation Logic by Dataset

## 8.1 Forecast Transformation
Raw forecast API responses are transformed into separate current, hourly, and daily tidy outputs.

## 8.2 Air Quality Transformation
Raw AQ API responses are transformed into current and hourly city-level outputs including AQI fields.

## 8.3 Historical Weather Transformation
Chunked hourly historical weather responses are combined into a single historical city-hour table.

## 8.4 Historical Forecast Transformation
Validated monthly historical forecast raw files are consolidated into extended tidy outputs, then merged with canonical short-range validated rows to preserve quality precedence on overlap windows.

## 8.5 Geography Transformation
Raw ADM1 boundaries are standardized and enriched into join-friendly geometry and province dimension outputs.

## 8.6 CAMS Transformation
Each pollutant NetCDF is sampled at the nearest grid point for each city coordinate, then merged into a city-hourly pollutant table.

---

# 9. Quality and Validation Checks Already Performed

The following checks were explicitly performed during setup:

## Open-Meteo
- province count verification
- city count verification
- hourly output row checks
- historical weather completeness checks
- historical forecast API staged validation
- city coordinate override corrections

## geoBoundaries
- ADM0 feature count = 1
- ADM1 feature count = 81
- ADM2 feature count = 973
- province naming mismatch identification and correction
- snapshot checksums pinned in `data/raw/geoboundaries/SOURCE_METADATA.md`

## CAMS
- six NetCDF files validated
- dimensions verified:
  - `time = 744`
  - `lat = 420`
  - `lon = 700`
- data variables verified
- final city-hourly output validated:
  - `81 cities × 744 hours = 60264 rows`
  - no missing values in pollutant fields

---

# 10. Current Pipeline Completion Status

## Completed
- geocoding acquisition and city dimension
- forecast acquisition and tidy outputs
- air quality acquisition and tidy outputs
- historical weather acquisition and tidy outputs
- geography acquisition and processing
- CAMS full 2024 monthly reanalysis acquisition and city extraction
- historical forecast canonical short-range validated set
- historical forecast extended consolidated 2024 set (`2024-01-01` to `2024-12-31`)
- city_hourly_environment_tr mart
- city_forecast_vs_actual_tr mart
- city_current_snapshot_tr mart
- province_map_metrics_tr mart
- mobile-ready lightweight parquet views
- DuckDB mart build + validation pipeline
- incremental mart refresh mode (`auto/full/incremental`)
- FastAPI local backend endpoints and JSON data contracts
- orchestration entrypoint (`scripts/run_pipeline.py`) with mode-based execution
- structured pipeline run manifests and step logs under `data/processed/pipeline_runs/`
- API hardening layer (optional auth, optional rate limiting, response cache, readiness endpoint)
- scheduler-ready execution wrapper (`scripts/run_scheduled_pipeline.sh`)
- scheduler templates for cron/launchd/systemd under `ops/scheduler/`
- local-first run alert hooks (file sink + optional webhook/mac notify)
- log hygiene tooling (`scripts/rotate_ops_logs.py`) and logrotate template (`ops/logrotate/`)
- lightweight ops visibility (`scripts/ops_status.py`, `data/processed/ops/ops_status_latest.json`, `GET /v1/meta/ops-status`)
- scheduler cadence health checks (`scripts/check_scheduler_health.py`) with stale/missed-run detection
- localization-ready contract export (`data/contracts/i18n_contract.json`) for TR/EN user-facing keys
- localization metadata endpoint (`GET /v1/meta/localization`) and locale-aware app-facing payload enrichment
- product-shell mapping contract export (`data/contracts/product_shell_view_models.json`)
- runnable Expo mobile shell scaffold (`frontend/mobile_shell_starter/`)

# 11. Recommended Next Pipeline Steps

## Immediate Next Step
Keep operational baseline healthy and extend coverage safely:

- activate one scheduler path in real use (cron or launchd/systemd)
- enable failure alerts in shared usage (`BREATHWISE_ALERTS_ENABLED=true`)
- keep retention and log rotation active in recurring runs
- monitor `ops_status_latest.json` and `/v1/meta/ops-status` regularly
- monitor scheduler cadence with `scripts/check_scheduler_health.py` (non-zero exit on unhealthy)
- keep localization contract (`data/contracts/i18n_contract.json`) synced with frontend/mobile bundles

## Next After That
Expand historical depth and product metrics:
- extend CAMS raw coverage beyond current months and rebuild processed city-hourly outputs
- add additional historical forecast years beyond 2024 with the same canonical validation rules
- add reliability and anomaly features to city/province marts

## Additional Expansion
- add model-ready training datasets
- add externalized monitoring/alert routing (Slack/email/on-call) when moving beyond local-first operation

---

# 12. Pipeline Principles

The project follows these pipeline principles:

- raw files are preserved as source-of-truth artifacts
- processed files are explicit derived layers
- joins are based on transparent and stable keys
- geography normalization happens before map joins
- rate-limited APIs should be expanded in controlled batches
- large gridded scientific data is sampled into tractable analytical outputs when appropriate

---

# 13. Operational Notes

- Historical Forecast API requires rate-limit-aware extraction planning.
- CAMS NetCDF files should remain archived even after city-level extraction.
- Province and city naming should not be changed ad hoc outside documented standardization rules.
- Processed geography outputs should be treated as shared dimensions.
- Future joins should prefer standardized names over raw names.

---

# 14. Suggested Future Documentation

After this file, recommended next project documents are:

- `ANALYTICS_LAYER.md`
- `PROJECT_STATE_AUDIT.md`
- `DATA_DICTIONARY.md`
- `JOIN_STRATEGY.md`
- `LOCALIZATION_STRATEGY.md`
- `PRODUCT_SHELL_INTEGRATION.md`
- `FEATURE_STORE_PLAN.md`
- `MODELING_DATA_SPEC.md`

---
