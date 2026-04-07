# Dataset Catalog

## Project
**Breathwise TR**

A structured catalog of all datasets collected, generated, and prepared for the project.  
This document explains where each dataset comes from, how it was acquired, what it contains, why it exists in the pipeline, and how it should be used.

---

# 1. Purpose of This Catalog

This catalog exists to make the data foundation of the project transparent, reproducible, and maintainable.

It documents:

- source systems and public datasets
- acquisition method
- storage format
- temporal and spatial grain
- output file locations
- intended use in analytics, modeling, dashboards, and app features
- key caveats and operational notes

---

# 2. Data Architecture Overview

The project currently uses a multi-source environmental intelligence stack built around:

- Open-Meteo APIs
- geoBoundaries Turkey administrative boundaries
- CAMS European Air Quality Reanalyses

The pipeline currently includes:

- raw API responses and downloaded files
- tidy city-level hourly/daily CSV outputs
- geography dimension files
- processed CAMS city-hourly output
- DuckDB-derived analytics marts in Parquet
- mobile-ready lightweight Parquet views
- API/parquet data contracts

---

# 3. Dataset Inventory Summary

| Source | Dataset | Status | Primary Use |
|---|---|---:|---|
| Open-Meteo | Geocoding API | Complete | Province coordinates and city dimension |
| Open-Meteo | Weather Forecast API | Complete | Current, hourly, and daily forecast weather |
| Open-Meteo | Air Quality API | Complete | Current and hourly city-level air quality |
| Open-Meteo | Historical Weather API | Complete | Historical actual weather conditions |
| Open-Meteo | Historical Forecast API | Extended 2024 canonical + validated tiers | Archived forecast data for forecast-vs-actual analysis |
| geoBoundaries | Turkey ADM0 / ADM1 / ADM2 | Complete | Country, province, and district geometry |
| CAMS ADS | European Air Quality Reanalyses | Complete for 2024 monthly coverage | Historical air quality reanalysis |
| Internal Derived | Analytics Marts (DuckDB + Parquet) | Complete (v1 local-first) | Unified analytics, app views, backend serving |

---

# 4. Source-by-Source Documentation

## 4.1 Open-Meteo Geocoding API

### Source
Open-Meteo Geocoding API

### Documentation
- https://open-meteo.com/en/docs/geocoding-api

### Acquisition Method
Python API requests were used to geocode all 81 Turkish provinces.  
A cache layer was created to avoid repeated geocoding calls and to stabilize province coordinates across pipelines.

### Main Outputs
Raw / cache:
- `data/raw/open_meteo/geocoding/*.json`

Tabular output:
- `data/raw/open_meteo/geocoding/dim_city_*.csv`

### Core Fields
- `city_name`
- `latitude`
- `longitude`
- `timezone`
- `country_code`
- `admin1`
- `population` if available

### Purpose
This dataset is the core geographic city dimension used to:
- call all Open-Meteo endpoints
- align city-level weather and air quality data
- support joins with geography and derived layers

### Notes
Several provinces required manual override handling due to geocoding inconsistencies.  
Province overrides are part of the project logic and should be preserved.

---

## 4.2 Open-Meteo Weather Forecast API

### Source
Open-Meteo Weather Forecast API

### Documentation
- https://open-meteo.com/en/docs

### Acquisition Method
Initial URL design and variable selection were validated through the Open-Meteo interface.  
A Python script then collected forecast data for all 81 provinces.

### Main Outputs
Raw:
- `data/raw/open_meteo/forecast/raw_json/*.json`

Tidy outputs:
- `data/raw/open_meteo/forecast/tidy/forecast_current_tr_*.csv`
- `data/raw/open_meteo/forecast/tidy/forecast_hourly_tr_*.csv`
- `data/raw/open_meteo/forecast/tidy/forecast_daily_tr_*.csv`

### Grain
- Current snapshot
- Hourly forecast
- Daily forecast

### Typical Variables
- temperature
- relative humidity
- dew point
- precipitation
- cloud cover
- wind
- pressure
- visibility
- UV
- weather code

### Purpose
Used for:
- current conditions
- short-term app forecast layer
- dashboard forecast panels
- environmental feature generation

---

## 4.3 Open-Meteo Air Quality API

### Source
Open-Meteo Air Quality API

### Documentation
- https://open-meteo.com/en/docs/air-quality-api

### Acquisition Method
Variables were first selected interactively in the Open-Meteo interface, then collected programmatically for all 81 provinces.

### Main Outputs
Raw:
- `data/raw/open_meteo/air_quality/raw_json/*.json`

Tidy outputs:
- `data/raw/open_meteo/air_quality/tidy/air_quality_current_tr_*.csv`
- `data/raw/open_meteo/air_quality/tidy/air_quality_hourly_tr_*.csv`

### Core Variables
- PM10
- PM2.5
- CO
- NO2
- O3
- SO2
- UV index
- European AQI and component-specific AQI fields

### Purpose
Used for:
- city-level short-term air quality layer
- alerting and risk scoring
- user-facing current conditions
- city-level environmental intelligence

---

## 4.4 Open-Meteo Historical Weather API

### Source
Open-Meteo Historical Weather API

### Documentation
- https://open-meteo.com/en/docs/historical-weather-api

### Acquisition Method
Historical weather was first validated manually via the UI.  
A more robust Python script then collected 2024 historical hourly weather for all 81 provinces using chunked requests.

### Main Outputs
Raw:
- `data/raw/open_meteo/historical_weather/raw_json/*.json`

Tidy output:
- `data/raw/open_meteo/historical_weather/tidy/historical_weather_hourly_tr_2024_*.csv`

### Grain
Hourly, province-level

### Core Variables
- temperature
- relative humidity
- dew point
- apparent temperature
- precipitation
- rain
- snowfall
- weather code
- pressure
- cloud cover
- wind speed / direction / gusts
- is_day
- sunshine_duration

### Purpose
Used for:
- historical actual weather layer
- model features
- seasonality and historical benchmarking
- forecast-vs-actual comparison base

---

## 4.5 Open-Meteo Historical Forecast API

### Source
Open-Meteo Historical Forecast API

### Documentation
- https://open-meteo.com/en/docs/historical-forecast-api

### Acquisition Method
This source required multiple staged tests due to API daily request limits.  
Mini tests, small batches, and full-variable short-range province runs were performed successfully.

### Output Layout
Raw JSON tiers:
- `data/raw/open_meteo/historical_forecast/raw_json/experimental/daily_chunks/`
- `data/raw/open_meteo/historical_forecast/raw_json/validated/monthly_chunks/`
- `data/raw/open_meteo/historical_forecast/raw_json/canonical/short_range/`

Tidy tiers:
- `data/raw/open_meteo/historical_forecast/tidy/experimental/`
- `data/raw/open_meteo/historical_forecast/tidy/validated/`
- `data/raw/open_meteo/historical_forecast/tidy/canonical/`

### Canonical Short-Range Validated Set
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_01_15_2024_01_21_light.csv`
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_01_15_2024_01_21_full.csv`

### Extended 2024 Consolidated Set
- `data/raw/open_meteo/historical_forecast/tidy/validated/historical_forecast_hourly_tr_2024_validated_monthly_full.csv`
- `data/raw/open_meteo/historical_forecast/tidy/validated/historical_forecast_hourly_tr_2024_validated_monthly_light.csv`
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_extended_full.csv`
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_extended_light.csv`

### Validation Metadata
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_canonical_short_range_manifest.csv`
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_folder_inventory.csv`
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_extended_manifest.json`
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_extended_monthly_coverage.csv`
- `data/raw/open_meteo/historical_forecast/README.md`

### Grain
Hourly, province-level, archived forecast

### Purpose
Used for:
- forecast-vs-actual analysis
- forecast quality measurement
- forecast reliability scoring
- future model monitoring and evaluation

### Status Note
The canonical short-range set remains pinned and stable.  
Extended 2024 coverage is now available through validated monthly consolidation plus canonical overlap priority.

---

## 4.6 geoBoundaries Turkey

### Source
geoBoundaries Turkey administrative boundary dataset

### Landing Page
- https://data.humdata.org/dataset/geoboundaries-admin-boundaries-for-turkey

### Acquisition Method
GeoJSON files were manually downloaded and renamed into project-friendly names.

### Raw Files
- `data/raw/geoboundaries/turkey_adm0_country.geojson`
- `data/raw/geoboundaries/turkey_adm1_provinces.geojson`
- `data/raw/geoboundaries/turkey_adm2_districts.geojson`

### Processed Outputs
- `data/processed/geography/dim_province_tr.csv`
- `data/processed/geography/adm1_provinces_tr.geojson`

### Snapshot Metadata
- `data/raw/geoboundaries/SOURCE_METADATA.md`

### Coverage
- ADM0: country boundary
- ADM1: 81 provinces
- ADM2: 973 districts

### Purpose
Used for:
- map layers
- province-level choropleths
- district-level future expansion
- joins between city/province metrics and spatial boundaries

### Important Normalization Note
ADM1 province naming required standardization for:
- `Canakkale -> Çanakkale`
- `Istanbul -> İstanbul`
- `Izmir -> İzmir`

---

## 4.7 CAMS European Air Quality Reanalyses

### Source
Copernicus Atmosphere Data Store (CAMS Europe Air Quality Reanalyses)

### Landing Page
- https://ads.atmosphere.copernicus.eu/datasets/cams-europe-air-quality-reanalyses

### Acquisition Method
The data was requested from the ADS portal after account registration and terms acceptance.  
The retrieval profile used:
- interim reanalysis
- year 2024
- monthly chunks (`2024_01` ... `2024_12`)
- level 0
- ensemble median
- six core variables

### Raw Files
Stored under:
- `data/raw/cams/YYYY_MM_interim_surface_ensemble/`

Each month folder contains six NetCDF files:
- `cams_eu_aq_interim_YYYY_MM_surface_ensemble_co.nc`
- `cams_eu_aq_interim_YYYY_MM_surface_ensemble_no2.nc`
- `cams_eu_aq_interim_YYYY_MM_surface_ensemble_o3.nc`
- `cams_eu_aq_interim_YYYY_MM_surface_ensemble_pm10.nc`
- `cams_eu_aq_interim_YYYY_MM_surface_ensemble_pm2p5.nc`
- `cams_eu_aq_interim_YYYY_MM_surface_ensemble_so2.nc`

### Processed Output
- `data/processed/cams/cams_city_hourly_tr_YYYY_MM.csv` (monthly outputs for available months)
- `data/processed/cams/cams_city_hourly_tr_all_available.csv`
- `data/processed/cams/cams_city_hourly_manifest.json`

### Grid Structure
Validated:
- `time = 744`
- `lat = 420`
- `lon = 700`

### Processed Approach
Instead of flattening the full grid into a giant table, the project samples the nearest CAMS grid cell to each city coordinate and produces a city-hourly tidy output.

### Purpose
Used for:
- historical air quality reanalysis
- stronger environmental backfill than lightweight API-only sources
- comparison with Open-Meteo AQ
- city-level pollution history

### Status Note
Local raw month folders and processed outputs cover `2024_01` through `2024_12`. The extraction script remains month-agnostic and auto-discovers newly added month folders.

---

## 4.8 Derived Analytics Layer (Local-First)

### Source
Internal derivation from validated project datasets using DuckDB.

### Build Components
- `scripts/build_analytics_marts.py`
- `scripts/validate_analytics_outputs.py`
- `scripts/export_data_contracts.py`
- `backend/main.py` (FastAPI serving layer)

### Main Marts
- `data/processed/marts/city_hourly_environment_tr.parquet`
- `data/processed/marts/city_forecast_vs_actual_tr.parquet`
- `data/processed/marts/city_current_snapshot_tr.parquet`
- `data/processed/marts/province_map_metrics_tr.parquet`

### Mobile Views
- `data/processed/views/mobile_city_current_snapshot_tr_light.parquet`
- `data/processed/views/mobile_city_hourly_timeline_tr_light.parquet`
- `data/processed/views/mobile_province_map_metrics_tr_light.parquet`

### Contracts and QA
- `data/processed/marts/marts_build_manifest.json`
- `data/processed/marts/validation_report.json`
- `data/contracts/parquet_contracts.json`
- `data/contracts/api_*.schema.json`

### Purpose
Used for:
- unified city-hourly analytics
- forecast-vs-actual evaluation
- province map metrics
- app-ready lightweight serving
- backend-ready local API contracts

---

# 5. Directory Map

## Raw Data
- `data/raw/open_meteo/geocoding/`
- `data/raw/open_meteo/forecast/`
- `data/raw/open_meteo/air_quality/`
- `data/raw/open_meteo/historical_weather/`
- `data/raw/open_meteo/historical_forecast/`
- `data/raw/open_meteo/historical_forecast/raw_json/`
- `data/raw/open_meteo/historical_forecast/tidy/`
- `data/raw/open_meteo/historical_forecast/manifests/`
- `data/raw/geoboundaries/`
- `data/raw/cams/`

## Processed Data
- `data/processed/geography/`
- `data/processed/cams/`
- `data/processed/marts/`
- `data/processed/views/`
- `data/contracts/`
- `data/db/`

---

# 6. Join Strategy

## Primary Province / City Join Key
- `city_name`

## Geography Join Keys
- `province_name`
- standardized province naming
- `shape_iso` for province-level geometry joins when needed

## Spatial Layer
- `adm1_provinces_tr.geojson`

## Historical Comparison Layer
- historical weather and historical forecast should align by:
  - `city_name`
  - `time`

## CAMS Alignment
- CAMS output is already transformed into city-level hourly data
- joins should use:
  - `city_name`
  - `time`

---

# 7. Why These Datasets Exist Together

This project is not a single-source weather dataset project.  
It is a layered environmental intelligence system.

Each source plays a different role:

- **Geocoding** provides stable city coordinates
- **Forecast** provides near-term predictive weather
- **Air Quality API** provides short-term pollution signals
- **Historical Weather** provides actual observed/reanalysis-like weather context
- **Historical Forecast** provides archived forecast context for reliability analysis
- **geoBoundaries** provides spatial geometry
- **CAMS** provides stronger historical pollution reanalysis

Together, these sources support:

- environmental dashboards
- map layers
- forecasting and reliability analytics
- exposure and pollution intelligence
- app features and user-facing environmental context

---

# 8. Current Completion Status

## Completed
- Open-Meteo Geocoding
- Open-Meteo Forecast
- Open-Meteo Air Quality
- Open-Meteo Historical Weather
- geoBoundaries raw and processed geography layers
- CAMS first processed batch
- Historical Forecast canonical short-range validated set (full + light)
- Historical Forecast extended 2024 consolidated set
- Analytics marts (`city_hourly_environment_tr`, `city_forecast_vs_actual_tr`, `city_current_snapshot_tr`, `province_map_metrics_tr`)
- Mobile lightweight views and FastAPI local serving contracts

## Remaining Expansion Work
- CAMS additional months
- feature-store style model datasets
- production hardening for scheduled refresh and deployment

---

# 9. Usage Notes

- Preserve raw files as source-of-truth artifacts.
- Do not overwrite processed outputs without version awareness.
- Standardized province naming must be preserved for map joins.
- Historical Forecast API extraction should be rate-limit aware.
- Historical Forecast canonical files should be treated as immutable unless re-validation is documented.
- geoBoundaries snapshot checksums and feature counts are pinned in `data/raw/geoboundaries/SOURCE_METADATA.md`.
- CAMS raw NetCDF files should remain archived even after city-level extraction.

---

# 10. Recommended Next Documentation Files

After this file, the next recommended project documentation files are:

- `ANALYTICS_LAYER.md`
- `PROJECT_STATE_AUDIT.md`
- `DATA_PIPELINE_OVERVIEW.md`
- `DATA_DICTIONARY.md`
- `JOIN_STRATEGY.md`

---
