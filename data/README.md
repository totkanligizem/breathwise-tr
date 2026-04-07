# Data Layer

This directory contains source artifacts, processed outputs, and formal contracts.

## Structure
- `data/raw/`: immutable source pulls (Open-Meteo, CAMS, geoBoundaries)
- `data/processed/`: derived marts/views, operational run outputs, generated assets
- `data/contracts/`: API/parquet/i18n/product-shell contracts

## Data Policy
- Raw source artifacts are immutable.
- Preserve reproducibility and deterministic selection logic.
- Keep forecast-derived and actual-derived fields explicitly labeled.
- Do not introduce duplicate `city_name + time` rows.
- Use canonical keys:
  - hourly facts: `city_name + time`
  - city joins: `city_name`
  - province geometry joins: `province_name`

## Canonical Analytical Outputs
- `data/processed/marts/city_hourly_environment_tr.parquet`
- `data/processed/marts/city_forecast_vs_actual_tr.parquet`
- `data/processed/marts/city_current_snapshot_tr.parquet`
- `data/processed/marts/province_map_metrics_tr.parquet`

## Canonical Mobile Views
- `data/processed/views/mobile_city_current_snapshot_tr_light.parquet`
- `data/processed/views/mobile_city_hourly_timeline_tr_light.parquet`
- `data/processed/views/mobile_province_map_metrics_tr_light.parquet`

## Contracts
- `data/contracts/parquet_contracts.json`
- `data/contracts/i18n_contract.json`
- `data/contracts/product_shell_view_models.json`
- `data/contracts/api_*.schema.json`
