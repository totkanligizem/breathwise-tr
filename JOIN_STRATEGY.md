# Join Strategy

## Project
**Breathwise TR**

This document defines how datasets should be aligned, joined, validated, and expanded across the project.

Its purpose is to prevent ambiguous merges, inconsistent grains, duplicate rows, broken map joins, and misleading analytics.

---

# 1. Why a Join Strategy Is Required

This project combines multiple independent source systems:

- Open-Meteo weather forecast
- Open-Meteo air quality
- Open-Meteo historical weather
- Open-Meteo historical forecast
- geoBoundaries Turkey geometry
- CAMS European air quality reanalysis

These sources do not share a single built-in relational schema.  
A clear join strategy is therefore required to ensure:

- correct analytical grain
- stable join keys
- transparent temporal alignment
- naming consistency
- reproducible downstream marts

---

# 2. Core Join Principles

The pipeline must follow these rules:

- always join datasets at the correct grain
- never join hourly and daily datasets directly without explicit aggregation
- always standardize city and province naming before joining
- use canonical dimensions rather than ad hoc string matching whenever possible
- preserve source-specific coordinate metadata
- keep raw and processed geography layers separate
- perform temporal joins explicitly and not by assumption

---

# 3. Canonical Join Keys

## 3.1 City-Time Join Key

The primary analytical join key is:

- `city_name`
- `time`

This key is used for hourly analytical layers.

### Datasets using this key
- forecast hourly
- air quality hourly
- historical weather hourly
- historical forecast hourly
- CAMS city-hourly output

---

## 3.2 City-Only Join Key

The city-only join key is:

- `city_name`

This key is used for:
- current snapshot layers
- city metadata joins
- joins to city dimension tables

### Datasets using this key
- forecast current
- air quality current
- `dim_city`

---

## 3.3 Province Geometry Join Key

The province geometry join key is:

- `province_name`

Supporting identifiers:
- `shape_iso`
- `shape_id`

### Datasets using this key
- `dim_province_tr.csv`
- `adm1_provinces_tr.geojson`
- province-level aggregated analytical tables

---

# 4. Canonical Dimensions Used in Joins

## 4.1 City Dimension

### Source
- `data/raw/open_meteo/geocoding/dim_city_*.csv`

### Role
This is the canonical city reference table.

### Required columns
- `city_name`
- `latitude`
- `longitude`
- `timezone`
- `admin1`
- `country_code`

### Join usage
Used to:
- standardize city-level metadata
- enrich hourly and current tables
- validate 81-city completeness

---

## 4.2 Province Dimension

### Source
- `data/processed/geography/dim_province_tr.csv`

### Role
Canonical province naming and province identifier layer.

### Required columns
- `province_name`
- `province_name_raw`
- `shape_iso`
- `shape_id`

### Join usage
Used to:
- support province-level geometry joins
- standardize mapping labels
- connect aggregated province metrics to ADM1 geometry
- validate pinned geometry snapshot details in `data/raw/geoboundaries/SOURCE_METADATA.md`

---

# 5. Dataset-by-Dataset Join Strategy

## 5.1 Forecast Current

### Grain
- city snapshot

### Join approach
Join only on:
- `city_name`

### Typical use
Used for:
- current condition cards
- latest city snapshot layer
- app landing view

### Important note
Do not directly join current data to hourly historical tables unless creating a specific "latest snapshot" mart.

---

## 5.2 Forecast Hourly

### Grain
- city-hourly

### Join key
- `city_name + time`

### Join targets
Can be joined to:
- air quality hourly
- historical weather hourly
- historical forecast hourly
- CAMS city-hourly, when time windows overlap

### Typical use
- forecast panels
- short-term environmental outlook
- near-term alerting

---

## 5.3 Forecast Daily

### Grain
- city-daily

### Join key
- `city_name + time` at daily level

### Join targets
Can be joined to:
- daily aggregated weather tables
- daily aggregated air quality tables
- province-daily derived marts

### Important note
Do not join daily tables directly to hourly tables without explicit resampling or aggregation.

---

## 5.4 Air Quality Current

### Grain
- city snapshot

### Join key
- `city_name`

### Join targets
Can be joined to:
- city dimension
- current weather snapshot

### Typical use
- current AQ widgets
- live pollution cards
- exposure summary

---

## 5.5 Air Quality Hourly

### Grain
- city-hourly

### Join key
- `city_name + time`

### Join targets
Can be joined to:
- forecast hourly
- historical weather hourly, when aligned on time
- CAMS city-hourly, where time windows overlap or for comparison marts

### Typical use
- short-term AQ analysis
- AQ-weather interaction analysis
- user-facing hourly pollution curves

---

## 5.6 Historical Weather Hourly

### Grain
- city-hourly

### Join key
- `city_name + time`

### Join targets
Can be joined to:
- historical forecast hourly
- CAMS city-hourly
- historical air quality or forecast tables
- any city-hour master table

### Role
This is the main “actual historical weather” layer.

---

## 5.7 Historical Forecast Hourly

### Grain
- city-hourly

### Join key
- `city_name + time`

### Join targets
Can be joined to:
- historical weather hourly

### Preferred canonical inputs
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_01_15_2024_01_21_light.csv`
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_01_15_2024_01_21_full.csv`

### Validation metadata
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_canonical_short_range_manifest.csv`

### Primary purpose
Forecast-vs-actual comparison.

### Important note
This dataset should be treated as a forecast layer, not an observed weather layer.
Use canonical outputs by default; treat `tidy/experimental/` outputs as non-production unless explicitly promoted.

---

## 5.8 CAMS City-Hourly Output

### Grain
- city-hourly

### Join key
- `city_name + time`

### Join targets
Can be joined to:
- historical weather hourly
- air quality hourly
- historical forecast hourly, if needed for broader environmental comparison
- future master city-hourly marts

### Role
This is the historical reanalysis pollution layer.

---

## 5.9 ADM1 Processed Geometry

### File
- `data/processed/geography/adm1_provinces_tr.geojson`

### Snapshot metadata
- `data/raw/geoboundaries/SOURCE_METADATA.md`

### Grain
- province geometry

### Join key
- `province_name`

### Join targets
Can be joined to:
- province-level aggregated metrics
- province-daily marts
- province snapshot tables
- map-ready outputs

### Important note
Do not join city-hourly fact tables directly to geometry without first aggregating to province-level outputs.

---

# 6. Required Standardization Before Joins

## 6.1 Province Name Standardization

Known corrections:

- `Canakkale -> Çanakkale`
- `Istanbul -> İstanbul`
- `Izmir -> İzmir`

These corrections must be applied before:
- geometry joins
- province-level map aggregation
- province reporting outputs

---

## 6.2 City Name Stability

The project assumes a canonical city naming layer from geocoding.

### Rule
Do not create manual alternative spellings in analytical tables.

Always preserve:
- `city_name` as canonical join name

---

# 7. Temporal Alignment Rules

## 7.1 Hourly Joins

Hourly tables should only be joined to other hourly tables.

### Example valid joins
- forecast hourly + AQ hourly
- historical weather + historical forecast
- historical weather + CAMS city-hourly

### Condition
Exact time alignment is required:
- same timezone basis
- same timestamp resolution
- same `city_name`

---

## 7.2 Daily Joins

Daily tables should only be joined to daily tables, or to hourly tables that have been explicitly aggregated to daily level.

### Examples
Valid:
- forecast daily + daily weather aggregation
- forecast daily + daily AQ aggregation

Invalid:
- forecast daily + raw hourly CAMS
- forecast daily + raw hourly historical weather

---

## 7.3 Current Snapshot Joins

Current snapshot datasets should only be joined to:
- current snapshot datasets
- city metadata
- app snapshot views

They should not be treated as time-series fact tables.

---

# 8. Recommended Master Tables

## 8.1 City-Hourly Master Layer

### Recommended grain
- `city_name + time`

### Suggested components
- historical weather
- historical forecast
- air quality hourly
- CAMS city-hourly
- forecast hourly, where relevant

### Suggested future output
- `data/processed/marts/city_hourly_environment_tr.parquet` (implemented)

---

## 8.2 Province Map Snapshot Layer

### Recommended grain
- province snapshot or province-day

### Suggested components
- aggregated city-level metrics
- latest AQ / weather risk signals
- province geometry join

### Suggested future output
- `data/processed/marts/province_map_metrics_tr.parquet` (implemented)

---

## 8.3 Forecast-vs-Actual Layer

### Recommended grain
- `city_name + time`

### Suggested components
- historical forecast
- historical weather

### Suggested future output
- `data/processed/marts/city_forecast_vs_actual_tr.parquet` (implemented)

---

# 9. Join Order Recommendations

The recommended order of integration is:

## Step 1
Build and validate dimensions:
- `dim_city`
- `dim_province_tr`

## Step 2
Prepare geography:
- `adm1_provinces_tr.geojson`

## Step 3
Assemble city-hourly environmental layers:
- historical weather
- air quality hourly
- CAMS city-hourly
- historical forecast

## Step 4
Add forecast hourly where needed for operational and app-facing layers

## Step 5
Create and validate aggregated province and snapshot marts

---

# 10. Duplicate Prevention Rules

Before finalizing any joined dataset:

- check row counts before and after joins
- confirm expected number of cities
- confirm expected number of timestamps
- inspect duplicate combinations of `city_name + time`

### Example rule
No final hourly analytical table should contain duplicate:
- `city_name`
- `time`

If duplicates exist, resolve them before downstream use.

---

# 11. Missing Data Handling Rules

## Rule 1
Do not silently fill missing joins without explicit documentation.

## Rule 2
Track coverage at the source layer first.

## Rule 3
Differentiate between:
- source missingness
- join mismatch
- timezone misalignment
- naming mismatch

## Rule 4
CAMS and Open-Meteo may differ in coverage windows; missing overlaps should be expected and documented.

---

# 12. Recommended Validation Checks After Each Join

After every major join, validate:

- row count
- unique city count
- time min / max
- duplicate `city_name + time`
- null counts in critical fields
- coverage by city
- small sample inspection

### Minimum validation fields
- `city_name`
- `time`
- one weather variable
- one AQ variable
- one metadata field

---

# 13. Spatial Join Rules

## Province geometry joins
Only join:
- aggregated province tables
to:
- `adm1_provinces_tr.geojson`

## Why
City-hourly facts are analytical tables, not direct geometry layers.

### Correct path
city-hourly facts  
→ aggregate to province  
→ join to ADM1 processed geometry

---

# 14. CAMS-Specific Join Notes

CAMS raw data is gridded scientific data, but the project already converts it into:

- city-hourly tidy format

Therefore downstream joins should use:
- `cams_city_hourly_tr_all_available.csv` (preferred combined output)
- or month-level files such as `cams_city_hourly_tr_2024_01.csv`
not raw NetCDF files.

This keeps joins:
- lightweight
- transparent
- reproducible

---

# 15. Historical Forecast Join Notes

Historical forecast should not be merged blindly into master history tables without labeling it as forecast.

Use the extended canonical historical forecast file as default input when available:
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_extended_full.csv`

Canonical short-range files remain the pinned validation reference set and should keep precedence on overlapping rows.

### Best practice
Retain clear naming such as:
- `hf_temperature_2m`
- `hw_temperature_2m`

or preserve source table separation until the comparison mart is built.

This avoids confusion between:
- forecast values
- actual values

---

# 16. Suggested Future Join Outputs

Implemented join-oriented outputs:

- `data/processed/marts/city_hourly_environment_tr.parquet`
- `data/processed/marts/city_forecast_vs_actual_tr.parquet`
- `data/processed/marts/province_map_metrics_tr.parquet`
- `data/processed/marts/city_current_snapshot_tr.parquet`

Remaining future extension:

- `data/processed/marts/city_pollution_weather_tr.parquet`

---

# 17. Anti-Patterns to Avoid

Do not:

- join raw geometry directly to city-hourly fact tables
- mix daily and hourly tables without resampling
- rely on raw unstandardized province names
- overwrite canonical join keys ad hoc
- flatten CAMS raw grid data into giant unnecessary joins when city-hourly output already exists
- treat current snapshot files as time-series master layers

---

# 18. Summary

The project should treat:

- `city_name + time` as the main hourly analytical key
- `province_name` as the map aggregation key
- `dim_city` and `dim_province_tr` as canonical dimensions
- processed CAMS and processed ADM1 geometry as preferred downstream layers

The safest join path is:

city dimension  
→ hourly environmental tables  
→ aggregated province marts  
→ processed geometry joins

---
