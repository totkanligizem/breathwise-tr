# Data Dictionary

## Project
**Breathwise TR**

This document defines the main datasets, column meanings, units, grains, and join-relevant fields used across the project.

It is intended to support:

- data understanding
- transformation consistency
- join planning
- dashboard design
- feature engineering
- future modeling workflows

---

# 1. Dictionary Conventions

## Grain
The term **grain** refers to the natural observation level of a dataset.

Examples:
- city-level current snapshot
- city-hourly
- city-daily
- province geometry

## Join Key
The term **join key** refers to the field or field combination used to connect datasets.

Examples:
- `city_name`
- `city_name + time`
- `province_name`

## Units
Units are documented where known from source APIs or dataset specs.

---

# 2. Core Join Keys

## 2.1 City-Level Analytical Join Key
Primary analytical join key:

- `city_name`
- `time`

Used in:
- forecast hourly
- air quality hourly
- historical weather hourly
- historical forecast hourly
- CAMS city-hourly output

## 2.2 Province Geometry Join Key
Primary geography join key:

- `province_name`

Secondary geographic identifiers:
- `shape_iso`
- `shape_id`

---

# 3. Canonical Dimensions

## 3.1 City Dimension

### Main file
- `data/raw/open_meteo/geocoding/dim_city_*.csv`

### Grain
- one row per city / province

### Core Columns

#### `city_name`
Standard province/city name used throughout the pipeline.

#### `latitude`
Requested city latitude.

#### `longitude`
Requested city longitude.

#### `timezone`
Timezone associated with the city.

#### `admin1`
Administrative region label returned by geocoding.

#### `country_code`
Country code, expected to be `TR`.

#### `population`
Population when available from the source geocoding response.

### Notes
This is the canonical city reference table for Open-Meteo API calls and city-level joins.

---

## 3.2 Province Dimension

### Main file
- `data/processed/geography/dim_province_tr.csv`

### Grain
- one row per province

### Core Columns

#### `province_name_raw`
Original province name from raw geoBoundaries properties.

#### `province_name`
Standardized province name used for joins.

#### `shape_iso`
Province ISO-style geographic code.

#### `shape_id`
geoBoundaries feature identifier.

#### `shape_group`
Country group code, expected to be `TUR`.

#### `shape_type`
Administrative level type, expected to be `ADM1`.

#### `level`
Administrative level label, expected to be `ADM1`.

### Notes
This table standardizes province names for map joins.

---

# 4. Open-Meteo Forecast Datasets

## 4.1 Forecast Current

### File pattern
- `forecast_current_tr_*.csv`

### Grain
- one row per city at extraction time

### Typical Columns

#### `city_name`
Canonical province/city name.

#### `temperature_2m`
Current air temperature at 2 meters.  
Unit: °C

#### `relative_humidity_2m`
Current relative humidity at 2 meters.  
Unit: %

#### `apparent_temperature`
Feels-like temperature.  
Unit: °C

#### `precipitation`
Current precipitation amount.  
Unit: mm

#### `rain`
Current rain amount.  
Unit: mm

#### `showers`
Current showers amount.  
Unit: mm

#### `snowfall`
Current snowfall amount.  
Unit: cm or source-defined unit depending on API output

#### `is_day`
Binary day/night indicator.  
Expected values: `0` or `1`

#### `weather_code`
WMO weather code.

#### `cloud_cover`
Total cloud cover.  
Unit: %

#### `wind_speed_10m`
Wind speed at 10 meters.  
Unit: km/h

#### `wind_direction_10m`
Wind direction at 10 meters.  
Unit: degrees

#### `wind_gusts_10m`
Wind gusts at 10 meters.  
Unit: km/h

#### `sea_level_pressure`
Sea-level pressure.  
Unit: hPa

#### `surface_pressure`
Surface pressure.  
Unit: hPa

### Join Relevance
Mostly useful for current snapshot views, not as a long historical fact table.

---

## 4.2 Forecast Hourly

### File pattern
- `forecast_hourly_tr_*.csv`

### Grain
- city-hourly

### Typical Columns

#### `city_name`
Canonical city/province name.

#### `time`
Hourly timestamp in local timezone-aligned ISO format.

#### `temperature_2m`
Air temperature at 2 meters.  
Unit: °C

#### `relative_humidity_2m`
Relative humidity at 2 meters.  
Unit: %

#### `dew_point_2m`
Dew point at 2 meters.  
Unit: °C

#### `apparent_temperature`
Feels-like temperature.  
Unit: °C

#### `precipitation_probability`
Probability of precipitation.  
Unit: %

#### `precipitation`
Total precipitation.  
Unit: mm

#### `rain`
Rain amount.  
Unit: mm

#### `showers`
Showers amount.  
Unit: mm

#### `snowfall`
Snowfall amount.  
Unit: cm or source-defined unit

#### `pressure_msl`
Mean sea-level pressure.  
Unit: hPa

#### `cloud_cover`
Total cloud cover.  
Unit: %

#### `cloud_cover_low`
Low cloud cover.  
Unit: %

#### `cloud_cover_mid`
Mid-level cloud cover.  
Unit: %

#### `cloud_cover_high`
High cloud cover.  
Unit: %

#### `visibility`
Horizontal visibility.  
Unit: m

#### `wind_speed_10m`
Wind speed at 10 meters.  
Unit: km/h

#### `wind_direction_10m`
Wind direction at 10 meters.  
Unit: degrees

#### `wind_gusts_10m`
Wind gusts at 10 meters.  
Unit: km/h

#### `uv_index`
UV index.

#### `is_day`
Day/night flag.  
Expected values: `0` or `1`

#### `freezing_level_height`
Freezing level height.  
Unit: m

#### `weather_code`
WMO weather code.

### Join Key
- `city_name + time`

---

## 4.3 Forecast Daily

### File pattern
- `forecast_daily_tr_*.csv`

### Grain
- city-daily

### Typical Columns

#### `city_name`
Canonical city/province name.

#### `time`
Daily date.

#### `weather_code`
Representative daily weather code.

#### `temperature_2m_max`
Daily maximum temperature.  
Unit: °C

#### `temperature_2m_min`
Daily minimum temperature.  
Unit: °C

#### `apparent_temperature_max`
Daily maximum apparent temperature.  
Unit: °C

#### `apparent_temperature_min`
Daily minimum apparent temperature.  
Unit: °C

#### `sunrise`
Sunrise timestamp.

#### `sunset`
Sunset timestamp.

#### `daylight_duration`
Daylight duration.  
Unit: seconds

#### `sunshine_duration`
Sunshine duration.  
Unit: seconds

#### `uv_index_max`
Maximum daily UV index.

#### `rain_sum`
Daily rain sum.  
Unit: mm

#### `showers_sum`
Daily showers sum.  
Unit: mm

#### `snowfall_sum`
Daily snowfall sum.  
Unit: cm or source-defined unit

#### `precipitation_sum`
Daily precipitation sum.  
Unit: mm

#### `precipitation_probability_max`
Maximum daily precipitation probability.  
Unit: %

#### `wind_speed_10m_max`
Maximum daily wind speed.  
Unit: km/h

#### `wind_gusts_10m_max`
Maximum daily gust speed.  
Unit: km/h

#### `wind_direction_10m_dominant`
Dominant daily wind direction.  
Unit: degrees

### Join Key
- `city_name + time` at daily level

---

# 5. Open-Meteo Air Quality Datasets

## 5.1 Air Quality Current

### File pattern
- `air_quality_current_tr_*.csv`

### Grain
- one row per city at extraction time

### Core Columns

#### `city_name`
Canonical city/province name.

#### `european_aqi`
European Air Quality Index.

#### `pm10`
Particulate matter < 10 µm.  
Unit: µg/m³

#### `pm2_5`
Particulate matter < 2.5 µm.  
Unit: µg/m³

#### `carbon_monoxide`
Carbon monoxide.  
Unit: µg/m³

#### `nitrogen_dioxide`
Nitrogen dioxide.  
Unit: µg/m³

#### `sulphur_dioxide`
Sulphur dioxide.  
Unit: µg/m³

#### `ozone`
Ozone.  
Unit: µg/m³

#### `uv_index`
UV index.

### Notes
Used primarily for current air quality presentation and snapshot alerting.

---

## 5.2 Air Quality Hourly

### File pattern
- `air_quality_hourly_tr_*.csv`

### Grain
- city-hourly

### Core Columns

#### `city_name`
Canonical city/province name.

#### `time`
Hourly timestamp.

#### `pm10`
Particulate matter < 10 µm.  
Unit: µg/m³

#### `pm2_5`
Particulate matter < 2.5 µm.  
Unit: µg/m³

#### `carbon_monoxide`
Carbon monoxide.  
Unit: µg/m³

#### `nitrogen_dioxide`
Nitrogen dioxide.  
Unit: µg/m³

#### `sulphur_dioxide`
Sulphur dioxide.  
Unit: µg/m³

#### `ozone`
Ozone.  
Unit: µg/m³

#### `uv_index`
UV index.

#### `european_aqi`
European AQI.

#### `european_aqi_pm2_5`
AQI component for PM2.5.

#### `european_aqi_pm10`
AQI component for PM10.

#### `european_aqi_ozone`
AQI component for ozone.

#### `european_aqi_nitrogen_dioxide`
AQI component for NO2.

#### `european_aqi_sulphur_dioxide`
AQI component for SO2.

### Join Key
- `city_name + time`

---

# 6. Open-Meteo Historical Weather Dataset

## 6.1 Historical Weather Hourly

### File pattern
- `historical_weather_hourly_tr_2024_*.csv`

### Grain
- city-hourly

### Core Columns

#### `city_name`
Canonical city/province name.

#### `time`
Hourly timestamp.

#### `temperature_2m`
Observed / historical air temperature at 2m.  
Unit: °C

#### `relative_humidity_2m`
Historical relative humidity.  
Unit: %

#### `dew_point_2m`
Historical dew point.  
Unit: °C

#### `apparent_temperature`
Historical apparent temperature.  
Unit: °C

#### `precipitation`
Historical precipitation.  
Unit: mm

#### `rain`
Historical rain.  
Unit: mm

#### `snowfall`
Historical snowfall.  
Unit: cm or source-defined unit

#### `weather_code`
Historical WMO weather code.

#### `pressure_msl`
Historical mean sea-level pressure.  
Unit: hPa

#### `cloud_cover`
Historical total cloud cover.  
Unit: %

#### `cloud_cover_low`
Historical low cloud cover.  
Unit: %

#### `cloud_cover_mid`
Historical middle cloud cover.  
Unit: %

#### `cloud_cover_high`
Historical high cloud cover.  
Unit: %

#### `wind_speed_10m`
Historical wind speed.  
Unit: km/h

#### `wind_direction_10m`
Historical wind direction.  
Unit: degrees

#### `wind_gusts_10m`
Historical gust speed.  
Unit: km/h

#### `is_day`
Historical day/night flag.

#### `sunshine_duration`
Historical sunshine duration.  
Unit: seconds

#### `latitude_requested`
Requested city latitude.

#### `longitude_requested`
Requested city longitude.

#### `latitude_used`
Latitude used by the source response.

#### `longitude_used`
Longitude used by the source response.

#### `elevation_used`
Elevation in source response.  
Unit: meters

#### `timezone`
Timezone label.

#### `timezone_abbreviation`
Timezone abbreviation.

#### `utc_offset_seconds`
UTC offset.  
Unit: seconds

#### `generationtime_ms`
API generation time.  
Unit: milliseconds

### Join Key
- `city_name + time`

### Analytical Role
Serves as the historical actual weather layer.

---

# 7. Open-Meteo Historical Forecast Datasets

## 7.0 Output Tier Layout

Canonical, validated, and experimental outputs are separated:

- `data/raw/open_meteo/historical_forecast/raw_json/experimental/daily_chunks/`
- `data/raw/open_meteo/historical_forecast/raw_json/validated/monthly_chunks/`
- `data/raw/open_meteo/historical_forecast/raw_json/canonical/short_range/`
- `data/raw/open_meteo/historical_forecast/tidy/experimental/`
- `data/raw/open_meteo/historical_forecast/tidy/validated/`
- `data/raw/open_meteo/historical_forecast/tidy/canonical/`

Canonical output manifest:
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_canonical_short_range_manifest.csv`

Folder inventory manifest:
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_folder_inventory.csv`

Extended coverage manifests:
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_extended_manifest.json`
- `data/raw/open_meteo/historical_forecast/manifests/historical_forecast_extended_monthly_coverage.csv`

Operational notes:
- `data/raw/open_meteo/historical_forecast/README.md`

---

## 7.1 Historical Forecast Light Batch

### File
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_01_15_2024_01_21_light.csv`

### Grain
- city-hourly

### Core Columns

#### `city_name`
Canonical city/province name.

#### `time`
Hourly timestamp.

#### `temperature_2m`
Archived forecast temperature.  
Unit: °C

#### `precipitation`
Archived forecast precipitation.  
Unit: mm

#### `weather_code`
Archived forecast WMO code.

#### `wind_speed_10m`
Archived forecast wind speed.  
Unit: km/h

#### `latitude_requested`
Requested city latitude.

#### `longitude_requested`
Requested city longitude.

#### `latitude_used`
Latitude used in the response.

#### `longitude_used`
Longitude used in the response.

#### `elevation_used`
Elevation from the response.

#### `timezone`
Timezone label.

#### `timezone_abbreviation`
Timezone abbreviation.

#### `utc_offset_seconds`
UTC offset in seconds.

#### `generationtime_ms`
Response generation time in milliseconds.

### Join Key
- `city_name + time`

### Analytical Role
Light validation layer for forecast-vs-actual workflows.

---

## 7.2 Historical Forecast Full Batch

### File
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_01_15_2024_01_21_full.csv`

### Grain
- city-hourly

### Core Columns

#### `city_name`
Canonical city/province name.

#### `time`
Hourly timestamp.

#### `temperature_2m`
Archived forecast temperature.  
Unit: °C

#### `relative_humidity_2m`
Archived forecast relative humidity.  
Unit: %

#### `dew_point_2m`
Archived forecast dew point.  
Unit: °C

#### `apparent_temperature`
Archived forecast apparent temperature.  
Unit: °C

#### `precipitation_probability`
Archived forecast precipitation probability.  
Unit: %

#### `precipitation`
Archived forecast precipitation.  
Unit: mm

#### `rain`
Archived forecast rain.  
Unit: mm

#### `showers`
Archived forecast showers.  
Unit: mm

#### `snowfall`
Archived forecast snowfall.  
Unit: cm or source-defined unit

#### `weather_code`
Archived forecast WMO weather code.

#### `pressure_msl`
Archived forecast mean sea-level pressure.  
Unit: hPa

#### `cloud_cover`
Archived forecast total cloud cover.  
Unit: %

#### `cloud_cover_low`
Archived forecast low cloud cover.  
Unit: %

#### `cloud_cover_mid`
Archived forecast mid cloud cover.  
Unit: %

#### `cloud_cover_high`
Archived forecast high cloud cover.  
Unit: %

#### `visibility`
Archived forecast visibility.  
Unit: m

#### `wind_speed_10m`
Archived forecast wind speed.  
Unit: km/h

#### `wind_direction_10m`
Archived forecast wind direction.  
Unit: degrees

#### `wind_gusts_10m`
Archived forecast gust speed.  
Unit: km/h

#### `uv_index`
Archived forecast UV index.

#### `is_day`
Archived forecast day/night flag.

#### `freezing_level_height`
Archived forecast freezing level height.  
Unit: m

#### `latitude_requested`
Requested latitude.

#### `longitude_requested`
Requested longitude.

#### `latitude_used`
Response latitude.

#### `longitude_used`
Response longitude.

#### `elevation_used`
Response elevation.

#### `timezone`
Timezone label.

#### `timezone_abbreviation`
Timezone abbreviation.

#### `utc_offset_seconds`
UTC offset in seconds.

#### `generationtime_ms`
Response generation time in milliseconds.

### Join Key
- `city_name + time`

### Analytical Role
Primary canonical archived forecast validation layer.

---

## 7.3 Historical Forecast Extended Consolidated Set

### Files
- `data/raw/open_meteo/historical_forecast/tidy/validated/historical_forecast_hourly_tr_2024_validated_monthly_full.csv`
- `data/raw/open_meteo/historical_forecast/tidy/validated/historical_forecast_hourly_tr_2024_validated_monthly_light.csv`
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_extended_full.csv`
- `data/raw/open_meteo/historical_forecast/tidy/canonical/historical_forecast_hourly_tr_2024_extended_light.csv`

### Grain
- city-hourly

### Core Additional Columns
- `source_tier`
- `source_batch`
- `raw_file_name` (full outputs)

### Coverage
- `2024-01-01T00:00` to `2024-12-31T23:00`

### Notes
- Canonical short-range rows keep precedence on overlap windows.
- Consolidation quality is tracked in the extended manifest.

---

# 8. Geography Datasets

## 8.0 Snapshot Metadata

Pinned local snapshot metadata is tracked in:
- `data/raw/geoboundaries/SOURCE_METADATA.md`

Pinned local feature counts:
- ADM0: 1
- ADM1: 81
- ADM2: 973

---

## 8.1 ADM0 Country Geometry

### File
- `data/raw/geoboundaries/turkey_adm0_country.geojson`

### Grain
- one country polygon

### Core Properties
- `shapeName`
- `Level`
- `shapeISO`
- `shapeID`
- `shapeGroup`
- `shapeType`

### Purpose
Country outline and national base map layer.

---

## 8.2 ADM1 Province Geometry Raw

### File
- `data/raw/geoboundaries/turkey_adm1_provinces.geojson`

### Grain
- one row / feature per province

### Core Properties
- `shapeName`
- `Level`
- `shapeISO`
- `shapeID`
- `shapeGroup`
- `shapeType`

### Purpose
Raw province geometry before name standardization.

---

## 8.3 ADM2 District Geometry Raw

### File
- `data/raw/geoboundaries/turkey_adm2_districts.geojson`

### Grain
- one row / feature per district

### Core Properties
- `shapeName`
- `Level`
- `shapeID`
- `shapeGroup`
- `shapeType`

### Purpose
District-level future map expansion and fine-grained spatial layer.

---

## 8.4 ADM1 Province Geometry Processed

### File
- `data/processed/geography/adm1_provinces_tr.geojson`

### Grain
- one province feature

### Core Properties

#### `province_name_raw`
Original province name from raw GeoJSON.

#### `province_name`
Standardized province name used in project joins.

#### `shape_iso`
Province geographic code.

#### `shape_id`
Province feature identifier.

#### `shape_group`
Expected country group code.

#### `shape_type`
Administrative type.

#### `level`
Administrative level label.

### Purpose
Map-ready province layer for joins and choropleths.

---

# 9. CAMS Datasets

## 9.1 CAMS Raw NetCDF Files

### Directory Pattern
- `data/raw/cams/YYYY_MM_interim_surface_ensemble/`

### Example Files (`2024_01`)
- `cams_eu_aq_interim_2024_01_surface_ensemble_co.nc`
- `cams_eu_aq_interim_2024_01_surface_ensemble_no2.nc`
- `cams_eu_aq_interim_2024_01_surface_ensemble_o3.nc`
- `cams_eu_aq_interim_2024_01_surface_ensemble_pm10.nc`
- `cams_eu_aq_interim_2024_01_surface_ensemble_pm2p5.nc`
- `cams_eu_aq_interim_2024_01_surface_ensemble_so2.nc`

### Grain
- gridded hourly NetCDF

### Grid Dimensions
- `time = 744`
- `lat = 420`
- `lon = 700`

### Variables
- `co`
- `no2`
- `o3`
- `pm10`
- `pm2p5`
- `so2`

### Purpose
Scientific reanalysis source-of-truth pollution files.

---

## 9.2 CAMS City-Hourly Processed Output

### Files
- `data/processed/cams/cams_city_hourly_tr_2024_01.csv`
- `data/processed/cams/cams_city_hourly_tr_all_available.csv`
- `data/processed/cams/cams_city_hourly_manifest.json`

### Grain
- city-hourly

### Core Columns

#### `city_name`
Canonical city/province name.

#### `time`
Hourly timestamp.

#### `latitude_requested`
Province coordinate latitude used for nearest-grid extraction.

#### `longitude_requested`
Province coordinate longitude used for nearest-grid extraction.

#### `latitude_used`
Nearest CAMS grid latitude selected.

#### `longitude_used`
Nearest CAMS grid longitude selected.

#### `source_month`
Origin month key (`YYYY_MM`) for each row.

#### `co`
Carbon monoxide.  
Unit: µg/m³

#### `no2`
Nitrogen dioxide.  
Unit: µg/m³

#### `o3`
Ozone.  
Unit: µg/m³

#### `pm10`
Particulate matter < 10 µm.  
Unit: µg/m³

#### `pm2p5`
Particulate matter < 2.5 µm.  
Unit: µg/m³

#### `so2`
Sulphur dioxide.  
Unit: µg/m³

#### `city_latitude`
Canonical city latitude from city dimension.

#### `city_longitude`
Canonical city longitude from city dimension.

#### `timezone`
City timezone.

#### `admin1`
Administrative region.

#### `country_code`
Country code.

### Join Key
- `city_name + time`

### Analytical Role
Historical reanalysis pollution layer aligned to city-level analysis.

---

# 10. Metadata / Technical Columns

These columns appear in multiple Open-Meteo datasets and should be interpreted consistently.

## `latitude_requested`
Latitude sent in the API request.

## `longitude_requested`
Longitude sent in the API request.

## `latitude_used`
Latitude returned / used by the source model.

## `longitude_used`
Longitude returned / used by the source model.

## `elevation_used`
Elevation reported by the response.

## `timezone`
Timezone identifier.

## `timezone_abbreviation`
Timezone short label.

## `utc_offset_seconds`
UTC offset in seconds.

## `generationtime_ms`
API response generation time in milliseconds.

---

# 11. Implemented Master Layer Grain

The implemented master analytical grain is:

- `city_name`
- `time`

at hourly resolution for `city_hourly_environment_tr`.

The table integrates:

- forecast hourly
- air quality hourly
- historical weather hourly
- historical forecast hourly
- CAMS city-hourly

---

# 12. Derived Analytics Marts

## 12.1 City Hourly Environment

### File
- `data/processed/marts/city_hourly_environment_tr.parquet`

### Grain
- city-hourly

### Semantic Prefixes
- `hw_*`: historical weather (actual)
- `hf_*`: historical forecast (archived forecast)
- `fw_*`: forecast hourly
- `aq_*`: air quality hourly
- `cams_*`: CAMS reanalysis

### Core Fields
- `city_name`
- `time`
- `province_name`
- `available_source_count`
- source flags: `has_historical_weather`, `has_historical_forecast`, `has_forecast_hourly`, `has_air_quality_hourly`, `has_cams_reanalysis`

## 12.2 Forecast vs Actual

### File
- `data/processed/marts/city_forecast_vs_actual_tr.parquet`

### Grain
- city-hourly

### Core Fields
- `city_name`
- `time`
- `hf_temperature_2m`
- `hw_temperature_2m`
- `err_temperature_2m`
- `abs_err_temperature_2m`
- `forecast_validation_window`

## 12.3 City Current Snapshot

### File
- `data/processed/marts/city_current_snapshot_tr.parquet`

### Grain
- one row per city

### Core Fields
- `city_name`
- `snapshot_time`
- `forecast_temperature_2m`
- `forecast_weather_code`
- `aq_european_aqi`
- `aq_category`

## 12.4 Province Map Metrics

### File
- `data/processed/marts/province_map_metrics_tr.parquet`

### Grain
- one row per province

### Core Fields
- `province_name`
- `shape_iso`
- `snapshot_time`
- `avg_aq_european_aqi`
- `avg_forecast_temperature_2m`
- `cams_avg_pm2p5`
- `cams_avg_no2`
- `cams_2024_01_avg_pm2p5` (legacy compatibility alias)
- `map_priority_score`
- `aq_alert_flag`
- `heat_alert_flag`

---

# 13. Notes on Naming Consistency

Province and city names must remain standardized.

Important known standardizations include:
- `Canakkale -> Çanakkale`
- `Istanbul -> İstanbul`
- `Izmir -> İzmir`

No ad hoc renaming should be introduced outside documented transformation logic.

---

# 14. Future Extensions

Likely future additions to this dictionary:

- engineered features
- derived risk scores
- anomaly and reliability metrics
- dashboard KPI definitions

---
