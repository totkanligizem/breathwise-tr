# Third-Party Sources and Attribution

This project integrates third-party public data/services. Source ownership and license terms remain with each provider.

## Data Providers
- **Open-Meteo**
  - Geocoding API
  - Forecast API
  - Air Quality API
  - Historical Weather API
  - Historical Forecast API
  - Docs: https://open-meteo.com/en/docs

- **geoBoundaries**
  - Turkey ADM0 / ADM1 / ADM2 boundaries
  - Docs/Home: https://www.geoboundaries.org/

- **CAMS (Copernicus Atmosphere Monitoring Service / ADS)**
  - Interim air-quality reanalysis files used for city-hourly derivation
  - Home: https://atmosphere.copernicus.eu/

## Usage and Compliance Notes
- Do not assume this repository transfers source IP rights.
- Check each upstream provider’s terms before public/commercial redistribution.
- Keep provenance metadata and snapshot notes in source manifests where available.
- Avoid copying third-party documentation text verbatim into product content.
- User-facing output should be generated from project data contracts, not scraped text.

## Project Policy
- Preserve raw source artifact traceability.
- Publish/share through sanitized metadata surfaces when needed (`publish_safe=true` on API metadata endpoints).
- Keep secrets and machine-specific details out of tracked files.
