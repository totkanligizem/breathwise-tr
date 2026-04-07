# Backend (FastAPI)

Breathwise TR backend serves validated parquet-based analytics outputs through a local-first FastAPI layer.

## Responsibilities
- Serve city/province/mobile endpoints from marts/views
- Enforce optional API-key auth
- Provide caching, rate-limit hooks, CORS controls
- Expose health/readiness and ops status metadata
- Keep publish-safe metadata mode (`publish_safe=true`)

## Main Files
- `backend/main.py`: routes, query logic, localization enrichment, ops metadata
- `backend/config.py`: env-based settings, project-root discovery, CORS/auth/cache/rate-limit config
- `backend/schemas.py`: response models

## Key Endpoints
- `GET /health`
- `GET /ready`
- `GET /v1/meta/ops-status`
- `GET /v1/meta/datasets`
- `GET /v1/meta/localization`
- `GET /v1/cities/current`
- `GET /v1/cities/{city_name}/hourly`
- `GET /v1/provinces/map-metrics`
- `GET /v1/mobile/cities/current`
- `GET /v1/mobile/cities/{city_name}/timeline`
- `GET /v1/mobile/provinces/map-metrics`

## Required Environment (Typical)
- `BREATHWISE_API_AUTH_ENABLED` (`true/false`)
- `BREATHWISE_API_KEY` (required when auth enabled)
- `BREATHWISE_API_KEY_HEADER_NAME` (default: `X-API-Key`)
- `BREATHWISE_MARTS_DIR`, `BREATHWISE_VIEWS_DIR` (optional overrides)
- `BREATHWISE_API_CORS_*` (optional CORS tuning)
- `BREATHWISE_API_CACHE_*`, `BREATHWISE_RATE_LIMIT_*` (optional tuning)

## Run
```bash
uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

## Notes
- Do not hardcode credentials.
- Preserve forecast-vs-actual semantic separation.
- Do not join geometry directly into raw city-hourly fact tables.
