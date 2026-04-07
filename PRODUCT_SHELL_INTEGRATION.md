# Product Shell Integration Guide

## Objective
This document defines a lightweight, bilingual (TR/EN), mobile-first product shell around the existing Breathwise TR backend.

The goal is to enable real app-shell implementation without changing core data/backend/ops guarantees.

## Information Architecture

Screen set (v1 shell):
1. `city_current_overview`
2. `city_hourly_timeline`
3. `province_map_metrics`
4. `settings_locale`

Optional next surfaces:
1. AQ-focused detail card stack
2. forecast-vs-actual insight card stream
3. "operator-visible status" panel for internal builds

## Backend Mapping

Primary endpoints:
- `GET /v1/mobile/cities/current?city_name=&limit=&locale=`
- `GET /v1/mobile/cities/{city_name}/timeline?start=&end=&limit=`
- `GET /v1/mobile/provinces/map-metrics?limit=&locale=`
- `GET /v1/meta/localization`

Operational visibility:
- `GET /v1/meta/ops-status?compact=true` (internal/admin UX)

## Bilingual UX Model (TR/EN)

### Translation-Key-First
- UI binds to translation keys as the stable contract.
- Backend includes key fields where user-facing labels matter:
  - `aq_category_key`
  - `aq_alert_key`
  - `heat_alert_key`

### Localized Label Convenience
- Backend also provides label convenience fields:
  - `aq_category_label`
  - `aq_alert_label`
  - `heat_alert_label`
- `label_locale` indicates which locale these labels represent.

### Locale Switching
- User locale state drives `locale` query on localization-aware endpoints.
- Recommended support:
  - canonical: `tr-TR`, `en-US`
  - aliases: `tr`, `en`
- Resolved locale should be read from `Content-Language`.
- Invalid locale currently returns `422`; client should normalize locale first and retry with default locale if needed.

### Fallback Rules
1. Contract translation for key in selected locale.
2. Server-provided label field.
3. Safe placeholder.

## Product-Facing View Models

Product contracts are exported in:
- `data/contracts/product_shell_view_models.json`

Screen-level shaping:
- City current overview:
  - status cards, AQ chip, update time, temperature and AQ summary
- City hourly timeline:
  - hourly chart series for forecast temp and AQ
- Province map:
  - choropleth metric + AQ/heat alert chips
- Settings locale:
  - locale picker driven by `/v1/meta/localization`

## React Native / Expo Readiness

Starter structure:
- `frontend/mobile_shell_starter/`

Includes:
- API client skeleton
- localization resolver
- backend payload types
- view model mapper layer
- screen map constants
- runnable Expo shell (`App.tsx`, `package.json`, `app.json`)

Run commands:
```bash
cd frontend/mobile_shell_starter
cp .env.example .env
npm install
npm run start
```

Env requirements:
- `EXPO_PUBLIC_BREATHWISE_API_BASE_URL`
- optional trusted internal mode: `EXPO_PUBLIC_BREATHWISE_API_KEY`

Recommended state split:
1. `session` (auth header source, build mode)
2. `preferences` (selected locale)
3. `query cache` (screen data by endpoint + params)

Recommended fetch pattern:
- include `locale` where endpoint supports localization
- keep response models thin at screen boundary
- derive UI-only formatting in view model mapper layer

## Shared-Mode Security Notes
- Keep all secrets environment-based.
- Never commit API keys.
- For public distribution, do not ship static backend API keys in client bundles.
- For internal test distribution, temporary environment-based key usage is acceptable with rotation.

## Future Scope
1. endpoint-level localization propagation to additional user-visible fields
2. frontend translation bundle sync automation against `i18n_contract.json`
3. richer insight surfaces (forecast vs actual narratives)
4. deployment-side auth/token exchange for public mobile rollout
