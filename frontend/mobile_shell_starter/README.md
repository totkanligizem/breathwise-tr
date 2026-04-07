# Breathwise Mobile Shell (Expo)

Runnable mobile/web shell for Breathwise TR using real backend contracts (no mock-only UI flow).

## What Is Included
- Expo app shell with TR/EN localization
- City current overview
- City timeline (`hourly`, `daily`, `weekly`)
- Province choropleth map + ranking/detail
- Settings and locale switch
- Environment-based backend/auth config

## Stack
- Expo + React Native + React Native Web
- TypeScript
- React Native SVG (map + visual primitives)
- AsyncStorage (locale persistence)

## Project Layout
- `App.tsx`: app shell + top-level navigation
- `src/api/`: API client and endpoint wrappers
- `src/config/`: environment helpers
- `src/state/`: locale context + persisted preference
- `src/i18n/`: localization and fallback catalog
- `src/screens/`: City, Timeline, Province, Settings screens
- `src/components/`: shared cards/charts/controls/map component
- `src/viewModels/`: backend payload -> UI mapping
- `src/assets/tr_adm1_map_lite.json`: lightweight map geometry

## Environment Setup
```bash
cp .env.example .env
```

Required:
- `EXPO_PUBLIC_BREATHWISE_API_BASE_URL`
  - same machine web example: `http://127.0.0.1:8000`
  - device example: `http://<YOUR_LOCAL_IP>:8000`

Optional (trusted internal/shared usage only):
- `EXPO_PUBLIC_BREATHWISE_API_KEY`

Do not hardcode secrets in source files.

## Run
```bash
npm install
npm run start
```

Useful commands:
```bash
npm run web
npm run typecheck
```

## Backend Integration
Endpoints used:
- `GET /v1/meta/localization`
- `GET /v1/mobile/cities/current`
- `GET /v1/mobile/cities/{city_name}/timeline`
- `GET /v1/mobile/provinces/map-metrics`

If backend auth is enabled, set `EXPO_PUBLIC_BREATHWISE_API_KEY`.

## Localization Model (TR/EN)
- Translation-key-first mapping
- Locale selected in Settings and persisted across restarts
- Fallback order:
  1. localized translation key
  2. server label
  3. safe placeholder (`—`)

## Notes
- Weather mood visuals are data-driven (no unconditional rain overlay).
- Choropleth map is optimized for app performance with a lightweight asset.
- For CORS differences in web mode, see root runbook: `OPERATIONS_RUNBOOK.md`.
