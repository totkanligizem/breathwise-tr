# Localization Strategy (TR/EN)

## Scope
Breathwise TR user-facing contracts are localization-ready for:
- Turkish (`tr-TR`)
- English (`en-US`)

This strategy applies to API payload contracts, mobile-facing views, and future frontend integration.

## Principles
- Keep data semantics language-neutral.
- Represent UI text via stable translation keys.
- Optionally include localized label values for operator/app convenience.
- Do not hardcode secrets or language-specific behavior in code paths that should remain generic.

## Contract Sources
- Primary localization contract:
  - `data/contracts/i18n_contract.json`
- Product shell mapping contract:
  - `data/contracts/product_shell_view_models.json`
- API metadata endpoint:
  - `GET /v1/meta/localization`
  - optional: `?include_translations=true&max_keys=200`

## Locale Resolution
- API endpoints accept locale query where relevant: `locale`.
- Supported values include canonical and aliases:
  - canonical: `tr-TR`, `en-US`
  - aliases: `tr`, `en`, underscore variants
- Invalid locale returns `422`.
- Resolved locale is returned via `Content-Language` response header.

## Translation Key Pattern
- Pattern:
  - `<domain>.<entity>.<label>`
- Examples:
  - `aq.category.good`
  - `alert.aq.warning`
  - `alert.heat.clear`

## App-Facing Payload Fields
- City current snapshot:
  - `aq_category_key`
  - `aq_category_label`
  - `label_locale`
- Province map metrics (web/mobile):
  - `aq_alert_key`
  - `aq_alert_label`
  - `heat_alert_key`
  - `heat_alert_label`
  - `label_locale`

These fields are additive and non-breaking relative to existing payload semantics.

## Client Integration Guidance
- Prefer keys as the primary binding in frontend/mobile state.
- Use label fields as convenience/fallback.
- Keep client translation bundles aligned with `i18n_contract.json`.
- Treat missing label as a recoverable condition and render from key where possible.
- Persist selected locale preference in app storage and restore on boot before first data fetch when possible.

## Future Expansion
- Extend endpoint-level localization for additional user-visible fields.
- Add weather-code translation dictionary and icon mappings.
- Add pluralization/context variants where UI copy requires it.
- Keep translation key namespaces stable to avoid client breakage.
