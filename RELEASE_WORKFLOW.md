# Release and Share Workflow (Canonical)

This is the canonical workflow for Breathwise TR internal review, portfolio packaging, and controlled public sharing.

## 1) Stable Baseline Definition
Release/share actions start only from a green baseline:
- `pytest -q` passes
- `python3 scripts/validate_analytics_outputs.py` passes
- `npm run -s typecheck` passes (`frontend/mobile_shell_starter`)
- `python3 scripts/check_scheduler_health.py --format text` reports `overall_status: healthy`
- latest pipeline run status is `succeeded` (or latest successful pointer is current)

## 2) Pre-Release Validation Commands
Run in this order:
```bash
pytest -q
python3 scripts/validate_analytics_outputs.py
python3 scripts/check_scheduler_health.py --format text
cd frontend/mobile_shell_starter && npm run -s typecheck && cd ../..
```

Optional but recommended:
```bash
python3 scripts/smoke_test_api.py
```

## 3) Canonical Export Command
Generate sanitized release-candidate bundle:
```bash
python3 scripts/export_publish_bundle.py --tag release_candidate --overwrite
```

Canonical output:
- `data/processed/publish_exports/release_candidate/`

## 4) Bundle Inspection (Before Sharing)
Quick checks:
```bash
ls -la data/processed/publish_exports/release_candidate
cat data/processed/publish_exports/release_candidate/bundle_manifest.json
rg -n "/Users/|[A-Za-z]:\\\\" data/processed/publish_exports/release_candidate
```

Expected:
- leak scan result is empty
- bundle manifest exists and lists copied sanitized artifacts

## 5) What Is Safe to Share
Share from bundle only:
- `data/processed/publish_exports/<tag>/docs/`
- `data/processed/publish_exports/<tag>/contracts/`
- `data/processed/publish_exports/<tag>/metadata/`
- `data/processed/publish_exports/<tag>/pipeline/`
- `data/processed/publish_exports/<tag>/bundle_manifest.json`
- `data/processed/publish_exports/<tag>/SUMMARY.md`

## 6) What Must Not Be Shared by Default
Do not share directly:
- `data/raw/**` (immutable source payloads)
- `data/processed/pipeline_runs/**` (internal run traces)
- local `.env*` files
- local runtime logs in `data/processed/api_logs/` and `data/processed/alerts/`
- unsanitized screenshots/logs that expose local machine paths, local IP/topology, or secrets

## 7) Failure Handling
If export fails:
1. Read error sample paths/lines.
2. Inspect offending files and sanitize source surface (not raw immutable data).
3. Re-run export command.
4. Do not share until export passes.

If validation fails:
1. Stop release/share flow.
2. Fix failing checks.
3. Re-run full pre-release validation.
4. Export only after green baseline is restored.

If checklist is incomplete:
1. Mark release candidate as blocked.
2. Complete missing checks.
3. Re-run export to produce final package.

## 8) Manual vs Automated
Automated:
- test/validation/typecheck commands
- scheduler health check
- sanitized export generation
- absolute-path leak fail gate in export step

Manual:
- final checklist sign-off
- external share decision
- selection of screenshot/media assets for safe publication

## 9) Deferred (Intentional)
- Full CI-based release orchestration (local-first remains primary)
- External notification channels for release events
- Automated asset redaction workflow for screenshots/media
