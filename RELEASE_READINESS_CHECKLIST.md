# Product Release Readiness Checklist

Canonical reference:
- `RELEASE_WORKFLOW.md`

## 1) Data and API Health
- [ ] `python3 scripts/validate_analytics_outputs.py` passes
- [ ] `pytest -q` passes
- [ ] `npm run -s typecheck` passes (`frontend/mobile_shell_starter`)
- [ ] API smoke passes (auth off + auth on)
- [ ] scheduler health is `healthy`

## 2) Security and Privacy
- [ ] No secrets in tracked files (`.env` ignored, env-only credentials)
- [ ] Publish-safe metadata review completed (`publish_safe=true`)
- [ ] Public bundle generated with `scripts/export_publish_bundle.py`
- [ ] Absolute local path leak scan on bundle is clean

## 3) Product UX (Web/Mobile)
- [ ] TR/EN locale switching and fallback verified
- [ ] Core screens load real backend data
- [ ] Weather visuals reflect real conditions (no misleading overlays)
- [ ] Empty/loading/error states are user-friendly

## 4) Operational Readiness
- [ ] Scheduler templates configured (cron/launchd/systemd as needed)
- [ ] Alert hook settings verified
- [ ] Log retention/rotation policy active
- [ ] Latest successful run pointer is current

## 5) Share/Publish Artifacts
- [ ] Contracts exported and version-checked
- [ ] README (TR) + README.eng.md + runbook updated for current release
- [ ] Third-party source attribution included
- [ ] Known limitations documented transparently
- [ ] Share package sourced only from `data/processed/publish_exports/<tag>/`

## Blocker Rule
- If any unchecked item remains, release/share is blocked.
