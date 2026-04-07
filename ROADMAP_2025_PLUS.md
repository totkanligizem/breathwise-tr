# 2025+ Data Expansion Plan (Strict-Validation)

## Objective
Extend Breathwise data horizon beyond 2024 without weakening current validation strictness.

## Scope
- Historical forecast yearly extension (2025+)
- CAMS monthly/yearly extension
- Marts/views refresh continuity
- Contract stability for API/mobile consumers

## Execution Plan
1. **Partition-first ingest**
   - Keep year/month partition strategy explicit (`YYYY_MM` or yearly manifests).
   - Do not rewrite existing raw partitions.
2. **Coverage guarantees**
   - Preserve full 81-city monthly coverage checks.
   - Fail build when city/month minimum coverage is not met.
3. **Deterministic selection**
   - Keep tie-break rules deterministic for forecast raw JSON selection.
   - Track source manifests per extension run.
4. **Incremental mart refresh**
   - Extend by new partitions only, avoid full rebuild unless required.
5. **Validation gate**
   - Keep duplicate, null, range, and join-key checks strict.
   - Block publish bundle when validation fails.

## Required Checks Per Extension Run
- `pytest -q`
- `python3 scripts/validate_analytics_outputs.py`
- `python3 scripts/check_scheduler_health.py --format text`
- pipeline run manifest status = `succeeded`

## Non-Negotiables
- No forecast/actual semantic mixing
- No duplicate `city_name + time`
- No raw artifact overwrite
- No secret/path leakage in share surfaces
- Preserve canonical release/share discipline in `RELEASE_WORKFLOW.md`
