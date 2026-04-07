from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if SCRIPTS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR.as_posix())

import build_mobile_province_map_asset as mba  # noqa: E402


def test_payloads_equivalent_ignores_generated_at() -> None:
    left = {
        "metadata": {"generated_at_utc": "2026-01-01T00:00:00+00:00", "source_geojson": "x"},
        "view_box": {"width": 1, "height": 1},
        "features": [],
    }
    right = {
        "metadata": {"generated_at_utc": "2027-01-01T00:00:00+00:00", "source_geojson": "x"},
        "view_box": {"width": 1, "height": 1},
        "features": [],
    }
    assert mba.payloads_equivalent(left, right)


def test_builder_check_equivalent_with_current_asset() -> None:
    source_geojson = PROJECT_ROOT / "data" / "processed" / "geography" / "adm1_provinces_tr.geojson"
    output = PROJECT_ROOT / "frontend" / "mobile_shell_starter" / "src" / "assets" / "tr_adm1_map_lite.json"

    expected = mba.build_asset_payload(
        source_geojson=source_geojson,
        source_reference="data/processed/geography/adm1_provinces_tr.geojson",
        epsilon_degrees=0.01,
        width=860,
        height=500,
    )
    current = json.loads(output.read_text(encoding="utf-8"))
    assert mba.payloads_equivalent(expected, current)


def test_source_reference_for_metadata_is_project_relative() -> None:
    source_geojson = PROJECT_ROOT / "data" / "processed" / "geography" / "adm1_provinces_tr.geojson"
    reference = mba.source_reference_for_metadata(PROJECT_ROOT, source_geojson)
    assert reference == "data/processed/geography/adm1_provinces_tr.geojson"
