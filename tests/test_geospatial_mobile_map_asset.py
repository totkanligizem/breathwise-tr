from __future__ import annotations

import json
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ASSET_PATH = PROJECT_ROOT / "frontend" / "mobile_shell_starter" / "src" / "assets" / "tr_adm1_map_lite.json"
MOBILE_PROVINCE_VIEW_PATH = PROJECT_ROOT / "data" / "processed" / "views" / "mobile_province_map_metrics_tr_light.parquet"


def test_mobile_map_asset_shape_and_metadata() -> None:
    payload = json.loads(ASSET_PATH.read_text(encoding="utf-8"))
    metadata = payload.get("metadata", {})
    features = payload.get("features", [])
    view_box = payload.get("view_box", {})

    assert metadata.get("source_geojson") == "data/processed/geography/adm1_provinces_tr.geojson"
    assert metadata.get("feature_count") == 81
    assert isinstance(features, list)
    assert len(features) == 81

    assert isinstance(view_box, dict)
    assert int(view_box.get("width", 0)) > 0
    assert int(view_box.get("height", 0)) > 0

    province_names = set()
    shape_isos = set()
    for item in features:
        assert isinstance(item.get("province_name"), str) and item["province_name"].strip()
        assert isinstance(item.get("shape_iso"), str) and item["shape_iso"].startswith("TR-")
        assert isinstance(item.get("path"), str) and item["path"].strip().startswith("M ")
        centroid = item.get("centroid")
        assert isinstance(centroid, list) and len(centroid) == 2
        province_names.add(item["province_name"])
        shape_isos.add(item["shape_iso"])

    assert len(province_names) == 81
    # Some upstream geography snapshots may have a repeated shape_iso token.
    assert len(shape_isos) >= 80


def test_mobile_map_asset_matches_mobile_province_view_coverage() -> None:
    payload = json.loads(ASSET_PATH.read_text(encoding="utf-8"))
    feature_pairs = {(item["province_name"], item["shape_iso"]) for item in payload.get("features", [])}

    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            select province_name, shape_iso
            from read_parquet('{MOBILE_PROVINCE_VIEW_PATH.as_posix()}')
            """
        ).fetchall()
    finally:
        con.close()

    view_pairs = {(province_name, shape_iso) for province_name, shape_iso in rows}
    assert len(view_pairs) == 81
    assert feature_pairs == view_pairs
