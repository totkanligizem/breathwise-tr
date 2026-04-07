from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if SCRIPTS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR.as_posix())

import export_publish_bundle as ep  # noqa: E402


def test_sanitize_payload_redacts_external_absolute_paths(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True, exist_ok=True)
    local_file = project_root / "data" / "x.json"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("{}", encoding="utf-8")

    payload = {
        "project_root": project_root.as_posix(),
        "artifacts": [
            {"manifest_path": local_file.as_posix()},
            {"manifest_path": "/Users/example/private/file.json"},
        ],
    }
    out = ep.sanitize_payload(project_root, payload)

    assert isinstance(out, dict)
    assert out["project_root"] == "."
    artifacts = out["artifacts"]
    assert isinstance(artifacts, list)
    assert artifacts[0]["manifest_path"] == "data/x.json"
    assert artifacts[1]["manifest_path"] == "<redacted:absolute-path>"


def test_export_publish_bundle_smoke(tmp_path: Path) -> None:
    project_root = tmp_path / "breathwise"
    (project_root / "data" / "contracts").mkdir(parents=True, exist_ok=True)
    (project_root / "data" / "processed" / "marts").mkdir(parents=True, exist_ok=True)
    (project_root / "data" / "processed" / "pipeline_runs" / "20260101t000000z").mkdir(
        parents=True, exist_ok=True
    )
    (project_root / "scripts").mkdir(parents=True, exist_ok=True)
    (project_root / "README.md").write_text("# Demo\n", encoding="utf-8")
    (project_root / "SECURITY.md").write_text("# Security\n", encoding="utf-8")
    (project_root / "THIRD_PARTY_SOURCES.md").write_text("# Sources\n", encoding="utf-8")
    (project_root / "data" / "contracts" / "api_health_response.schema.json").write_text(
        json.dumps({"path": "/Users/demo/secret/path"}),
        encoding="utf-8",
    )
    (project_root / "data" / "processed" / "marts" / "validation_report.json").write_text(
        json.dumps({"passed": True}),
        encoding="utf-8",
    )
    pointer_path = project_root / "data" / "processed" / "pipeline_runs" / "latest_run_manifest.json"
    pointer_path.write_text(
        json.dumps(
            {
                "run_id": "20260101t000000z",
                "manifest_path": "20260101t000000z/run_manifest.json",
                "status": "succeeded",
            }
        ),
        encoding="utf-8",
    )
    (project_root / "data" / "processed" / "pipeline_runs" / "20260101t000000z" / "run_manifest.json").write_text(
        json.dumps({"project_root": "/Users/demo/workspace"}),
        encoding="utf-8",
    )

    out_root = tmp_path / "exports"
    bundle_dir = ep.export_publish_bundle(project_root=project_root, output_root=out_root, tag="unit", overwrite=False)

    manifest_path = bundle_dir / "bundle_manifest.json"
    assert manifest_path.exists()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["bundle_id"] == "unit"
    assert payload["copied_file_count"] >= 4

    contract_copy = bundle_dir / "contracts" / "api_health_response.schema.json"
    assert contract_copy.exists()
    contract_payload = json.loads(contract_copy.read_text(encoding="utf-8"))
    assert contract_payload["path"] == "<redacted:absolute-path>"
