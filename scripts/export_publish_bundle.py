from __future__ import annotations

import argparse
import hashlib
import json
import ntpath
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from _shared import discover_project_root, slugify_ascii


DOC_ALLOWLIST = [
    "README.md",
    "SECURITY.md",
    "THIRD_PARTY_SOURCES.md",
    "ANALYTICS_LAYER.md",
    "DATA_PIPELINE_OVERVIEW.md",
    "DATASET_CATALOG.md",
    "DATA_DICTIONARY.md",
    "JOIN_STRATEGY.md",
    "LOCALIZATION_STRATEGY.md",
    "OPERATIONS_RUNBOOK.md",
    "PRODUCT_SHELL_INTEGRATION.md",
]

JSON_METADATA_ALLOWLIST = [
    "data/processed/marts/validation_report.json",
    "data/processed/marts/marts_build_manifest.json",
    "data/processed/cams/cams_city_hourly_manifest.json",
    "data/processed/ops/ops_status_latest.json",
    "data/raw/open_meteo/historical_forecast/manifests/historical_forecast_extended_manifest.json",
]

JSON_POINTER_ALLOWLIST = [
    "data/processed/pipeline_runs/latest_run_manifest.json",
    "data/processed/pipeline_runs/latest_success_run_manifest.json",
]


@dataclass(frozen=True)
class CopiedFile:
    source: str
    destination: str
    sha256: str
    bytes: int
    sanitized: bool


@dataclass(frozen=True)
class LeakFinding:
    file: str
    line: int
    pattern: str


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def rel_path(project_root: Path, path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return resolved.as_posix()


def is_absolute_path_text(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    if text.startswith("/"):
        return True
    if text.startswith("\\\\"):
        return True
    return ntpath.isabs(text)


def looks_local_filesystem_path(project_root: Path, value: str) -> bool:
    text = value.strip()
    if not is_absolute_path_text(text):
        return False
    if text.startswith(("\\\\", "C:\\", "D:\\")):
        return True
    if text.startswith("/Users/"):
        return True
    if text.startswith("/home/"):
        return True
    if text.startswith("/private/"):
        return True
    if text.startswith("/var/"):
        return True
    if text.startswith("/tmp/"):
        return True
    return text.startswith(project_root.resolve().as_posix())


def sanitize_path(project_root: Path, value: object) -> object:
    if not isinstance(value, str):
        return value
    if not is_absolute_path_text(value):
        return value

    try:
        relative = Path(value).resolve().relative_to(project_root.resolve()).as_posix()
    except Exception:
        return "<redacted:absolute-path>"
    return relative or "."


def sanitize_payload(project_root: Path, payload: object, key_name: str | None = None) -> object:
    path_like_key = (
        key_name == "path"
        or key_name == "project_root"
        or (isinstance(key_name, str) and key_name.endswith("_path"))
        or (isinstance(key_name, str) and key_name.endswith("_dir"))
        or (isinstance(key_name, str) and key_name.endswith("_manifest"))
    )

    if isinstance(payload, dict):
        out: dict[str, object] = {}
        for key, value in payload.items():
            key_text = key if isinstance(key, str) else str(key)
            key_is_path_like = (
                key_text in {"path", "project_root"}
                or key_text.endswith("_path")
                or key_text.endswith("_dir")
                or key_text.endswith("_manifest")
            )
            if isinstance(value, (dict, list)):
                out[key_text] = sanitize_payload(project_root, value, key_name=key_text)
            elif path_like_key or key_is_path_like:
                out[key_text] = sanitize_path(project_root, value)
            elif isinstance(value, str) and looks_local_filesystem_path(project_root, value):
                out[key_text] = sanitize_path(project_root, value)
            else:
                out[key_text] = value
        return out

    if isinstance(payload, list):
        return [sanitize_payload(project_root, item, key_name=key_name) for item in payload]

    if isinstance(payload, str) and looks_local_filesystem_path(project_root, payload):
        return sanitize_path(project_root, payload)

    if path_like_key:
        return sanitize_path(project_root, payload)

    return payload


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def scan_absolute_path_leaks(bundle_dir: Path) -> list[LeakFinding]:
    findings: list[LeakFinding] = []
    patterns = [
        (re.compile(r"/Users/"), "/Users/"),
        (re.compile(r"[A-Za-z]:\\\\"), "windows_drive"),
    ]
    for path in bundle_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".json", ".jsonl", ".md", ".txt"}:
            continue
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        for idx, line in enumerate(lines, start=1):
            for regex, label in patterns:
                if regex.search(line):
                    findings.append(
                        LeakFinding(
                            file=path.relative_to(bundle_dir).as_posix(),
                            line=idx,
                            pattern=label,
                        )
                    )
    return findings


def load_json(path: Path) -> dict[str, Any] | list[Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if isinstance(payload, (dict, list)):
        return payload
    return None


def ensure_under(root: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(root.resolve())
    except Exception:
        return False
    return True


def copy_text_file(
    project_root: Path,
    bundle_root: Path,
    source: Path,
    destination: Path,
    copied: list[CopiedFile],
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    copied.append(
        CopiedFile(
            source=rel_path(project_root, source),
            destination=destination.relative_to(bundle_root).as_posix(),
            sha256=sha256_file(destination),
            bytes=destination.stat().st_size,
            sanitized=False,
        )
    )


def copy_json_sanitized(
    project_root: Path,
    bundle_root: Path,
    source: Path,
    destination: Path,
    copied: list[CopiedFile],
) -> None:
    payload = load_json(source)
    if payload is None:
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    sanitized = sanitize_payload(project_root, payload)
    destination.write_text(json.dumps(sanitized, ensure_ascii=False, indent=2), encoding="utf-8")
    copied.append(
        CopiedFile(
            source=rel_path(project_root, source),
            destination=destination.relative_to(bundle_root).as_posix(),
            sha256=sha256_file(destination),
            bytes=destination.stat().st_size,
            sanitized=True,
        )
    )


def resolve_pointed_manifest(project_root: Path, pointer_path: Path) -> Path | None:
    payload = load_json(pointer_path)
    if not isinstance(payload, dict):
        return None
    manifest_rel = payload.get("manifest_path")
    if not isinstance(manifest_rel, str) or not manifest_rel.strip():
        return None
    pipeline_root = project_root / "data" / "processed" / "pipeline_runs"
    candidate = (pipeline_root / manifest_rel).resolve()
    if not ensure_under(pipeline_root, candidate):
        return None
    if not candidate.exists():
        return None
    return candidate


def export_publish_bundle(
    project_root: Path,
    output_root: Path,
    tag: str | None = None,
    overwrite: bool = False,
    allow_unsafe_paths: bool = False,
) -> Path:
    run_tag = slugify_ascii(tag) if isinstance(tag, str) and tag.strip() else datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%SZ").lower()
    bundle_dir = output_root / run_tag

    if bundle_dir.exists():
        if not overwrite:
            raise RuntimeError(f"Bundle directory already exists: {bundle_dir}")
        shutil.rmtree(bundle_dir)

    copied: list[CopiedFile] = []

    docs_dir = bundle_dir / "docs"
    contracts_dir = bundle_dir / "contracts"
    metadata_dir = bundle_dir / "metadata"
    pipeline_dir = bundle_dir / "pipeline"

    for doc in DOC_ALLOWLIST:
        source = project_root / doc
        if source.exists():
            copy_text_file(project_root, bundle_dir, source, docs_dir / doc, copied)

    for contract_path in sorted((project_root / "data" / "contracts").glob("*.json")):
        copy_json_sanitized(project_root, bundle_dir, contract_path, contracts_dir / contract_path.name, copied)

    for rel in JSON_METADATA_ALLOWLIST:
        source = project_root / rel
        if source.exists():
            destination = metadata_dir / Path(rel).name
            copy_json_sanitized(project_root, bundle_dir, source, destination, copied)

    for rel in JSON_POINTER_ALLOWLIST:
        source = project_root / rel
        if not source.exists():
            continue
        copy_json_sanitized(project_root, bundle_dir, source, pipeline_dir / Path(rel).name, copied)
        pointed = resolve_pointed_manifest(project_root, source)
        if pointed is not None:
            copy_json_sanitized(project_root, bundle_dir, pointed, pipeline_dir / pointed.name, copied)

    bundle_manifest = {
        "bundle_id": run_tag,
        "generated_at_utc": now_utc_iso(),
        "project_root": "<redacted:absolute-path>",
        "source_policy": {
            "raw_immutability_preserved": True,
            "absolute_paths_sanitized": True,
            "secrets_included": False,
        },
        "copied_file_count": len(copied),
        "files": [item.__dict__ for item in copied],
    }

    bundle_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = bundle_dir / "bundle_manifest.json"
    manifest_path.write_text(json.dumps(bundle_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_lines = [
        "# Breathwise Publish Bundle",
        "",
        f"- bundle_id: `{run_tag}`",
        f"- generated_at_utc: `{bundle_manifest['generated_at_utc']}`",
        f"- copied_file_count: `{len(copied)}`",
        "- absolute local paths: sanitized",
        "- raw immutable source artifacts: not rewritten",
        "",
        "## Contents",
        "- `docs/`: core project documentation (public-safe)",
        "- `contracts/`: sanitized contract files",
        "- `metadata/`: sanitized validation/ops/manifests",
        "- `pipeline/`: sanitized latest pointer + referenced run manifests",
        "- `bundle_manifest.json`: deterministic export index with checksums",
    ]
    (bundle_dir / "SUMMARY.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    leak_findings = scan_absolute_path_leaks(bundle_dir)
    if leak_findings and not allow_unsafe_paths:
        sample = ", ".join(f"{item.file}:{item.line}" for item in leak_findings[:5])
        raise RuntimeError(
            "Absolute path leak scan failed for publish bundle. "
            f"Findings={len(leak_findings)} sample={sample}"
        )
    return bundle_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a publish-safe Breathwise export bundle (sanitized paths, no secrets)."
    )
    parser.add_argument(
        "--output-dir",
        default="data/processed/publish_exports",
        help="Output directory root for bundles.",
    )
    parser.add_argument(
        "--tag",
        default=None,
        help="Optional bundle tag. Defaults to UTC timestamp slug.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output tag directory if it already exists.",
    )
    parser.add_argument(
        "--allow-unsafe-paths",
        action="store_true",
        help="Allow bundle creation even if absolute-path leak scan finds matches.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = discover_project_root(Path(__file__))
    output_root = Path(args.output_dir).expanduser()
    if not output_root.is_absolute():
        output_root = (project_root / output_root).resolve()

    bundle_dir = export_publish_bundle(
        project_root=project_root,
        output_root=output_root,
        tag=args.tag,
        overwrite=args.overwrite,
        allow_unsafe_paths=args.allow_unsafe_paths,
    )
    print(f"Publish bundle created: {bundle_dir.as_posix()}")


if __name__ == "__main__":
    main()
