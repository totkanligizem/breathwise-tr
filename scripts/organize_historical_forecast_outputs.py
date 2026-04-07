from __future__ import annotations

import argparse
import hashlib
import re
import shutil
from pathlib import Path

import pandas as pd
from _shared import discover_project_root


PROJECT_ROOT = discover_project_root(Path(__file__))
DATA_ROOT = PROJECT_ROOT / "data" / "raw" / "open_meteo"

HIST_FC_BASE_DIR = DATA_ROOT / "historical_forecast"
RAW_DIR = HIST_FC_BASE_DIR / "raw_json"
TIDY_DIR = HIST_FC_BASE_DIR / "tidy"
MANIFEST_DIR = HIST_FC_BASE_DIR / "manifests"

RAW_EXPERIMENTAL_DAILY_DIR = RAW_DIR / "experimental" / "daily_chunks"
RAW_EXPERIMENTAL_MISC_DIR = RAW_DIR / "experimental" / "misc"
RAW_VALIDATED_MONTHLY_DIR = RAW_DIR / "validated" / "monthly_chunks"
RAW_CANONICAL_SHORT_RANGE_DIR = RAW_DIR / "canonical" / "short_range"

TIDY_EXPERIMENTAL_DIR = TIDY_DIR / "experimental"
TIDY_VALIDATED_DIR = TIDY_DIR / "validated"
TIDY_CANONICAL_DIR = TIDY_DIR / "canonical"

CANONICAL_SHORT_RANGE_FILES = {
    "full": "historical_forecast_hourly_tr_2024_01_15_2024_01_21_full.csv",
    "light": "historical_forecast_hourly_tr_2024_01_15_2024_01_21_light.csv",
}

RAW_DAILY_PATTERN = re.compile(r".+_D\d{3}\.json$")
RAW_MONTHLY_PATTERN = re.compile(r".+_\d{4}_\d{2}_historical_forecast_\d{8}_\d{6}\.json$")


def ensure_structure() -> None:
    for folder in [
        RAW_EXPERIMENTAL_DAILY_DIR,
        RAW_EXPERIMENTAL_MISC_DIR,
        RAW_VALIDATED_MONTHLY_DIR,
        RAW_CANONICAL_SHORT_RANGE_DIR,
        TIDY_EXPERIMENTAL_DIR,
        TIDY_VALIDATED_DIR,
        TIDY_CANONICAL_DIR,
        MANIFEST_DIR,
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def classify_legacy_raw_file(path: Path) -> Path:
    if RAW_DAILY_PATTERN.match(path.name):
        return RAW_EXPERIMENTAL_DAILY_DIR / path.name
    if RAW_MONTHLY_PATTERN.match(path.name):
        return RAW_VALIDATED_MONTHLY_DIR / path.name
    return RAW_EXPERIMENTAL_MISC_DIR / path.name


def classify_legacy_tidy_file(path: Path) -> Path:
    if path.name in CANONICAL_SHORT_RANGE_FILES.values():
        return TIDY_CANONICAL_DIR / path.name
    if path.name.startswith("historical_forecast_hourly_tr_"):
        return TIDY_EXPERIMENTAL_DIR / path.name
    if path.name.startswith("historical_forecast_failures_tr_"):
        return TIDY_EXPERIMENTAL_DIR / path.name
    return TIDY_VALIDATED_DIR / path.name


def move_legacy_files(apply: bool) -> dict[str, int]:
    moved = {"raw": 0, "tidy": 0, "base": 0}

    for raw_path in sorted(RAW_DIR.glob("*.json")):
        target = classify_legacy_raw_file(raw_path)
        if apply:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(raw_path), str(target))
        moved["raw"] += 1

    for tidy_path in sorted(TIDY_DIR.glob("*.csv")):
        target = classify_legacy_tidy_file(tidy_path)
        if apply:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(tidy_path), str(target))
        moved["tidy"] += 1

    for base_csv in sorted(HIST_FC_BASE_DIR.glob("*.csv")):
        target = TIDY_EXPERIMENTAL_DIR / base_csv.name
        if apply:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(base_csv), str(target))
        moved["base"] += 1

    return moved


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_canonical_manifest() -> Path:
    rows: list[dict[str, object]] = []

    for variant, filename in CANONICAL_SHORT_RANGE_FILES.items():
        path = TIDY_CANONICAL_DIR / filename
        if not path.exists():
            rows.append(
                {
                    "variant": variant,
                    "path": str(path.relative_to(PROJECT_ROOT)),
                    "status": "missing",
                    "rows": 0,
                    "city_count": 0,
                    "time_min": None,
                    "time_max": None,
                    "sha256": None,
                }
            )
            continue

        df = pd.read_csv(path)
        rows.append(
            {
                "variant": variant,
                "path": str(path.relative_to(PROJECT_ROOT)),
                "status": "present",
                "rows": int(len(df)),
                "city_count": int(df["city_name"].nunique()) if "city_name" in df.columns else 0,
                "time_min": df["time"].min() if "time" in df.columns and not df.empty else None,
                "time_max": df["time"].max() if "time" in df.columns and not df.empty else None,
                "sha256": file_sha256(path),
            }
        )

    manifest_df = pd.DataFrame(rows)
    manifest_path = MANIFEST_DIR / "historical_forecast_canonical_short_range_manifest.csv"
    manifest_df.to_csv(manifest_path, index=False, encoding="utf-8")
    return manifest_path


def build_inventory_report() -> Path:
    rows: list[dict[str, object]] = []
    for folder in [
        RAW_EXPERIMENTAL_DAILY_DIR,
        RAW_EXPERIMENTAL_MISC_DIR,
        RAW_VALIDATED_MONTHLY_DIR,
        RAW_CANONICAL_SHORT_RANGE_DIR,
        TIDY_EXPERIMENTAL_DIR,
        TIDY_VALIDATED_DIR,
        TIDY_CANONICAL_DIR,
    ]:
        files = sorted(folder.glob("*"))
        file_count = sum(1 for p in files if p.is_file())
        rows.append(
            {
                "folder": str(folder.relative_to(PROJECT_ROOT)),
                "file_count": file_count,
            }
        )
    report_df = pd.DataFrame(rows)
    report_path = MANIFEST_DIR / "historical_forecast_folder_inventory.csv"
    report_df.to_csv(report_path, index=False, encoding="utf-8")
    return report_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Organize historical_forecast raw/tidy outputs into experimental, "
            "validated, and canonical folders."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply file moves. Without this flag, only prints what would happen.",
    )
    args = parser.parse_args()

    ensure_structure()
    moved = move_legacy_files(apply=args.apply)
    manifest_path = build_canonical_manifest()
    report_path = build_inventory_report()

    mode = "APPLIED" if args.apply else "DRY-RUN"
    print(f"[{mode}] legacy raw files classified: {moved['raw']}")
    print(f"[{mode}] legacy tidy files classified: {moved['tidy']}")
    print(f"[{mode}] root-level historical_forecast csv files classified: {moved['base']}")
    print(f"Canonical manifest: {manifest_path}")
    print(f"Folder inventory: {report_path}")


if __name__ == "__main__":
    main()
