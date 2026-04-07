from __future__ import annotations

import argparse
import gzip
import shutil
from datetime import datetime, timezone
from pathlib import Path

from _shared import discover_project_root


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def compress_file(path: Path) -> Path:
    gz_path = path.with_suffix(path.suffix + ".gz")
    with path.open("rb") as src, gzip.open(gz_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    path.unlink(missing_ok=True)
    return gz_path


def archive_glob(path: Path) -> str:
    # api_access.jsonl -> api_access.*.jsonl*
    return f"{path.stem}.*{path.suffix}*"


def rotate_file(path: Path, max_bytes: int, compress: bool) -> dict[str, object]:
    if not path.exists():
        return {"path": path.as_posix(), "status": "missing"}

    size = path.stat().st_size
    if size <= max_bytes:
        return {"path": path.as_posix(), "status": "skipped", "size_bytes": size}

    stamped = path.with_name(f"{path.stem}.{now_stamp()}{path.suffix}")
    path.rename(stamped)
    archived = compress_file(stamped) if compress else stamped
    # Recreate active file for writers.
    path.touch(exist_ok=True)
    return {
        "path": path.as_posix(),
        "status": "rotated",
        "size_bytes": size,
        "archived_path": archived.as_posix(),
    }


def prune_archives(path: Path, retain_archives: int) -> dict[str, object]:
    archives = sorted(path.parent.glob(archive_glob(path)), key=lambda p: p.stat().st_mtime, reverse=True)
    removed: list[str] = []
    for stale in archives[retain_archives:]:
        stale.unlink(missing_ok=True)
        removed.append(stale.as_posix())
    return {
        "path": path.as_posix(),
        "retain_archives": retain_archives,
        "archive_count": len(archives),
        "removed_count": len(removed),
        "removed_paths": removed,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rotate and prune Breathwise local operational logs."
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=5_000_000,
        help="Rotate a log file when size exceeds this threshold.",
    )
    parser.add_argument(
        "--retain-archives",
        type=int,
        default=14,
        help="Keep this many archived log files per target.",
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Disable gzip compression for rotated archives.",
    )
    parser.add_argument(
        "--targets",
        default="data/processed/api_logs/api_access.jsonl,data/processed/alerts/alerts_history.jsonl",
        help="Comma-separated log file paths (relative to project root or absolute).",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.max_bytes <= 0:
        raise ValueError("--max-bytes must be positive")
    if args.retain_archives < 0:
        raise ValueError("--retain-archives must be >= 0")

    project_root = discover_project_root(Path(__file__))
    compress = not args.no_compress
    target_values = [item.strip() for item in args.targets.split(",") if item.strip()]
    targets: list[Path] = []
    for raw in target_values:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (project_root / path).resolve()
        targets.append(path)

    print("Breathwise ops log rotation")
    print(f"- max_bytes={args.max_bytes}")
    print(f"- retain_archives={args.retain_archives}")
    print(f"- compress={compress}")
    print(f"- dry_run={args.dry_run}")

    for path in targets:
        if args.dry_run:
            exists = path.exists()
            size = path.stat().st_size if exists else None
            print(f"[dry-run] target={path} exists={exists} size={size}")
            continue

        rotate_result = rotate_file(path=path, max_bytes=args.max_bytes, compress=compress)
        prune_result = prune_archives(path=path, retain_archives=args.retain_archives)
        print(f"- rotate: {rotate_result}")
        print(f"- prune: {prune_result}")


if __name__ == "__main__":
    main()
