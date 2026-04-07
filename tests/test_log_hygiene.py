from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if SCRIPTS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR.as_posix())

import rotate_ops_logs as rol  # noqa: E402


def test_rotate_file_rotates_when_size_exceeds_threshold(tmp_path: Path) -> None:
    target = tmp_path / "api_access.jsonl"
    target.write_text("x" * 64, encoding="utf-8")

    result = rol.rotate_file(path=target, max_bytes=10, compress=False)
    assert result["status"] == "rotated"
    assert target.exists()

    archives = list(tmp_path.glob("api_access.*.jsonl"))
    assert len(archives) == 1


def test_prune_archives_keeps_latest_n(tmp_path: Path) -> None:
    target = tmp_path / "alerts_history.jsonl"
    target.write_text("", encoding="utf-8")
    for i in range(5):
        archived = tmp_path / f"alerts_history.20260406T00000{i}Z.jsonl.gz"
        archived.write_text("x", encoding="utf-8")

    result = rol.prune_archives(path=target, retain_archives=2)
    assert result["removed_count"] == 3
    assert len(list(tmp_path.glob("alerts_history.*.jsonl.gz"))) == 2
