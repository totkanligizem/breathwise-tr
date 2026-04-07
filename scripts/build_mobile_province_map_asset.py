from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from _shared import discover_project_root


@dataclass(frozen=True)
class Bounds:
    min_lon: float
    max_lon: float
    min_lat: float
    max_lat: float


def _load_geojson(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Invalid GeoJSON payload type: {type(payload)!r}")
    features = payload.get("features")
    if not isinstance(features, list) or not features:
        raise RuntimeError("GeoJSON does not contain a valid features array.")
    return payload


def _iter_all_points(geometry: dict) -> Iterable[tuple[float, float]]:
    gtype = geometry.get("type")
    coords = geometry.get("coordinates")

    if gtype == "Polygon":
        for ring in coords:
            for lon, lat in ring:
                yield float(lon), float(lat)
        return

    if gtype == "MultiPolygon":
        for polygon in coords:
            for ring in polygon:
                for lon, lat in ring:
                    yield float(lon), float(lat)
        return

    raise RuntimeError(f"Unsupported geometry type for map asset: {gtype!r}")


def _bounds_from_features(features: list[dict]) -> Bounds:
    min_lon = math.inf
    max_lon = -math.inf
    min_lat = math.inf
    max_lat = -math.inf

    for feature in features:
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict):
            continue
        for lon, lat in _iter_all_points(geometry):
            min_lon = min(min_lon, lon)
            max_lon = max(max_lon, lon)
            min_lat = min(min_lat, lat)
            max_lat = max(max_lat, lat)

    if not (math.isfinite(min_lon) and math.isfinite(max_lon) and math.isfinite(min_lat) and math.isfinite(max_lat)):
        raise RuntimeError("Could not derive finite map bounds from features.")

    return Bounds(min_lon=min_lon, max_lon=max_lon, min_lat=min_lat, max_lat=max_lat)


def _project_point(
    lon: float,
    lat: float,
    bounds: Bounds,
    width: int,
    height: int,
) -> tuple[float, float]:
    lon_span = bounds.max_lon - bounds.min_lon or 1.0
    lat_span = bounds.max_lat - bounds.min_lat or 1.0
    x = ((lon - bounds.min_lon) / lon_span) * width
    y = ((bounds.max_lat - lat) / lat_span) * height
    return x, y


def _distance_to_segment(point: tuple[float, float], start: tuple[float, float], end: tuple[float, float]) -> float:
    px, py = point
    sx, sy = start
    ex, ey = end
    dx = ex - sx
    dy = ey - sy
    if dx == 0 and dy == 0:
        return math.hypot(px - sx, py - sy)
    t = ((px - sx) * dx + (py - sy) * dy) / (dx * dx + dy * dy)
    proj_x = sx + t * dx
    proj_y = sy + t * dy
    return math.hypot(px - proj_x, py - proj_y)


def _rdp(points: list[tuple[float, float]], epsilon: float) -> list[tuple[float, float]]:
    if len(points) <= 2:
        return points

    start = points[0]
    end = points[-1]
    max_distance = -1.0
    max_index = 0

    for idx in range(1, len(points) - 1):
        dist = _distance_to_segment(points[idx], start, end)
        if dist > max_distance:
            max_distance = dist
            max_index = idx

    if max_distance > epsilon:
        left = _rdp(points[: max_index + 1], epsilon)
        right = _rdp(points[max_index:], epsilon)
        return left[:-1] + right

    return [start, end]


def _simplify_ring(ring: list[list[float]], epsilon_degrees: float) -> list[tuple[float, float]]:
    if len(ring) < 5:
        return [(float(x), float(y)) for x, y in ring]

    as_points = [(float(x), float(y)) for x, y in ring]
    closed = as_points[0] == as_points[-1]
    body = as_points[:-1] if closed else as_points[:]
    simplified = _rdp(body, epsilon_degrees)
    if len(simplified) < 3:
        simplified = body[:3]
    if closed:
        simplified.append(simplified[0])
    if len(simplified) < 4:
        return as_points[:4]
    return simplified


def _polygon_rings(geometry: dict) -> list[list[list[float]]]:
    gtype = geometry.get("type")
    coords = geometry.get("coordinates")

    if gtype == "Polygon":
        return [coords[0]]
    if gtype == "MultiPolygon":
        return [polygon[0] for polygon in coords]
    raise RuntimeError(f"Unsupported geometry type for map asset: {gtype!r}")


def _ring_to_path(
    ring: list[tuple[float, float]],
    bounds: Bounds,
    width: int,
    height: int,
) -> str:
    points = [_project_point(lon, lat, bounds=bounds, width=width, height=height) for lon, lat in ring]
    if len(points) < 3:
        return ""
    path_tokens = [f"M {points[0][0]:.1f} {points[0][1]:.1f}"]
    for x, y in points[1:]:
        path_tokens.append(f"L {x:.1f} {y:.1f}")
    path_tokens.append("Z")
    return " ".join(path_tokens)


def _centroid_of_ring(
    ring: list[tuple[float, float]],
    bounds: Bounds,
    width: int,
    height: int,
) -> tuple[float, float]:
    projected = [_project_point(lon, lat, bounds=bounds, width=width, height=height) for lon, lat in ring]
    if not projected:
        return width / 2.0, height / 2.0
    x_sum = sum(x for x, _ in projected)
    y_sum = sum(y for _, y in projected)
    count = max(1, len(projected))
    return x_sum / count, y_sum / count


def build_asset_payload(
    source_geojson: Path,
    source_reference: str,
    epsilon_degrees: float,
    width: int,
    height: int,
) -> dict:
    payload = _load_geojson(source_geojson)
    features = payload["features"]
    bounds = _bounds_from_features(features)

    output_features: list[dict] = []
    total_path_count = 0
    total_vertex_count = 0
    shape_iso_counts: dict[str, int] = {}

    for feature in features:
        props = feature.get("properties") or {}
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict):
            continue

        province_name = props.get("province_name")
        shape_iso = props.get("shape_iso")
        if not isinstance(province_name, str) or not province_name.strip():
            continue
        if not isinstance(shape_iso, str) or not shape_iso.strip():
            continue

        ring_paths: list[str] = []
        centroid_ring: list[tuple[float, float]] | None = None
        for raw_ring in _polygon_rings(geometry):
            simplified = _simplify_ring(raw_ring, epsilon_degrees=epsilon_degrees)
            total_vertex_count += len(simplified)
            path = _ring_to_path(simplified, bounds=bounds, width=width, height=height)
            if not path:
                continue
            ring_paths.append(path)
            centroid_ring = centroid_ring or simplified

        if not ring_paths:
            continue

        total_path_count += len(ring_paths)
        cx, cy = _centroid_of_ring(centroid_ring or [], bounds=bounds, width=width, height=height)

        output_features.append(
            {
                "province_name": province_name.strip(),
                "shape_iso": shape_iso.strip(),
                "path": " ".join(ring_paths),
                "centroid": [round(cx, 1), round(cy, 1)],
            }
        )
        shape_iso_counts[shape_iso.strip()] = shape_iso_counts.get(shape_iso.strip(), 0) + 1

    output_features.sort(key=lambda item: item["province_name"])
    duplicate_shape_iso_count = sum(1 for count in shape_iso_counts.values() if count > 1)

    result = {
        "metadata": {
            "source_geojson": source_reference,
            "feature_count": len(output_features),
            "path_count": total_path_count,
            "vertex_count": total_vertex_count,
            "shape_iso_duplicate_count": duplicate_shape_iso_count,
            "epsilon_degrees": epsilon_degrees,
            "generated_at_utc": datetime_utc_now(),
        },
        "view_box": {"width": width, "height": height},
        "features": output_features,
    }
    return result


def write_asset(output_path: Path, payload: dict) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def payload_for_compare(payload: dict) -> dict:
    clone = json.loads(json.dumps(payload))
    metadata = clone.get("metadata")
    if isinstance(metadata, dict):
        metadata.pop("generated_at_utc", None)
    return clone


def payloads_equivalent(left: dict, right: dict) -> bool:
    return payload_for_compare(left) == payload_for_compare(right)


def source_reference_for_metadata(project_root: Path, source_geojson: Path) -> str:
    resolved = source_geojson.resolve()
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return resolved.as_posix()


def datetime_utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build lightweight province-level map asset for Expo choropleth rendering."
    )
    parser.add_argument(
        "--source-geojson",
        type=Path,
        default=None,
        help="Source GeoJSON path (defaults to data/processed/geography/adm1_provinces_tr.geojson).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output asset path (defaults to frontend/mobile_shell_starter/src/assets/tr_adm1_map_lite.json).",
    )
    parser.add_argument("--epsilon-degrees", type=float, default=0.01, help="RDP simplification threshold in degrees.")
    parser.add_argument("--width", type=int, default=860, help="Map viewBox width.")
    parser.add_argument("--height", type=int, default=500, help="Map viewBox height.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check whether output asset is up to date without writing changes.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = discover_project_root(Path(__file__))

    source_geojson = args.source_geojson or (
        project_root / "data" / "processed" / "geography" / "adm1_provinces_tr.geojson"
    )
    output = args.output or (
        project_root / "frontend" / "mobile_shell_starter" / "src" / "assets" / "tr_adm1_map_lite.json"
    )

    result = build_asset_payload(
        source_geojson=source_geojson,
        source_reference=source_reference_for_metadata(project_root, source_geojson),
        epsilon_degrees=args.epsilon_degrees,
        width=args.width,
        height=args.height,
    )
    if args.check:
        if not output.exists():
            print(f"Map asset missing: {output}")
            raise SystemExit(1)
        current = json.loads(output.read_text(encoding="utf-8"))
        if not isinstance(current, dict) or not payloads_equivalent(result, current):
            print("Map asset is stale. Rebuild with: python3 scripts/build_mobile_province_map_asset.py")
            raise SystemExit(1)
        print("Map asset is up to date.")
        raise SystemExit(0)

    write_asset(output, result)
    print(f"Map asset written: {output}")
    print(
        "feature_count="
        f"{result['metadata']['feature_count']} "
        f"vertex_count={result['metadata']['vertex_count']} "
        f"shape_iso_duplicate_count={result['metadata']['shape_iso_duplicate_count']}"
    )


if __name__ == "__main__":
    main()
