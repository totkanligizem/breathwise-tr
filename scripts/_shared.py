from __future__ import annotations

import os
import re
import unicodedata
from pathlib import Path


TURKISH_GEOCODING_ALIASES = {
    "İstanbul": "Istanbul",
    "İzmir": "Izmir",
    "Iğdır": "Igdir",
    "Şanlıurfa": "Sanliurfa",
    "Şırnak": "Sirnak",
    "Çanakkale": "Canakkale",
    "Çankırı": "Cankiri",
    "Çorum": "Corum",
    "Gümüşhane": "Gumushane",
    "Kırklareli": "Kirklareli",
    "Kırıkkale": "Kirikkale",
    "Kırşehir": "Kirsehir",
    "Kahramanmaraş": "Kahramanmaras",
    "Muğla": "Mugla",
    "Nevşehir": "Nevsehir",
    "Niğde": "Nigde",
    "Uşak": "Usak",
    "Ağrı": "Agri",
    "Adıyaman": "Adiyaman",
    "Eskişehir": "Eskisehir",
    "Tekirdağ": "Tekirdag",
    "Düzce": "Duzce",
    "Bartın": "Bartin",
    "Karabük": "Karabuk",
    "Hakkâri": "Hakkari",
}


_TURKISH_ASCII_TRANSLATION = str.maketrans(
    {
        "ı": "i",
        "İ": "I",
        "ş": "s",
        "Ş": "S",
        "ğ": "g",
        "Ğ": "G",
        "ü": "u",
        "Ü": "U",
        "ö": "o",
        "Ö": "O",
        "ç": "c",
        "Ç": "C",
        "â": "a",
        "Â": "A",
        "î": "i",
        "Î": "I",
        "û": "u",
        "Û": "U",
    }
)


def _is_project_root(path: Path) -> bool:
    return (path / "data").is_dir() and (path / "scripts").is_dir()


def discover_project_root(start: Path | None = None) -> Path:
    env_root = os.getenv("BREATHWISE_PROJECT_ROOT")
    if env_root:
        root = Path(env_root).expanduser().resolve()
        if not root.exists():
            raise RuntimeError(
                "BREATHWISE_PROJECT_ROOT is set but does not exist: "
                f"{root}"
            )
        if not _is_project_root(root):
            raise RuntimeError(
                "BREATHWISE_PROJECT_ROOT does not look like the Breathwise "
                f"project root: {root}"
            )
        return root

    anchors: list[Path] = []
    if start is not None:
        anchors.append(start.resolve())
    else:
        anchors.append(Path(__file__).resolve())
    anchors.append(Path.cwd().resolve())

    for anchor in anchors:
        probe = anchor if anchor.is_dir() else anchor.parent
        for candidate in [probe, *probe.parents]:
            if _is_project_root(candidate):
                return candidate

    raise RuntimeError(
        "Unable to discover project root. Set BREATHWISE_PROJECT_ROOT or run "
        "inside the project directory."
    )


def normalize_turkish_city_name(name: str) -> str:
    return TURKISH_GEOCODING_ALIASES.get(name, name)


def slugify_ascii(value: str) -> str:
    mapped = value.translate(_TURKISH_ASCII_TRANSLATION)
    normalized = unicodedata.normalize("NFKD", mapped)
    stripped = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    ascii_only = stripped.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^A-Za-z0-9]+", "_", ascii_only).strip("_").lower()
    slug = re.sub(r"_+", "_", slug)
    return slug or "item"
