from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Mapping


SOURCE_SHA256_METADATA_KEY = "bahtflow-source-sha256"


class LandingConflictError(RuntimeError):
    pass


class UploadAction(str, Enum):
    UPLOAD = "upload"
    SKIP = "skip"


@dataclass(frozen=True)
class LandingSource:
    local_path: Path
    object_name: str


_TRANSACTION_PATTERN = re.compile(
    r"^business_date=\d{4}-\d{2}-\d{2}/sales_(bkk|central|north|northeast|south)_\d{8}\.csv\.gz$"
)
_FX_PATTERN = re.compile(r"^fx_daily/\d{4}/\d{2}/fx_\d{8}\.csv$")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def transaction_object_name(relative_path: str) -> str:
    normalized = PurePosixPath(relative_path).as_posix()
    if not _TRANSACTION_PATTERN.fullmatch(normalized):
        raise ValueError(f"Unsupported transaction source path: {relative_path}")
    return f"transactions/{normalized}"


def fx_object_name(relative_path: str) -> str:
    normalized = PurePosixPath(relative_path).as_posix()
    if not _FX_PATTERN.fullmatch(normalized):
        raise ValueError(f"Unsupported FX source path: {relative_path}")
    return f"fx/{normalized.removeprefix('fx_daily/')}"


def manifest_object_name(local_path: Path) -> str:
    normalized = local_path.as_posix()
    mapping = {
        "data/daily_source_manifest.csv": "manifests/daily_source_manifest.csv",
        "fx/manifest.csv": "manifests/fx_manifest.csv",
    }
    try:
        return mapping[normalized]
    except KeyError as exc:
        raise ValueError(f"Unsupported manifest path: {normalized}") from exc


def decide_upload(
    local_sha256: str,
    *,
    remote_exists: bool,
    remote_metadata: Mapping[str, str] | None,
) -> UploadAction:
    if not remote_exists:
        return UploadAction.UPLOAD

    metadata = remote_metadata or {}
    remote_sha256 = metadata.get(SOURCE_SHA256_METADATA_KEY)
    if not remote_sha256:
        raise LandingConflictError("Existing object is missing source checksum metadata")
    if remote_sha256 != local_sha256:
        raise LandingConflictError("Existing object checksum mismatch")
    return UploadAction.SKIP


def iter_landing_sources(repo_root: Path) -> list[LandingSource]:
    sources: list[LandingSource] = []
    transaction_root = repo_root / "data" / "daily_regional_sales"
    fx_daily_root = repo_root / "fx" / "fx_daily"

    if not transaction_root.is_dir():
        raise FileNotFoundError(transaction_root)
    if not fx_daily_root.is_dir():
        raise FileNotFoundError(fx_daily_root)

    for path in transaction_root.glob("business_date=*/*.csv.gz"):
        relative = path.relative_to(transaction_root).as_posix()
        sources.append(LandingSource(path, transaction_object_name(relative)))

    fx_root = repo_root / "fx"
    for path in fx_daily_root.glob("*/*/fx_*.csv"):
        relative = path.relative_to(fx_root).as_posix()
        sources.append(LandingSource(path, fx_object_name(relative)))

    for path in [
        repo_root / "data" / "daily_source_manifest.csv",
        repo_root / "fx" / "manifest.csv",
    ]:
        if not path.is_file():
            raise FileNotFoundError(path)
        sources.append(LandingSource(path, manifest_object_name(path.relative_to(repo_root))))

    return sorted(sources, key=lambda item: item.object_name)
