"""Boto3-backed real-S3 I/O for Landing partition payloads (parquet/manifest/`_SUCCESS`) and
the promotion gate's transport-integrity checks — mirrors the dual-mode (`s3://` vs local-disk)
pattern `pipeline.common.watermark` already proved out for the tiny watermark JSON files,
extended here to partition-shaped reads/writes (2026-07-16, real-data ingest session, staff-DE
ruling: the local-disk staging shim previously used unconditionally by
`pipeline/extract/salesforce_extract.py` and `pipeline/promote/promotion_gate.py`'s batch path
was an unbuilt corner, not a deliberate design choice — real AWS creds should mean real S3,
per `pipeline/common/lake_paths.py`'s own docstring).

`pipeline/extract/cdc_common.py`, `cdc_initial_snapshot.py`, and `obp_client.py` are NOT
migrated to this module yet (named follow-up — Teradata/OBP are out of this session's scope).
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

try:
    import boto3
except ImportError:  # pragma: no cover - local-disk fallback doesn't need boto3
    boto3 = None


def is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _split(path: str) -> tuple[str, str]:
    parsed = urlparse(path)
    return parsed.netloc, parsed.path.lstrip("/")


def exists(path: str) -> bool:
    if is_s3(path):
        bucket, key = _split(path)
        client = boto3.client("s3")
        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except client.exceptions.ClientError:
            return False
    return Path(path).exists()


def read_bytes(path: str) -> bytes | None:
    if is_s3(path):
        bucket, key = _split(path)
        client = boto3.client("s3")
        try:
            return client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except client.exceptions.ClientError:
            return None
    p = Path(path)
    return p.read_bytes() if p.exists() else None


def read_text(path: str) -> str | None:
    data = read_bytes(path)
    return data.decode("utf-8") if data is not None else None


def write_bytes(path: str, data: bytes) -> None:
    if is_s3(path):
        bucket, key = _split(path)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data)
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def write_text(path: str, text: str) -> None:
    write_bytes(path, text.encode("utf-8"))


def prefix_has_objects(path: str) -> bool:
    """Directory-existence check (used for 'has this Bronze table ever been written before')."""
    if is_s3(path):
        bucket, key_prefix = _split(path)
        prefix = key_prefix.rstrip("/") + "/"
        resp = boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0
    return Path(path).exists()


def _delete_prefix(dest_path: str) -> None:
    """Deletes every object under an S3 prefix — used by `upload_dir` to mirror Spark's
    `mode("overwrite")` semantics. Without this, a re-run's new part-file (a fresh UUID
    filename each write) would land ALONGSIDE, not replace, a prior run's part-file: the local
    `mode("overwrite")` write correctly replaces the LOCAL staging dir, but `upload_dir` only
    ever pushes what's currently local — it never removes what a previous run left in S3. Real
    bug, caught live (2026-07-17): a re-run of the PaySim extractor left a stale 20K-row test
    part-file sitting next to the new 6.36M-row one in the same `dt=` partition."""
    bucket, key_prefix = _split(dest_path)
    prefix = key_prefix.rstrip("/") + "/"
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})


def upload_dir(local_dir: str, dest_path: str) -> None:
    """Uploads every file under `local_dir` (recursively) to `dest_path` in S3, preserving
    relative layout — the JDBC batch extractors' S3 write path (2026-07-17, staff-DE ruling):
    Spark writes its native parquet/manifest output to a LOCAL temp dir first (no S3A needed,
    Spark spills to disk instead of collecting to the driver), then this function pushes the
    bytes up via boto3, same mechanism `write_bytes`/`write_text` already use. No-op (local-disk
    fallback) when `dest_path` isn't `s3://` — the caller's local temp dir IS the final location.
    Clears any pre-existing objects at `dest_path` first (see `_delete_prefix`) so a re-run
    truly overwrites the partition instead of accumulating stale files alongside it.
    """
    if not is_s3(dest_path):
        return
    _delete_prefix(dest_path)
    local_root = Path(local_dir)
    for f in local_root.rglob("*"):
        # Skip Hadoop LocalFileSystem's `.crc` sidecar files (e.g. `.part-....parquet.crc`) —
        # an artifact of writing through the local filesystem at all; a native S3A write
        # (Databricks) never produces these, so uploading them would be staging-path clutter,
        # not real payload.
        if f.is_file() and not f.name.endswith(".crc"):
            rel = f.relative_to(local_root).as_posix()
            write_bytes(f"{dest_path}/{rel}", f.read_bytes())


def list_dt_partitions(landing_path: str) -> list[str]:
    """`dt=...` partition directory names directly under `landing_path` — S3 "directories"
    are just common key prefixes, so this lists them via `Delimiter="/"`."""
    if is_s3(landing_path):
        bucket, key_prefix = _split(landing_path)
        prefix = key_prefix.rstrip("/") + "/"
        client = boto3.client("s3")
        paginator = client.get_paginator("list_objects_v2")
        names: set[str] = set()
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                sub = cp["Prefix"][len(prefix):].rstrip("/")
                if sub.startswith("dt="):
                    names.add(sub)
        return sorted(names)
    local_dir = Path(landing_path)
    if not local_dir.exists():
        return []
    return sorted(p.name for p in local_dir.glob("dt=*"))
