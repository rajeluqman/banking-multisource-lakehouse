"""Resolves Landing/Bronze/Silver/Gold paths — S3 (sole truth, ADR-002) with a local-disk
fallback (same layout) when no AWS credentials are present, per D-14's dev-cheap loop.

Every pipeline module imports its paths from here rather than constructing
`s3://...`/local strings inline — one place to change the root, not N places.
"""

from __future__ import annotations

import os

LAYERS = ("landing", "bronze", "silver", "gold")


def _has_aws_credentials() -> bool:
    return bool(os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"))


def lake_root() -> str:
    """s3://<bucket>/<prefix> if AWS creds are present, else ./data (local-disk fallback,
    same relative layout — journey/07_PIPELINE_SPEC.md)."""
    if _has_aws_credentials():
        bucket = os.environ["S3_BUCKET"]
        prefix = os.environ.get("S3_PREFIX", "banking/").rstrip("/")
        return f"s3://{bucket}/{prefix}"
    return "data"


def layer_path(layer: str, *parts: str) -> str:
    if layer not in LAYERS:
        raise ValueError(f"unknown layer '{layer}' — must be one of {LAYERS}")
    root = lake_root()
    suffix = "/".join(parts)
    return f"{root}/{layer}/{suffix}" if suffix else f"{root}/{layer}"


def control_path(*parts: str) -> str:
    """Watermark/offset state and other pipeline run metadata — lives IN the lake
    (journey/07_PIPELINE_SPEC.md: "watermark state in the lake, not code"), not in a local
    file that wouldn't survive a fresh Databricks cluster."""
    root = lake_root()
    suffix = "/".join(parts)
    return f"{root}/_control/{suffix}" if suffix else f"{root}/_control"
