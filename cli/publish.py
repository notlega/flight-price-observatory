"""Publish subcommand: upload silver Parquet to Cloudflare R2."""

import argparse
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_SILVER_DIR = "storage/silver"

_ENV_KEYS = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")


def configure_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],  # type: ignore[reportPrivateUsage]
) -> None:
    """Register the ``publish`` subcommand on ``subparsers``."""
    p = subparsers.add_parser("publish", help="Upload silver Parquet to R2")
    p.add_argument(
        "--input",
        type=str,
        default=DEFAULT_SILVER_DIR,
        help=f"Input Parquet directory (default: {DEFAULT_SILVER_DIR})",
    )
    p.set_defaults(func=run)


def _r2_client() -> Any:
    """Build an S3 client for R2, requiring the R2 env vars."""
    import boto3

    missing = [k for k in _ENV_KEYS if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"error: missing env vars: {', '.join(missing)}")

    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )


def _upload_files(
    client: Any, input_dir: str, bucket: str, prefix: str = "silver"
) -> int:
    """Upload every Parquet file under ``input_dir``; return the count."""
    count = 0
    for path in sorted(Path(input_dir).rglob("*.parquet")):
        key = f"{prefix}/{path.relative_to(input_dir).as_posix()}"
        client.upload_file(str(path), bucket, key)
        count += 1
    return count


def run(args: argparse.Namespace) -> None:
    """Upload silver Parquet to R2 and report the uploaded count."""
    client = _r2_client()
    bucket = os.environ["R2_BUCKET"]
    count = _upload_files(client, args.input, bucket)
    logger.info("Uploaded %d Parquet files", count)
    print(f"Uploaded {count} Parquet files to s3://{bucket}/silver/")
