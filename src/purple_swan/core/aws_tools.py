"""
AWS S3 helper utilities for storing, listing, and retrieving files.

Requirements:
- boto3

Environment variables honored (optional):
- AWS_PROFILE
- AWS_REGION or AWS_DEFAULT_REGION
- AWS_S3_ENDPOINT_URL (e.g., for LocalStack or S3-compatible stores)
"""

from __future__ import annotations

import os
import logging
import mimetypes
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


# ---------------------------
# Session / Client management
# ---------------------------

@lru_cache(maxsize=4)
def _get_boto3_session(
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
) -> boto3.session.Session:
    """
    Returns a cached boto3 Session. Honors AWS_PROFILE and AWS_REGION if not provided.
    """
    profile = profile_name or os.getenv("AWS_PROFILE")
    region = region_name or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if profile:
        return boto3.session.Session(profile_name=profile, region_name=region)
    return boto3.session.Session(region_name=region)


@lru_cache(maxsize=8)
def get_s3_client(
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    connect_timeout: int = 10,
    read_timeout: int = 60,
    max_attempts: int = 5,
):
    """
    Returns a cached S3 client with sensible defaults and retries.
    Honors AWS_S3_ENDPOINT_URL if not provided (useful for LocalStack).
    """
    session = _get_boto3_session(profile_name=profile_name, region_name=region_name)
    endpoint = endpoint_url or os.getenv("AWS_S3_ENDPOINT_URL")

    cfg = Config(
        retries={"max_attempts": max_attempts, "mode": "standard"},
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        s3={"addressing_style": "auto"},
    )

    return session.client("s3", endpoint_url=endpoint, config=cfg)


# ---------------------------
# Helpers
# ---------------------------

def _guess_content_type(path_or_key: str) -> str:
    ctype, _ = mimetypes.guess_type(path_or_key)
    return ctype or "application/octet-stream"


def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """
    Parse an s3://bucket/key URI.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("S3 URI must start with 's3://'")
    without_scheme = s3_uri[5:]
    parts = without_scheme.split("/", 1)
    if len(parts) != 2 or not parts[0]:
        raise ValueError("S3 URI must be of the form s3://bucket/key")
    bucket, key = parts[0], parts[1]
    return bucket, key


# ---------------------------
# Core operations
# ---------------------------

def upload_file(
    local_path: str,
    bucket: str,
    key: str,
    *,
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    extra_args: Optional[Dict] = None,
) -> None:
    """
    Upload a local file to S3 at bucket/key.

    extra_args may include common S3 args, e.g.:
    - ACL: "private" | "public-read" | ...
    - ContentType: "image/png", ...
    - ServerSideEncryption: "aws:kms" | "AES256", SSEKMSKeyId, etc.
    """
    s3 = get_s3_client(profile_name, region_name, endpoint_url)
    args = dict(extra_args or {})
    args.setdefault("ContentType", _guess_content_type(key))

    logger.debug("Uploading file %s to s3://%s/%s", local_path, bucket, key)
    s3.upload_file(local_path, bucket, key, ExtraArgs=args)


def upload_bytes(
    data: bytes,
    bucket: str,
    key: str,
    *,
    content_type: Optional[str] = None,
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    extra_args: Optional[Dict] = None,
) -> None:
    """
    Upload raw bytes to S3.
    """
    s3 = get_s3_client(profile_name, region_name, endpoint_url)
    args = dict(extra_args or {})
    args.setdefault("ContentType", content_type or _guess_content_type(key))

    logger.debug("Putting object bytes to s3://%s/%s", bucket, key)
    s3.put_object(Bucket=bucket, Key=key, Body=data, **args)


def download_file(
    bucket: str,
    key: str,
    local_path: str,
    *,
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> None:
    """
    Download an object from S3 to a local file.
    """
    s3 = get_s3_client(profile_name, region_name, endpoint_url)
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    logger.debug("Downloading s3://%s/%s to %s", bucket, key, local_path)
    s3.download_file(bucket, key, local_path)


def get_object_bytes(
    bucket: str,
    key: str,
    *,
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> bytes:
    """
    Retrieve object content as bytes.
    """
    s3 = get_s3_client(profile_name, region_name, endpoint_url)
    try:
        logger.debug("Getting object bytes from s3://%s/%s", bucket, key)
        resp = s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except ClientError as e:
        if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404 or e.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            raise FileNotFoundError(f"s3://{bucket}/{key} not found") from e
        raise


def list_objects(
    bucket: str,
    prefix: str = "",
    *,
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    max_keys: Optional[int] = None,
) -> Iterator[Dict]:
    """
    List objects under a prefix. Yields dictionaries with Key, Size, LastModified, ETag, StorageClass.

    max_keys limits total yielded items (client-side).
    """
    s3 = get_s3_client(profile_name, region_name, endpoint_url)
    paginator = s3.get_paginator("list_objects_v2")

    yielded = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
        for obj in page.get("Contents", []):
            yield obj
            yielded += 1
            if max_keys is not None and yielded >= max_keys:
                return


def list_keys(
    bucket: str,
    prefix: str = "",
    **kwargs,
) -> Iterator[str]:
    """
    Convenience wrapper over list_objects that yields only keys.
    """
    for obj in list_objects(bucket, prefix, **kwargs):
        yield obj["Key"]


def object_exists(
    bucket: str,
    key: str,
    *,
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> bool:
    """
    Return True if object exists, False otherwise.
    """
    s3 = get_s3_client(profile_name, region_name, endpoint_url)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in {"404", "NotFound", "NoSuchKey"}:
            return False
        raise


def delete_object(
    bucket: str,
    key: str,
    *,
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> None:
    """
    Delete a single object.
    """
    s3 = get_s3_client(profile_name, region_name, endpoint_url)
    logger.debug("Deleting s3://%s/%s", bucket, key)
    s3.delete_object(Bucket=bucket, Key=key)


def delete_objects(
    bucket: str,
    keys: Iterable[str],
    *,
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> Dict:
    """
    Batch delete up to 1000 keys per request. Returns the service response.
    """
    s3 = get_s3_client(profile_name, region_name, endpoint_url)
    key_list = list(keys)
    resp_summary: Dict = {"Deleted": [], "Errors": []}

    for i in range(0, len(key_list), 1000):
        chunk = key_list[i : i + 1000]
        if not chunk:
            continue
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
        )
        resp_summary["Deleted"].extend(resp.get("Deleted", []))
        resp_summary["Errors"].extend(resp.get("Errors", []))
    return resp_summary


def generate_presigned_url(
    bucket: str,
    key: str,
    *,
    expires_in: int = 3600,
    method: str = "get_object",
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> str:
    """
    Generate a presigned URL for GET/PUT operations.
    method: "get_object" or "put_object"
    """
    s3 = get_s3_client(profile_name, region_name, endpoint_url)
    return s3.generate_presigned_url(
        ClientMethod=method,
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )


# ---------------------------
# S3 URI convenience wrappers
# ---------------------------

def upload_file_to_uri(
    local_path: str,
    s3_uri: str,
    **kwargs,
) -> None:
    bucket, key = parse_s3_uri(s3_uri)
    upload_file(local_path, bucket, key, **kwargs)


def upload_bytes_to_uri(
    data: bytes,
    s3_uri: str,
    **kwargs,
) -> None:
    bucket, key = parse_s3_uri(s3_uri)
    upload_bytes(data, bucket, key, **kwargs)


def download_uri_to_file(
    s3_uri: str,
    local_path: str,
    **kwargs,
) -> None:
    bucket, key = parse_s3_uri(s3_uri)
    download_file(bucket, key, local_path, **kwargs)


def get_uri_bytes(
    s3_uri: str,
    **kwargs,
) -> bytes:
    bucket, key = parse_s3_uri(s3_uri)
    return get_object_bytes(bucket, key, **kwargs)


def object_exists_uri(
    s3_uri: str,
    **kwargs,
) -> bool:
    bucket, key = parse_s3_uri(s3_uri)
    return object_exists(bucket, key, **kwargs)


def delete_uri(
    s3_uri: str,
    **kwargs,
) -> None:
    bucket, key = parse_s3_uri(s3_uri)
    delete_object(bucket, key, **kwargs)