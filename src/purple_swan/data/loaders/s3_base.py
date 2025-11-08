from __future__ import annotations

import os
from typing import Generic, TypeVar

import boto3

from purple_swan.data.loaders.data_loader import DataLoader, T_co


class S3DataLoaderBase(DataLoader[T_co], Generic[T_co]):
    """
    Base class for S3-based loaders.

    - Handles {env} substitution in bucket names via PSWN_ENV (defaults to 'dev')
    - Provides a boto3 S3 client helper.
    """

    def __init__(self, region: str | None = None):
        self.region = region or "us-east-1"

    @staticmethod
    def resolve_env_placeholder(value: str) -> str:
        """
        Replace '{env}' in a string with the current environment name.

        PSWN_ENV=dev|staging|prod (default 'dev'):
            'pawn-{env}' -> 'pawn-dev'
        """
        env_name = os.getenv("PSWN_ENV", "dev")
        return value.replace("{env}", env_name)

    def _resolve_bucket(self, bucket: str) -> str:
        return self.resolve_env_placeholder(bucket)

    def _get_s3_client(self):
        return boto3.client("s3", region_name=self.region)
