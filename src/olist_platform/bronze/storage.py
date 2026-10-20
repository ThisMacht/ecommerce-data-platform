"""Conditional S3 writes and verification of the Bronze publication boundary."""

from __future__ import annotations

import hashlib
import json

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError


class BronzeError(ValueError):
    """A batch, configuration or publication invariant failed."""


def encode(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class ObjectStore:
    def __init__(self, client: BaseClient, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    def initialize(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
                raise
            try:
                self.client.create_bucket(Bucket=self.bucket)
            except ClientError as conflict:
                if conflict.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                    raise

    def read(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        with response["Body"] as stream:
            return stream.read()

    def put_immutable(self, key: str, body: bytes) -> None:
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                IfNoneMatch="*",
                Metadata={"sha256": sha256(body)},
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("PreconditionFailed", "412"):
                raise
            if self.read(key) != body:
                raise BronzeError(f"Immutable object conflict: {key}") from error


def s3_client(endpoint: str) -> BaseClient:
    # Credentials come from standard AWS environment, never command arguments.
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=10,
            read_timeout=30,
            retries={"mode": "standard", "max_attempts": 3},
        ),
    )


def verify_published(store: ObjectStore, prefix: str) -> dict:
    """Consumers must call this before accepting a batch as complete."""
    marker = json.loads(store.read(f"{prefix}/_SUCCESS"))
    body = store.read(f"{prefix}/manifest.json")
    if marker != {"manifest_sha256": sha256(body)}:
        raise BronzeError("Published manifest checksum mismatch")
    manifest = json.loads(body)
    if manifest["prefix"] != prefix or manifest["format_version"] != 1:
        raise BronzeError("Published manifest location/version mismatch")
    if sha256(encode(manifest["source_schema"])) != manifest["schema_fingerprint"]:
        raise BronzeError("Published schema fingerprint mismatch")
    total = 0
    for number, part in enumerate(manifest["objects"], 1):
        if part["key"] != f"{prefix}/attempt=0001/part-{number:06d}.jsonl":
            raise BronzeError("Unexpected Bronze object path")
        content = store.read(part["key"])
        if sha256(content) != part["sha256"] or len(content) != part["size_bytes"]:
            raise BronzeError("Published object checksum/size mismatch")
        rows = content.splitlines()
        if len(rows) != part["row_count"]:
            raise BronzeError("Published object row count mismatch")
        for row in rows:
            json.loads(row)
        total += len(rows)
    if (
        total != manifest["row_count"]
        or total != manifest["source_row_count"]
        or len(manifest["objects"]) != manifest["page_count"]
    ):
        raise BronzeError("Published batch count mismatch")
    return manifest
