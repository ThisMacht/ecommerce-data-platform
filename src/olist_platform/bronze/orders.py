"""Compatibility API for bounded orders extraction."""

from .extraction import (
    CURSOR,
    LOCK,
    PIPELINE,
    configuration,
    cursor_tuple,
    cursor_value,
    prepare,
)
from .extraction import ingest_source as ingest_orders

__all__ = [
    "CURSOR",
    "LOCK",
    "PIPELINE",
    "configuration",
    "cursor_tuple",
    "cursor_value",
    "prepare",
    "ingest_orders",
]
