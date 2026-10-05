"""xxHash3 deterministic hashing and fingerprinting."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import xxhash

FINGERPRINT_EXCLUDE_KEYS: frozenset[str] = frozenset(
    {
        "force",
        "force_rebuild",
        "dry_run",
    }
)


def compute_buffer_hash(data: bytes) -> str:
    """Compute 64-bit xxHash3 hex digest for bytes data.

    Args:
        data: Raw byte payload.

    Returns:
        Hexadecimal hash string.
    """
    return xxhash.xxh3_64(data).hexdigest()


def compute_file_hash(file_path: Path) -> str:
    """Compute 64-bit xxHash3 hex digest for a file on disk.

    Args:
        file_path: Path to the target file.

    Returns:
        Hexadecimal hash string.
    """
    hasher = xxhash.xxh3_64()
    with file_path.open("rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def compute_step_fingerprint(
    step_id: str,
    step_inputs: dict[str, Any],
    exclude_keys: frozenset[str] | None = None,
) -> str:
    """Compute deterministic xxHash3 fingerprint for a step invocation.

    Args:
        step_id: Step identifier.
        step_inputs: Resolved inputs mapping passed to the step.
        exclude_keys: Keys to ignore during fingerprint calculation.

    Returns:
        Hexadecimal hash string.
    """
    ignored = exclude_keys if exclude_keys is not None else FINGERPRINT_EXCLUDE_KEYS
    stable_inputs = {k: v for k, v in step_inputs.items() if k not in ignored}
    payload = json.dumps({"id": step_id, "inputs": stable_inputs}, sort_keys=True, default=str)
    return compute_buffer_hash(payload.encode("utf-8"))
