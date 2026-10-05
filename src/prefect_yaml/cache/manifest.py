"""Persistent manifest cache for incremental workflow execution."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loguru import logger
from pydantic import BaseModel, Field


class CacheRecord(BaseModel):
    """Metadata record for a cached step execution."""

    key: str
    fingerprint: str
    status: str = "ok"
    code_version: str | None = None
    outputs: dict[str, Any] = Field(default_factory=dict)
    processed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ManifestCache(BaseModel):
    """File-backed execution cache storing item fingerprints and outputs."""

    records: dict[str, CacheRecord] = Field(default_factory=dict)

    def is_fresh(
        self,
        key: str,
        *,
        fingerprint: str,
        force: bool = False,
        code_version: str | None = None,
    ) -> bool:
        """Check whether a cached item record is valid and up to date.

        Args:
            key: Unique item or step identifier.
            fingerprint: Current execution fingerprint.
            force: Force re-execution regardless of cache status.
            code_version: Secondary versioning tag.

        Returns:
            True if cached item is fresh and reusable, False otherwise.
        """
        if force:
            return False
        record = self.records.get(key)
        if record is None:
            return False
        if record.fingerprint != fingerprint:
            return False
        if code_version is not None and record.code_version != code_version:
            return False
        return True

    def get_output(self, key: str, output_field: str) -> Any:
        """Retrieve a specific output value from a cached record.

        Args:
            key: Cached item identifier.
            output_field: Target output dictionary key.

        Returns:
            Output value if found, None otherwise.
        """
        record = self.records.get(key)
        return record.outputs.get(output_field) if record else None

    def record_success(
        self,
        key: str,
        fingerprint: str,
        *,
        outputs: dict[str, Any] | None = None,
        code_version: str | None = None,
    ) -> None:
        """Record successful execution of an item into the cache.

        Args:
            key: Unique item or step identifier.
            fingerprint: Execution fingerprint.
            outputs: Produced output metadata.
            code_version: Secondary code or schema version.
        """
        self.records[key] = CacheRecord(
            key=key,
            fingerprint=fingerprint,
            status="ok",
            code_version=code_version,
            outputs=outputs or {},
        )

    def record_failure(self, key: str, fingerprint: str, error: str = "") -> None:
        """Record failed execution of an item.

        Args:
            key: Unique item or step identifier.
            fingerprint: Execution fingerprint.
            error: Error message string.
        """
        self.records[key] = CacheRecord(
            key=key,
            fingerprint=fingerprint,
            status="error",
            outputs={"error": error} if error else {},
        )

    @classmethod
    def load(cls, path: Path | None, *, warn_on_error: bool = True) -> ManifestCache:
        """Load manifest records from a JSON file.

        Args:
            path: Path to manifest JSON file.
            warn_on_error: Whether to warn and return empty cache on corrupt JSON.

        Returns:
            Loaded or initialized ManifestCache instance.
        """
        if path is None or not path.exists():
            return cls()
        try:
            text = path.read_text(encoding="utf-8")
            return cls.model_validate_json(text)
        except Exception as exc:
            if warn_on_error:
                logger.warning("Failed to load manifest from {}: {}. Starting fresh.", path, exc)
                return cls()
            raise

    def save(self, path: Path) -> None:
        """Persist cache records to disk in JSON format.

        Args:
            path: Target JSON file path.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.model_dump_json(indent=2), encoding="utf-8")
        logger.debug("Saved manifest cache ({} records) to {}", len(self.records), path)


def default_manifest_path(workflow_name: str, cache_dir: Path | str | None = None) -> Path:
    """Determine the default location for a workflow execution manifest.

    Args:
        workflow_name: Name of the workflow.
        cache_dir: Optional explicit base cache directory.

    Returns:
        Path to the manifest JSON file.
    """
    if cache_dir:
        base = Path(cache_dir)
    else:
        base = Path.cwd() / ".cache" / "workflows"
    return base / workflow_name / "manifest.json"
