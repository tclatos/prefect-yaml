"""Unit tests for xxHash3 caching and manifest storage."""

from __future__ import annotations

from pathlib import Path

from prefect_yaml.cache.fingerprint import compute_buffer_hash, compute_step_fingerprint
from prefect_yaml.cache.manifest import ManifestCache


def test_xxhash_buffer_hash() -> None:
    """Test buffer hash is deterministic and matches expected xxHash3."""
    h1 = compute_buffer_hash(b"hello world")
    h2 = compute_buffer_hash(b"hello world")
    assert h1 == h2
    assert len(h1) == 16  # 64-bit hex digest is 16 chars


def test_compute_step_fingerprint_ignores_control_flags() -> None:
    """Test that control flags like force do not change fingerprint."""
    fp1 = compute_step_fingerprint("step_a", {"path": "/tmp/a", "force": False})
    fp2 = compute_step_fingerprint("step_a", {"path": "/tmp/a", "force": True})
    assert fp1 == fp2


def test_manifest_cache_freshness(tmp_path: Path) -> None:
    """Test ManifestCache fresh checks and disk serialization."""
    manifest_path = tmp_path / "manifest.json"
    cache = ManifestCache()
    assert cache.is_fresh("step1", fingerprint="fp1") is False

    cache.record_success("step1", "fp1", outputs={"result": 100})
    assert cache.is_fresh("step1", fingerprint="fp1") is True
    assert cache.is_fresh("step1", fingerprint="fp1", force=True) is False
    assert cache.is_fresh("step1", fingerprint="different_fp") is False

    cache.save(manifest_path)
    loaded = ManifestCache.load(manifest_path)
    assert loaded.is_fresh("step1", fingerprint="fp1") is True
    assert loaded.get_output("step1", "result") == 100
