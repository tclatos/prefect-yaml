"""Unit tests for routing pattern matcher, partitioner, and switch router."""

from __future__ import annotations

from prefect_yaml.models.compiled import RouterRule, SwitchSpec
from prefect_yaml.routing.matcher import expand_brace_pattern, matches_pattern
from prefect_yaml.routing.partitioner import partition_items
from prefect_yaml.routing.switch import evaluate_switch


def test_expand_brace_pattern() -> None:
    """Brace pattern alternatives are expanded correctly."""
    patterns = expand_brace_pattern("**/*.{pdf,docx}")
    assert patterns == ["**/*.pdf", "**/*.docx"]


def test_matches_pattern_pathspec_and_regex() -> None:
    """Test matching against gitwildmatch, regex, and URL prefixes."""
    assert matches_pattern("**/*.pdf", "docs/file.pdf") is True
    assert matches_pattern("**/*.pdf", "docs/file.txt") is False

    assert matches_pattern("re:^https://api\\.", "https://api.example.com") is True
    assert matches_pattern("re:^https://api\\.", "https://www.example.com") is False

    assert matches_pattern("https://youtube.com/**", "https://youtube.com/watch?v=123") is True


def test_partition_items() -> None:
    """Items are partitioned into buckets based on ordered rules."""
    rules = [
        RouterRule(match="**/*.pdf", run="pdf_flow", with_={"mode": "ocr"}),
        RouterRule(match="https://**", run="web_flow"),
    ]
    items = ["doc.pdf", "https://example.com/page", "report.pdf", "notes.txt"]

    buckets = partition_items(items, rules, default="default_flow")

    pdf_key = ("pdf_flow", '{"mode": "ocr"}')
    web_key = ("web_flow", "{}")
    default_key = ("default_flow", "{}")

    assert buckets[pdf_key]["items"] == ["doc.pdf", "report.pdf"]
    assert buckets[web_key]["items"] == ["https://example.com/page"]
    assert buckets[default_key]["items"] == ["notes.txt"]


def test_evaluate_switch() -> None:
    """Switch router returns matching case or default."""
    spec = SwitchSpec(
        value="${steps.check.result.status}",
        cases={"ready": "run_full", "degraded": "run_light"},
        default="run_fallback",
    )
    assert evaluate_switch(spec, "ready") == "run_full"
    assert evaluate_switch(spec, "degraded") == "run_light"
    assert evaluate_switch(spec, "unknown") == "run_fallback"
