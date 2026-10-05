"""Pattern matchers for workflow routing."""

from __future__ import annotations

import re

import pathspec


def expand_brace_pattern(pattern: str) -> list[str]:
    """Expand simple brace alternatives like *.{jpg,png} into multiple patterns.

    Args:
        pattern: Pattern containing brace alternatives.

    Returns:
        List of expanded pattern strings.
    """
    match = re.search(r"\{([^{}]+)\}", pattern)
    if not match:
        return [pattern]

    prefix = pattern[: match.start()]
    suffix = pattern[match.end() :]
    options = match.group(1).split(",")

    expanded: list[str] = []
    for opt in options:
        sub_pattern = f"{prefix}{opt.strip()}{suffix}"
        expanded.extend(expand_brace_pattern(sub_pattern))
    return expanded


def matches_pattern(pattern: str, item: str) -> bool:
    """Evaluate whether an item string matches a given pattern.

    Args:
        pattern: Pathspec, regex (prefixed with re:), or prefix pattern.
        item: Target string to test.

    Returns:
        True if item matches the pattern, False otherwise.
    """
    if pattern.startswith("re:"):
        regex_pat = pattern[3:]
        return bool(re.search(regex_pat, item))

    if pattern.startswith(("http://", "https://")) and pattern.endswith(("/**", "/*")):
        prefix = pattern.rstrip("/*")
        return item.startswith(prefix)

    patterns = expand_brace_pattern(pattern)
    spec = pathspec.PathSpec.from_lines("gitignore", patterns)
    return bool(spec.match_file(item))
