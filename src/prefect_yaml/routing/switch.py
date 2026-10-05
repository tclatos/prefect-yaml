"""Switch router: conditional branch execution based on dynamic evaluation."""

from __future__ import annotations

from typing import Any

from prefect_yaml.models.compiled import SwitchSpec


def evaluate_switch(switch_spec: SwitchSpec, context_value: Any) -> str | None:
    """Evaluate a switch condition and return the selected branch target.

    Args:
        switch_spec: Switch specification containing cases and default.
        context_value: The evaluated value to match against cases.

    Returns:
        The target branch name or callable, or None if unmatched and no default.
    """
    key = str(context_value)
    if key in switch_spec.cases:
        return switch_spec.cases[key]
    return switch_spec.default
