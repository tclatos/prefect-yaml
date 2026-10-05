"""Input contract specification, validation, and type coercion."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from prefect_yaml.contracts.types import ContractType


class ContractValidationError(ValueError):
    """Exception raised when an input contract validation fails."""


class InputSpec(BaseModel):
    """Specification of a workflow input parameter."""

    type: str = "any"
    required: bool = False
    default: Any = None
    description: str = ""
    choices: list[Any] | None = None
    minimum: float | int | None = None
    maximum: float | int | None = None
    regex: str | None = None
    items_type: str | None = None

    model_config = {"extra": "allow"}


def _coerce_value(value: Any, expected_type: str, items_type: str | None = None) -> Any:
    norm_type = expected_type.lower()
    if norm_type in (ContractType.ANY.value,):
        return value

    if norm_type in (ContractType.STRING.value, ContractType.STR.value):
        if value is None:
            return None
        return str(value)

    if norm_type in (ContractType.INTEGER.value, ContractType.INT.value):
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str):
            val_strip = value.strip()
            return int(val_strip)
        return int(value)

    if norm_type in (ContractType.FLOAT.value, ContractType.NUMBER.value):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if isinstance(value, str):
            val_strip = value.strip()
            return float(val_strip)
        return float(value)

    if norm_type in (ContractType.BOOLEAN.value, ContractType.BOOL.value):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            val_lower = value.strip().lower()
            if val_lower in ("true", "1", "yes", "on"):
                return True
            if val_lower in ("false", "0", "no", "off"):
                return False
            raise ValueError(f"Cannot parse boolean from '{value}'")
        return bool(value)

    if norm_type == ContractType.PATH.value:
        if value is None:
            return None
        return Path(str(value))

    if norm_type == ContractType.LIST.value:
        if isinstance(value, (list, tuple)):
            items = list(value)
        elif isinstance(value, str):
            val_strip = value.strip()
            if val_strip.startswith("[") and val_strip.endswith("]"):
                try:
                    items = json.loads(val_strip)
                except Exception:
                    items = [x.strip() for x in val_strip[1:-1].split(",") if x.strip()]
            else:
                items = [x.strip() for x in val_strip.split(",") if x.strip()]
        else:
            items = list(value)

        if items_type:
            return [_coerce_value(item, items_type) for item in items]
        return items

    if norm_type == ContractType.DICT.value:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            return json.loads(value)
        return dict(value)

    return value


def validate_input_value(param_name: str, value: Any, spec: InputSpec) -> Any:
    """Validate and coerce a single input value against its specification.

    Args:
        param_name: Name of the input parameter.
        value: Provided input value.
        spec: Specification defining constraints.

    Returns:
        The validated and type-coerced value.
    """
    if value is None:
        if spec.required:
            raise ContractValidationError(f"Missing required parameter '{param_name}'.")
        return spec.default

    try:
        coerced = _coerce_value(value, spec.type, items_type=spec.items_type)
    except Exception as exc:
        raise ContractValidationError(
            f"Parameter '{param_name}' expected type '{spec.type}', but value '{value}' could not be coerced: {exc}"
        ) from exc

    if spec.choices is not None and coerced not in spec.choices:
        raise ContractValidationError(
            f"Parameter '{param_name}' must be one of {spec.choices}, got '{coerced}'."
        )

    if spec.minimum is not None and coerced < spec.minimum:
        raise ContractValidationError(
            f"Parameter '{param_name}' value {coerced} is below minimum {spec.minimum}."
        )

    if spec.maximum is not None and coerced > spec.maximum:
        raise ContractValidationError(
            f"Parameter '{param_name}' value {coerced} exceeds maximum {spec.maximum}."
        )

    if (
        spec.regex is not None
        and isinstance(coerced, (str, Path))
        and not re.search(spec.regex, str(coerced))
    ):
        raise ContractValidationError(
            f"Parameter '{param_name}' value '{coerced}' does not match pattern '{spec.regex}'."
        )

    return coerced


def validate_workflow_inputs(
    inputs_specs: dict[str, InputSpec | dict[str, Any]],
    raw_values: dict[str, Any],
) -> dict[str, Any]:
    """Validate and coerce all input values for a workflow.

    Args:
        inputs_specs: Map of input names to their specifications.
        raw_values: Merged raw values provided by defaults, presets, and CLI.

    Returns:
        Validated dictionary containing coerced values.
    """
    normalized_specs: dict[str, InputSpec] = {}
    for name, spec in inputs_specs.items():
        if isinstance(spec, InputSpec):
            normalized_specs[name] = spec
        elif isinstance(spec, dict):
            normalized_specs[name] = InputSpec.model_validate(spec)
        else:
            normalized_specs[name] = InputSpec(description=str(spec))

    validated_values: dict[str, Any] = dict(raw_values)

    for name, spec in normalized_specs.items():
        val = raw_values.get(name, spec.default)
        validated_values[name] = validate_input_value(name, val, spec)

    return validated_values
