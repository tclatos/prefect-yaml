"""Input contract specification, validation, and dynamic model generation."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ValidationError

from prefect_yaml.dynamic_models.builder import (
    build_model_from_spec,
)


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


def _format_pydantic_error(exc: ValidationError) -> str:
    """Format Pydantic ValidationError into readable contract error messages."""
    messages: list[str] = []
    for err in exc.errors():
        loc = ".".join(str(x) for x in err.get("loc", []))
        msg = err.get("msg", "")
        err_type = err.get("type", "")

        if err_type == "missing":
            messages.append(f"Missing required parameter '{loc}'.")
        elif "literal" in err_type:
            messages.append(f"Parameter '{loc}' must be one of allowed choices: {msg}.")
        elif "greater_than" in err_type:
            messages.append(f"Parameter '{loc}' value is below minimum: {msg}.")
        elif "less_than" in err_type:
            messages.append(f"Parameter '{loc}' value exceeds maximum: {msg}.")
        elif "pattern" in err_type:
            messages.append(f"Parameter '{loc}' value does not match pattern: {msg}.")
        else:
            messages.append(f"Parameter '{loc}': {msg}.")

    return " ".join(messages)


def validate_input_value(param_name: str, value: Any, spec: InputSpec | dict[str, Any]) -> Any:
    """Validate and coerce a single input value against its specification.

    Args:
        param_name: Name of the input parameter.
        value: Provided input value.
        spec: Specification defining constraints.

    Returns:
        The validated and type-coerced value.
    """
    spec_dict = (
        spec.model_dump()
        if isinstance(spec, BaseModel)
        else dict(spec)
        if isinstance(spec, dict)
        else {"type": str(spec)}
    )

    if value is None:
        if spec_dict.get("required"):
            raise ContractValidationError(f"Missing required parameter '{param_name}'.")
        return spec_dict.get("default")

    model = build_model_from_spec("SingleInputModel", {param_name: spec_dict})
    try:
        inst = model.model_validate({param_name: value})
        return getattr(inst, param_name)
    except ValidationError as exc:
        raise ContractValidationError(_format_pydantic_error(exc)) from exc


def validate_workflow_inputs(
    inputs_specs: dict[str, Any] | str | type[BaseModel],
    raw_values: dict[str, Any],
    *,
    workflow_name: str = "Workflow",
) -> dict[str, Any]:
    """Validate and coerce all input values for a workflow using dynamic Pydantic models.

    Args:
        inputs_specs: Map of input names to their specifications, or a Pydantic model / dotted path.
        raw_values: Merged raw values provided by defaults, presets, and CLI.
        workflow_name: Name of the workflow for the generated model class.

    Returns:
        Validated dictionary containing coerced values.
    """
    if not inputs_specs:
        return dict(raw_values)

    normalized_specs: dict[str, Any] = {}
    if isinstance(inputs_specs, dict):
        for name, spec in inputs_specs.items():
            if isinstance(spec, BaseModel):
                normalized_specs[name] = spec.model_dump()
            elif isinstance(spec, dict):
                normalized_specs[name] = spec
            else:
                normalized_specs[name] = {"type": str(spec)}
        target_spec: Any = normalized_specs
    else:
        target_spec = inputs_specs

    model = build_model_from_spec(f"{workflow_name}Inputs", target_spec)

    try:
        validated = model.model_validate(raw_values)
        dumped = validated.model_dump()
        result = dict(raw_values)
        result.update(dumped)
        return result
    except ValidationError as exc:
        raise ContractValidationError(_format_pydantic_error(exc)) from exc
