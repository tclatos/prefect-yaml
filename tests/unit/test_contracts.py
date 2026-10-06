"""Unit tests for input contracts and type validation."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import BaseModel

from prefect_yaml.contracts.validator import (
    ContractValidationError,
    InputSpec,
    validate_input_value,
    validate_workflow_inputs,
)


def test_coerce_primitive_types() -> None:
    """Test string coercion to int, float, and bool."""
    spec_int = InputSpec(type="int")
    assert validate_input_value("count", "42", spec_int) == 42

    spec_float = InputSpec(type="float")
    assert validate_input_value("ratio", "3.14", spec_float) == 3.14

    spec_bool = InputSpec(type="bool")
    assert validate_input_value("flag_true", "true", spec_bool) is True
    assert validate_input_value("flag_false", "0", spec_bool) is False


def test_coerce_path() -> None:
    """Test coercion to pathlib.Path."""
    spec = InputSpec(type="path")
    res = validate_input_value("dir", "/tmp/docs", spec)
    assert isinstance(res, Path)
    assert str(res) == "/tmp/docs"


def test_coerce_list_with_items_type() -> None:
    """Test list parsing and inner item type coercion."""
    spec = InputSpec(type="list", items_type="int")
    assert validate_input_value("numbers", "1, 2, 3", spec) == [1, 2, 3]
    assert validate_input_value("numbers_json", "[10, 20]", spec) == [10, 20]


def test_validation_choices() -> None:
    """Test constraint validation for choices."""
    spec = InputSpec(type="str", choices=["fast", "slow"])
    assert validate_input_value("speed", "fast", spec) == "fast"
    with pytest.raises(ContractValidationError, match="must be one of"):
        validate_input_value("speed", "turbo", spec)


def test_validation_bounds() -> None:
    """Test minimum and maximum numeric bounds."""
    spec = InputSpec(type="int", minimum=1, maximum=10)
    assert validate_input_value("val", 5, spec) == 5
    with pytest.raises(ContractValidationError, match="below minimum"):
        validate_input_value("val", 0, spec)
    with pytest.raises(ContractValidationError, match="exceeds maximum"):
        validate_input_value("val", 11, spec)


def test_validation_regex() -> None:
    """Test regex pattern matching."""
    spec = InputSpec(type="str", regex=r"^v\d+\.\d+$")
    assert validate_input_value("ver", "v1.2", spec) == "v1.2"
    with pytest.raises(ContractValidationError, match="does not match pattern"):
        validate_input_value("ver", "1.2", spec)


def test_required_validation() -> None:
    """Test missing required parameter raises error."""
    spec = InputSpec(type="str", required=True)
    with pytest.raises(ContractValidationError, match="Missing required parameter"):
        validate_input_value("target", None, spec)


def test_validate_workflow_inputs_batch() -> None:
    """Test batch validation of input dictionaries."""
    specs = {
        "batch_size": {"type": "int", "default": 10},
        "name": {"type": "str", "required": True},
    }
    raw = {"batch_size": "25", "name": "sample"}
    validated = validate_workflow_inputs(specs, raw)
    assert validated["batch_size"] == 25
    assert validated["name"] == "sample"


class PipelineConfig(BaseModel):
    """Sample Python Pydantic model for hybrid workflows."""

    environment: str = "prod"
    retries: int = 3


def test_validate_workflow_inputs_with_python_model() -> None:
    """Test validating workflow inputs using an existing Python Pydantic model."""
    raw = {"environment": "staging", "retries": "5"}
    validated = validate_workflow_inputs(PipelineConfig, raw, workflow_name="deploy")
    assert validated["environment"] == "staging"
    assert validated["retries"] == 5

    # Also test via dotted path string
    validated_dotted = validate_workflow_inputs(
        "tests.unit.test_contracts.PipelineConfig",
        {"environment": "dev"},
        workflow_name="deploy",
    )
    assert validated_dotted["environment"] == "dev"
    assert validated_dotted["retries"] == 3


def test_workflow_def_get_input_model() -> None:
    """Test get_input_model on WorkflowDef."""
    from prefect_yaml.models.authoring import WorkflowDef

    # From dict spec
    wf = WorkflowDef(
        name="test_etl",
        inputs={
            "workers": {"type": "int", "default": 4},
            "source": {"type": "path", "required": True},
        },
        run="json.dumps",
    )
    model = wf.get_input_model()
    assert "workers" in model.model_fields
    assert "source" in model.model_fields

    # From Python model directly
    wf_hybrid = WorkflowDef(
        name="test_hybrid",
        inputs=PipelineConfig,
        run="json.dumps",
    )
    model_hybrid = wf_hybrid.get_input_model()
    assert model_hybrid is PipelineConfig
