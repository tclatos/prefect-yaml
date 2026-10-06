"""Unit tests for the isolated dynamic_models package."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import pytest
from pydantic import BaseModel, ValidationError

from prefect_yaml.dynamic_models.builder import (
    build_model_from_spec,
)
from prefect_yaml.dynamic_models.loader import load_models_from_yaml
from prefect_yaml.dynamic_models.types import resolve_type_hint


class SamplePythonModel(BaseModel):
    """Sample Python Pydantic model for hybrid testing."""

    username: str
    max_retries: int = 3


def test_resolve_type_hint_primitives() -> None:
    """Test resolution of basic type names."""
    assert resolve_type_hint("str") is str
    assert resolve_type_hint("string") is str
    assert resolve_type_hint("int") is int
    assert resolve_type_hint("float") is float
    assert resolve_type_hint("bool") is bool
    assert resolve_type_hint("path") is Path
    assert resolve_type_hint("Path") is Path
    assert resolve_type_hint("any") is Any


def test_resolve_type_hint_complex() -> None:
    """Test resolution of generics, unions, and literals."""
    assert resolve_type_hint("list[int]") == list[int]
    assert resolve_type_hint("dict[str, int]") == dict[str, int]
    assert resolve_type_hint("str | None") == (str | None)
    assert resolve_type_hint("Literal['fast', 'deep']") == Literal["fast", "deep"]


def test_resolve_type_hint_dotted_import() -> None:
    """Test resolution of dotted Python paths for existing classes."""
    resolved = resolve_type_hint("tests.unit.test_dynamic_models.SamplePythonModel")
    assert resolved is SamplePythonModel


def test_build_model_from_spec_basic() -> None:
    """Test dynamic model generation from field specifications."""
    spec = {
        "title": {"type": "str", "required": True},
        "count": {"type": "int", "default": 5, "minimum": 1, "maximum": 10},
        "ratio": {"type": "float", "default": 0.5},
        "mode": {"type": "str", "choices": ["auto", "manual"], "default": "auto"},
        "tags": {"type": "list[str]", "default": ["a", "b"]},
        "path": {"type": "path", "default": "/tmp"},
    }

    Model = build_model_from_spec("DemoModel", spec)
    assert issubclass(Model, BaseModel)
    assert "title" in Model.model_fields
    assert "count" in Model.model_fields

    # Valid instantiation
    inst = Model(title="hello")
    assert inst.title == "hello"
    assert inst.count == 5
    assert inst.mode == "auto"
    assert inst.tags == ["a", "b"]
    assert isinstance(inst.path, Path)

    # Validation errors
    with pytest.raises(ValidationError):
        Model()  # title is required

    with pytest.raises(ValidationError):
        Model(title="hi", count=20)  # count exceeds maximum 10

    with pytest.raises(ValidationError):
        Model(title="hi", mode="invalid")  # mode not in choices


def test_cli_string_coercion() -> None:
    """Test that CLI string representations are cleanly coerced."""
    spec = {
        "active": {"type": "bool", "default": False},
        "items": {"type": "list[int]", "default": []},
        "config": {"type": "dict", "default": {}},
    }

    Model = build_model_from_spec("CliModel", spec)

    inst = Model(active="yes", items="1, 2, 3", config='{"key": "value"}')
    assert inst.active is True
    assert inst.items == [1, 2, 3]
    assert inst.config == {"key": "value"}

    inst2 = Model(active="0", items="[10, 20]")
    assert inst2.active is False
    assert inst2.items == [10, 20]


def test_model_extends_preset() -> None:
    """Test schema inheritance via extends."""
    yaml_schema = """
presets:
  base_task:
    fields:
      task_id:
        type: str
        required: true
      timeout:
        type: int
        default: 30

entities:
  CustomTask:
    extends: base_task
    fields:
      endpoint:
        type: str
        required: true
"""
    models = load_models_from_yaml(yaml_schema)
    assert "CustomTask" in models
    TaskModel = models["CustomTask"]

    task = TaskModel(task_id="t1", endpoint="https://api.example.com")
    assert task.task_id == "t1"
    assert task.timeout == 30
    assert task.endpoint == "https://api.example.com"


def test_cross_model_reference() -> None:
    """Test forward and cross model references in schema."""
    yaml_schema = """
entities:
  User:
    fields:
      name:
        type: str
      address:
        type: Address
  Address:
    fields:
      city:
        type: str
        default: "Paris"
"""
    models = load_models_from_yaml(yaml_schema)
    UserModel = models["User"]
    AddressModel = models["Address"]

    addr = AddressModel(city="Lyon")
    user = UserModel(name="Alice", address=addr)
    assert user.address.city == "Lyon"


def test_reuse_existing_python_model() -> None:
    """Test reusing an existing Python Pydantic model directly."""
    # Pass Python class directly
    Model1 = build_model_from_spec("Imported1", SamplePythonModel)
    assert Model1 is SamplePythonModel

    # Pass dotted path string
    Model2 = build_model_from_spec("Imported2", "tests.unit.test_dynamic_models.SamplePythonModel")
    assert Model2 is SamplePythonModel
    inst = Model2(username="bob")
    assert inst.username == "bob"
    assert inst.max_retries == 3
