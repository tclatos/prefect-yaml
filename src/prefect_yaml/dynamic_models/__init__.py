"""Dynamic Pydantic model generation from YAML specifications and type expressions.

This package is completely isolated and self-contained: it has no dependencies on Prefect
or workflow internals, and can be reused in any Python project requiring schema-driven
Pydantic v2 model construction.
"""

from __future__ import annotations

from prefect_yaml.dynamic_models.builder import DynamicBaseModel, build_field_definition, build_model_from_spec
from prefect_yaml.dynamic_models.loader import ModelRegistry, load_models_from_dict, load_models_from_yaml
from prefect_yaml.dynamic_models.types import PRIMITIVE_TYPE_MAP, resolve_type_hint

__all__ = [
    "PRIMITIVE_TYPE_MAP",
    "DynamicBaseModel",
    "ModelRegistry",
    "build_field_definition",
    "build_model_from_spec",
    "load_models_from_dict",
    "load_models_from_yaml",
    "resolve_type_hint",
]
