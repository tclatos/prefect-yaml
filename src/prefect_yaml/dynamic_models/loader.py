"""Loader for YAML schema files defining dynamic Pydantic models."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

from prefect_yaml.dynamic_models.builder import build_model_from_spec


class ModelRegistry:
    """Registry maintaining dynamic model definitions and instances."""

    def __init__(self) -> None:
        self._models: dict[str, type[BaseModel]] = {}
        self._presets: dict[str, dict[str, Any]] = {}

    def register(self, name: str, model: type[BaseModel]) -> None:
        self._models[name] = model

    def get(self, name: str) -> type[BaseModel] | None:
        return self._models.get(name)

    def register_preset(self, name: str, spec: dict[str, Any]) -> None:
        self._presets[name] = spec

    def get_preset(self, name: str) -> dict[str, Any] | None:
        return self._presets.get(name)

    @property
    def models(self) -> dict[str, type[BaseModel]]:
        return dict(self._models)


def load_models_from_dict(data: dict[str, Any], registry: ModelRegistry | None = None) -> dict[str, type[BaseModel]]:
    """Load and build models from a dictionary with two-pass forward reference resolution.

    Args:
        data: Dict containing 'presets' and 'entities' (or mapping of model names to specs).
        registry: Optional existing ModelRegistry.

    Returns:
        Mapping of entity names to their compiled Pydantic model classes.
    """
    reg = registry or ModelRegistry()

    # Register presets
    presets = data.get("presets", {})
    if isinstance(presets, dict):
        for name, spec in presets.items():
            if isinstance(spec, dict):
                reg.register_preset(name, spec)

    raw_entities: dict[str, Any] = data.get("entities", {})
    if not raw_entities and not presets:
        # If the dict is directly a map of ModelName -> field_specs
        raw_entities = {k: v for k, v in data.items() if isinstance(v, dict)}

    built_models: dict[str, type[BaseModel]] = {}

    # Pass 1: build models (cross-references to other entities resolve as ForwardRef)
    for name, spec in raw_entities.items():
        if not isinstance(spec, dict):
            continue

        fields = spec.get("fields", spec)
        extends = spec.get("extends")
        if extends and extends in reg._presets:
            parent_preset = reg._presets[extends]
            parent_fields = parent_preset.get("fields", parent_preset)
            merged_fields = dict(parent_fields)
            merged_fields.update(fields)
            fields = merged_fields
            extends = None

        model = build_model_from_spec(
            name=name,
            fields_spec=fields,
            extends=extends,
            registry=reg._models,
        )
        built_models[name] = model
        reg.register(name, model)

    # Pass 2: resolve all ForwardRefs across the built models
    for model in built_models.values():
        model.model_rebuild(_types_namespace=reg._models)

    return built_models


def load_models_from_yaml(source: Path | str, registry: ModelRegistry | None = None) -> dict[str, type[BaseModel]]:
    """Load models from a YAML file or raw YAML string.

    Args:
        source: File path (Path) or raw YAML string content.
        registry: Optional existing ModelRegistry.

    Returns:
        Mapping of entity names to compiled Pydantic models.
    """
    if isinstance(source, Path) or (isinstance(source, str) and "\n" not in source and Path(source).exists()):
        text = Path(source).read_text(encoding="utf-8")
    else:
        text = str(source)

    raw_data = yaml.safe_load(text) or {}
    if not isinstance(raw_data, dict):
        raise TypeError(f"Expected YAML dictionary, got {type(raw_data).__name__}")

    return load_models_from_dict(raw_data, registry=registry)
