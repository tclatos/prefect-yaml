"""Builder for dynamically creating Pydantic v2 models from YAML specifications."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, create_model, model_validator

from prefect_yaml.dynamic_models.types import resolve_type_hint


class DynamicBaseModel(BaseModel):
    """Base model for dynamically generated models with flexible string coercion."""

    model_config = {
        "extra": "allow",
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    @model_validator(mode="before")
    @classmethod
    def _coerce_cli_and_string_values(cls, data: Any) -> Any:
        """Pre-coerce CLI string inputs (e.g. lists, dicts, booleans)."""
        if not isinstance(data, dict):
            return data

        coerced: dict[str, Any] = dict(data)
        fields = cls.model_fields

        for field_name, field_info in fields.items():
            if field_name not in coerced:
                continue
            val = coerced[field_name]
            if not isinstance(val, str):
                continue

            ann = field_info.annotation
            origin = getattr(ann, "__origin__", ann)

            # 1. Boolean string parsing
            if origin is bool:
                val_lower = val.strip().lower()
                if val_lower in ("true", "1", "yes", "on"):
                    coerced[field_name] = True
                elif val_lower in ("false", "0", "no", "off"):
                    coerced[field_name] = False

            # 2. List string parsing
            elif origin is list or (isinstance(origin, type) and issubclass(origin, (list, tuple))):
                val_strip = val.strip()
                if val_strip.startswith("[") and val_strip.endswith("]"):
                    try:
                        coerced[field_name] = json.loads(val_strip)
                    except Exception:
                        coerced[field_name] = [x.strip() for x in val_strip[1:-1].split(",") if x.strip()]
                elif "," in val_strip:
                    coerced[field_name] = [x.strip() for x in val_strip.split(",") if x.strip()]
                elif val_strip:
                    coerced[field_name] = [val_strip]
                else:
                    coerced[field_name] = []

            # 3. Dict string parsing
            elif origin is dict or (isinstance(origin, type) and issubclass(origin, dict)):
                val_strip = val.strip()
                if val_strip.startswith("{") and val_strip.endswith("}"):
                    try:
                        coerced[field_name] = json.loads(val_strip)
                    except Exception:
                        pass

        return coerced


def build_field_definition(
    spec: Any,
    registry: dict[str, Any] | None = None,
) -> tuple[type, Any]:
    """Convert a single field specification into a (type, FieldInfo) tuple for create_model."""
    # Shorthand: simple type string or type object, e.g. "int" or int
    if not isinstance(spec, dict):
        resolved_type = resolve_type_hint(spec, registry)
        return (resolved_type, Field(default=None))

    type_spec = spec.get("type", "any")
    items_type = spec.get("items_type")
    choices = spec.get("choices")
    description = spec.get("description", "")
    required = spec.get("required", False)
    default = spec.get("default", None)

    # Resolve target type
    if choices:
        resolved_type = Literal[tuple(choices)]  # type: ignore[valid-type]
    elif type_spec in ("list", "List") and items_type:
        inner = resolve_type_hint(items_type, registry)
        resolved_type = list[inner]  # type: ignore[valid-type]
    else:
        resolved_type = resolve_type_hint(type_spec, registry)

    # Numeric & regex constraints
    field_kwargs: dict[str, Any] = {"description": description}

    min_val = spec.get("minimum", spec.get("ge"))
    if min_val is not None:
        field_kwargs["ge"] = min_val

    max_val = spec.get("maximum", spec.get("le"))
    if max_val is not None:
        field_kwargs["le"] = max_val

    pattern = spec.get("regex", spec.get("pattern"))
    if pattern is not None:
        field_kwargs["pattern"] = pattern

    # Determine default vs required
    if required:
        field_info = Field(..., **field_kwargs)
    elif default is not None:
        if isinstance(default, list):
            field_info = Field(default_factory=lambda d=default: list(d), **field_kwargs)
        elif isinstance(default, dict):
            field_info = Field(default_factory=lambda d=default: dict(d), **field_kwargs)
        elif resolved_type is Path and isinstance(default, str):
            field_info = Field(default=Path(default), **field_kwargs)
        else:
            field_info = Field(default=default, **field_kwargs)
    else:
        field_info = Field(default=None, **field_kwargs)

    return (resolved_type, field_info)


def build_model_from_spec(
    name: str,
    fields_spec: dict[str, Any] | str | type[BaseModel],
    *,
    extends: str | type[BaseModel] | None = None,
    registry: dict[str, Any] | None = None,
    base_model: type[BaseModel] | None = None,
) -> type[BaseModel]:
    """Dynamically construct a Pydantic v2 BaseModel from a specification dictionary or reuse an existing model.

    Args:
        name: Name of the generated model class.
        fields_spec: Dict of field definitions, dotted Python path to a BaseModel, or a BaseModel class.
        extends: Optional parent model name in registry, dotted path, or BaseModel class.
        registry: Model registry for cross-references.
        base_model: Optional base class (defaults to DynamicBaseModel).

    Returns:
        Dynamically constructed or imported Pydantic BaseModel class.
    """
    # 1. Direct model pass-through or dotted Python path
    if isinstance(fields_spec, type) and issubclass(fields_spec, BaseModel):
        return fields_spec

    if isinstance(fields_spec, str):
        imported = resolve_type_hint(fields_spec, registry)
        if isinstance(imported, type) and issubclass(imported, BaseModel):
            return imported
        raise ValueError(f"Dotted path '{fields_spec}' resolved to '{imported}', which is not a Pydantic BaseModel.")

    # 2. Determine base model (with extends support)
    parent_cls: type[BaseModel] = base_model or DynamicBaseModel
    if extends:
        if isinstance(extends, type) and issubclass(extends, BaseModel):
            parent_cls = extends
        elif isinstance(extends, str):
            resolved_parent = resolve_type_hint(extends, registry)
            if isinstance(resolved_parent, type) and issubclass(resolved_parent, BaseModel):
                parent_cls = resolved_parent
            else:
                raise TypeError(f"extends target '{extends}' is not a Pydantic BaseModel")

    # 3. Build create_model field arguments
    fields_kwargs: dict[str, Any] = {}
    for field_name, fspec in fields_spec.items():
        fields_kwargs[field_name] = build_field_definition(fspec, registry=registry)

    model = create_model(
        name,
        __base__=parent_cls,
        **fields_kwargs,
    )

    if registry is not None:
        registry[name] = model

    return model
