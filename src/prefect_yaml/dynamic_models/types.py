"""Type resolution from YAML type strings and dotted Python paths."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, ForwardRef, Literal

PRIMITIVE_TYPE_MAP: dict[str, type] = {
    "str": str,
    "string": str,
    "int": int,
    "integer": int,
    "float": float,
    "number": float,
    "bool": bool,
    "boolean": bool,
    "path": Path,
    "Path": Path,
    "any": Any,
    "Any": Any,
    "dict": dict[str, Any],
    "list": list[Any],
    "None": type(None),
    "none": type(None),
}


def _import_symbol(dotted_path: str) -> Any:
    """Import a class or symbol from a dotted Python path."""
    module_path, _, attr_name = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(f"Invalid dotted path: '{dotted_path}'")
    module = importlib.import_module(module_path)
    if not hasattr(module, attr_name):
        raise AttributeError(f"Module '{module_path}' has no attribute '{attr_name}'")
    return getattr(module, attr_name)


def resolve_type_hint(spec: Any, registry: dict[str, Any] | None = None) -> Any:
    """Resolve a type specification (string, type, or expression) to a Python type.

    Supports:
      - Primitive aliases: 'str', 'int', 'float', 'bool', 'path', 'any'
      - Generic lists: 'list[int]', 'list[str]'
      - Generic dicts: 'dict[str, Any]', 'dict[str, int]'
      - Unions / Optionals: 'str | None', 'int | float'
      - Choices via Literal: ['fast', 'deep']
      - Dotted Python paths: 'my_package.models.MyModel'
      - Local model registry references
    """
    if spec is None:
        return Any

    if isinstance(spec, type):
        return spec

    if not isinstance(spec, str):
        return Any

    spec = spec.strip()

    # 1. Registry lookup (for forward/cross references)
    if registry and spec in registry:
        return registry[spec]

    # 2. Union / Optional: 'A | B'
    if " | " in spec:
        parts = [resolve_type_hint(part.strip(), registry) for part in spec.split(" | ")]
        result = parts[0]
        for part in parts[1:]:
            result = result | part
        return result

    # 3. List: 'list[T]'
    if spec.startswith("list[") and spec.endswith("]"):
        inner = spec[5:-1].strip()
        inner_type = resolve_type_hint(inner, registry)
        return list[inner_type]  # type: ignore[valid-type]

    # 4. Dict: 'dict[K, V]'
    if spec.startswith("dict[") and spec.endswith("]"):
        inner = spec[5:-1].strip()
        if ", " in inner:
            k_spec, v_spec = inner.split(", ", 1)
        elif "," in inner:
            k_spec, v_spec = inner.split(",", 1)
        else:
            k_spec, v_spec = "str", "Any"
        return dict[resolve_type_hint(k_spec, registry), resolve_type_hint(v_spec, registry)]  # type: ignore[valid-type]

    # 5. Literal: 'Literal["a", "b"]'
    if spec.startswith("Literal[") and spec.endswith("]"):
        raw_items = [item.strip().strip("'\"") for item in spec[8:-1].split(",")]
        return Literal[tuple(raw_items)]  # type: ignore[valid-type]

    # 6. Primitive alias mapping
    if spec in PRIMITIVE_TYPE_MAP:
        return PRIMITIVE_TYPE_MAP[spec]

    lower_spec = spec.lower()
    if lower_spec in PRIMITIVE_TYPE_MAP:
        return PRIMITIVE_TYPE_MAP[lower_spec]

    # 7. Dotted Python import path
    if "." in spec:
        try:
            return _import_symbol(spec)
        except Exception as exc:
            raise ValueError(f"Could not import type '{spec}': {exc}") from exc

    # 8. Local identifier / Forward reference (e.g. 'Address', 'User')
    if spec.isidentifier():
        return ForwardRef(spec)

    raise ValueError(f"Unknown or unsupported type specification: '{spec}'")
