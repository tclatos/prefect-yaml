"""Registry and decorator for Python-defined workflow callables."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel


class RegisteredWorkflow(BaseModel):
    """Metadata for a registered workflow callable."""

    name: str
    description: str
    dotted_path: str
    callable_: Any
    hidden: bool = False

    model_config = {"arbitrary_types_allowed": True}

    def get_params_schema(self) -> dict[str, dict[str, Any]]:
        """Introspect callable signature to extract parameter information.

        Returns:
            Mapping of parameter names to parameter metadata.
        """
        sig = inspect.signature(self.callable_)
        params: dict[str, dict[str, Any]] = {}
        for pname, param in sig.parameters.items():
            if pname in ("self", "cls"):
                continue
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue
            info: dict[str, Any] = {}
            if param.default is inspect.Parameter.empty:
                info["required"] = True
            else:
                info["required"] = False
                info["default"] = param.default
            if param.annotation is not inspect.Parameter.empty:
                info["annotation"] = param.annotation
            params[pname] = info
        return params


class WorkflowRegistry(BaseModel):
    """Global registry of callable workflows."""

    entries: dict[str, RegisteredWorkflow] = {}

    model_config = {"arbitrary_types_allowed": True}

    def register(
        self,
        fn: Callable,
        *,
        name: str | None = None,
        description: str = "",
        hidden: bool = False,
    ) -> None:
        """Register a callable function under an optional workflow name.

        Args:
            fn: Function to register.
            name: Workflow registration name.
            description: Human-readable description.
            hidden: Whether to hide from listings.
        """
        wf_name = name or fn.__name__
        module = getattr(fn, "__module__", None) or ""
        qualname = getattr(fn, "__qualname__", fn.__name__)
        dotted_path = f"{module}.{qualname}" if module else qualname
        desc = description or (fn.__doc__ or "").strip().split("\n")[0]
        self.entries[wf_name] = RegisteredWorkflow(
            name=wf_name,
            description=desc,
            dotted_path=dotted_path,
            callable_=fn,
            hidden=hidden,
        )

    def get(self, name: str) -> RegisteredWorkflow | None:
        """Retrieve registered workflow metadata by name.

        Args:
            name: Name of the registered workflow.

        Returns:
            Registered workflow metadata if found, None otherwise.
        """
        return self.entries.get(name)

    def list_all(self) -> list[RegisteredWorkflow]:
        """Return all registered workflow entries sorted by name.

        Returns:
            List of registered workflow metadata objects.
        """
        return sorted(self.entries.values(), key=lambda e: e.name)

    def __contains__(self, name: str) -> bool:
        return name in self.entries


registry = WorkflowRegistry()


def workflow(
    fn: Callable | None = None,
    *,
    name: str | None = None,
    description: str = "",
    hidden: bool = False,
) -> Any:
    """Decorator to register a callable as a named workflow.

    Args:
        fn: The callable to register.
        name: Override the registration name.
        description: Human-readable description.
        hidden: Omit from workflow listings when True.

    Returns:
        The original callable unchanged.
    """
    if fn is not None:
        registry.register(fn, name=name, description=description, hidden=hidden)
        return fn

    def decorator(fn2: Callable) -> Callable:
        registry.register(fn2, name=name, description=description, hidden=hidden)
        return fn2

    return decorator
