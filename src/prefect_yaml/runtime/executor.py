"""Execution entry-point running resolved workflow invocations."""

from __future__ import annotations

import importlib
import inspect
from typing import Any

from prefect_yaml.compiler.compiler import WorkflowCompiler
from prefect_yaml.models.authoring import ResolvedWorkflowInvocation
from prefect_yaml.models.compiled import StepKind
from prefect_yaml.runtime.flow_factory import PrefectFlowFactory, WorkflowExecutionError, _prepare_inputs


def _preflight_check_signatures(compiled: Any) -> None:
    """Validate step keyword arguments against callable signature before scheduling tasks."""
    for step in compiled.steps:
        if step.invoke.kind != StepKind.callable or not step.invoke.target:
            continue

        try:
            module_path, fn_name = step.invoke.target.rsplit(".", 1)
            fn = getattr(importlib.import_module(module_path), fn_name)
        except (ImportError, AttributeError, ValueError) as exc:
            raise WorkflowExecutionError(f"Step '{step.id}': cannot import '{step.invoke.target}': {exc}") from exc

        sig = inspect.signature(fn)
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
            continue

        step_inputs = _prepare_inputs(step.with_, {})
        valid_params = set(sig.parameters)
        unexpected = sorted(set(step_inputs) - valid_params)
        if unexpected:
            raise WorkflowExecutionError(
                f"Step '{step.id}': unexpected argument(s) for '{step.invoke.target}': {unexpected}.\n"
                f"Function accepts: {sorted(valid_params)}."
            )


def execute_workflow(invocation: ResolvedWorkflowInvocation) -> dict[str, Any]:
    """Execute a resolved workflow invocation using Prefect.

    Args:
        invocation: Resolved workflow invocation.

    Returns:
        Mapping of step ID to execution results.
    """
    values = dict(invocation.values)
    if invocation.force:
        values.setdefault("force", True)

    compiled = WorkflowCompiler().compile(invocation.workflow, values)
    _preflight_check_signatures(compiled)
    return PrefectFlowFactory(compiled=compiled).run()
