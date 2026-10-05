"""Compiler converting authoring WorkflowSpec into execution CompiledWorkflow."""

from __future__ import annotations

import importlib
import re
from typing import Any

from loguru import logger
from pydantic import BaseModel

from prefect_yaml.compiler.dag import (
    WorkflowCompilationError,
    validate_dag,
    validate_step_ids,
    validate_wait_for_refs,
)
from prefect_yaml.models.authoring import StepSpec, WorkflowSpec
from prefect_yaml.models.compiled import (
    ArtifactSpec,
    CacheSpec,
    CompiledStep,
    CompiledWorkflow,
    ExecutionSpec,
    InvokeSpec,
    StepKind,
)


def _resolve_values_dict(data: Any, values: dict[str, Any]) -> Any:
    if isinstance(data, dict):
        return {k: _resolve_values_dict(v, values) for k, v in data.items()}
    if isinstance(data, list):
        return [_resolve_values_dict(item, values) for item in data]
    if isinstance(data, str):
        pattern = r"\$\{values\.([a-zA-Z0-9_.-]+)\}"
        exact = re.fullmatch(pattern, data)
        if exact:
            key = exact.group(1)
            return values.get(key)
        return re.sub(pattern, lambda m: str(values.get(m.group(1), "")), data)
    return data


def _import_target(dotted_path: str) -> Any:
    module_path, _, attr_name = dotted_path.rpartition(".")
    if not module_path:
        raise WorkflowCompilationError(f"Invalid dotted path '{dotted_path}': must be module.attr")
    module = importlib.import_module(module_path)
    if not hasattr(module, attr_name):
        raise WorkflowCompilationError(f"Module '{module_path}' has no attribute '{attr_name}'")
    return getattr(module, attr_name)


class WorkflowCompiler(BaseModel):
    """Compiler transforming WorkflowSpec into CompiledWorkflow."""

    def compile(self, spec: WorkflowSpec, values: dict[str, Any]) -> CompiledWorkflow:
        """Compile a workflow specification with resolved values.

        Args:
            spec: Workflow specification.
            values: Resolved values mapping.

        Returns:
            The compiled workflow representation.
        """
        validate_step_ids(spec.steps)
        validate_wait_for_refs(spec.steps)
        validate_dag(spec.steps)

        compiled_steps = [self._compile_step(step, values) for step in spec.steps]
        logger.debug("Compiled workflow '{}': {} steps", spec.name, len(compiled_steps))

        return CompiledWorkflow(
            name=spec.name,
            description=spec.description,
            steps=compiled_steps,
            values=values,
            inputs=dict(spec.inputs),
        )

    def _compile_step(self, step: StepSpec, values: dict[str, Any]) -> CompiledStep:
        invoke = self._resolve_invoke(step)
        resolved_with = _resolve_values_dict(step.with_, values)

        return CompiledStep(
            id=step.id,
            invoke=invoke,
            wait_for=list(step.wait_for),
            with_=resolved_with,
            execution=ExecutionSpec(**step.execution.model_dump()),
            cache=CacheSpec(**step.cache.model_dump()),
            artifacts=ArtifactSpec(**step.artifacts.model_dump()),
            foreach=step.foreach,
            inline=step.inline,
            router=step.router,
            switch=step.switch,
        )

    def _resolve_invoke(self, step: StepSpec) -> InvokeSpec:
        if step.router is not None:
            return InvokeSpec(kind=StepKind.router, target="")
        if step.switch is not None:
            return InvokeSpec(kind=StepKind.switch, target="")

        if not step.invoke.target:
            raise WorkflowCompilationError(f"Step '{step.id}' has no target or router/switch specification.")

        if step.invoke.kind != StepKind.callable:
            return step.invoke

        try:
            obj = _import_target(step.invoke.target)
        except Exception:
            return InvokeSpec(kind=StepKind.callable, target=step.invoke.target)

        try:
            from prefect.flows import Flow as PrefectFlow
            from prefect.tasks import Task as PrefectTask

            if isinstance(obj, PrefectFlow):
                return InvokeSpec(kind=StepKind.flow, target=step.invoke.target)
            if isinstance(obj, PrefectTask):
                return InvokeSpec(kind=StepKind.task, target=step.invoke.target)
        except ImportError:
            pass

        return InvokeSpec(kind=StepKind.callable, target=step.invoke.target)
