"""Unit tests for WorkflowCompiler and DAG algorithms."""

from __future__ import annotations

import pytest

from prefect_yaml.compiler.compiler import WorkflowCompiler
from prefect_yaml.compiler.dag import (
    WorkflowCompilationError,
    topological_sort,
    validate_dag,
    validate_step_ids,
    validate_wait_for_refs,
)
from prefect_yaml.models.authoring import StepSpec, WorkflowSpec
from prefect_yaml.models.compiled import CompiledStep, InvokeSpec, StepKind


def test_validate_step_ids_detects_duplicates() -> None:
    """Duplicate step IDs raise an error."""
    steps = [
        StepSpec(id="step1", invoke=InvokeSpec(target="json.dumps")),
        StepSpec(id="step1", invoke=InvokeSpec(target="json.dumps")),
    ]
    with pytest.raises(WorkflowCompilationError, match="Duplicate"):
        validate_step_ids(steps)


def test_validate_wait_for_refs_detects_unknown() -> None:
    """Unknown dependencies raise an error."""
    steps = [
        StepSpec(id="step1", invoke=InvokeSpec(target="json.dumps"), wait_for=["unknown_step"]),
    ]
    with pytest.raises(WorkflowCompilationError, match="unknown step"):
        validate_wait_for_refs(steps)


def test_validate_dag_detects_cycles() -> None:
    """Cyclic dependencies raise an error."""
    steps = [
        StepSpec(id="a", invoke=InvokeSpec(target="json.dumps"), wait_for=["b"]),
        StepSpec(id="b", invoke=InvokeSpec(target="json.dumps"), wait_for=["a"]),
    ]
    with pytest.raises(WorkflowCompilationError, match="Cycle detected"):
        validate_dag(steps)


def test_topological_sort_orders_steps() -> None:
    """Topological sort returns steps in dependency order."""
    steps = [
        CompiledStep(id="c", invoke=InvokeSpec(), wait_for=["b"]),
        CompiledStep(id="a", invoke=InvokeSpec()),
        CompiledStep(id="b", invoke=InvokeSpec(), wait_for=["a"]),
    ]
    sorted_steps = topological_sort(steps)
    ids = [s.id for s in sorted_steps]
    assert ids == ["a", "b", "c"]


def test_compiler_interpolates_values() -> None:
    """Compiler interpolates ${values.*} in step kwargs."""
    spec = WorkflowSpec(
        name="test_wf",
        steps=[
            StepSpec(
                id="step1",
                invoke=InvokeSpec(kind=StepKind.callable, target="json.dumps"),
                with_={"path": "${values.root_dir}", "count": 5},
            )
        ],
    )
    compiled = WorkflowCompiler().compile(spec, {"root_dir": "/tmp/test"})
    assert compiled.steps[0].with_["path"] == "/tmp/test"
    assert compiled.steps[0].with_["count"] == 5
