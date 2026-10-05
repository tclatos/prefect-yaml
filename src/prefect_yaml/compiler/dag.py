"""DAG validation and topological sorting algorithms."""

from __future__ import annotations

from prefect_yaml.models.authoring import StepSpec
from prefect_yaml.models.compiled import CompiledStep


class WorkflowCompilationError(ValueError):
    """Exception raised when a workflow fails compilation or DAG validation."""


def validate_step_ids(steps: list[StepSpec] | list[CompiledStep]) -> None:
    """Verify that all step identifiers within a pipeline are unique.

    Args:
        steps: List of step specifications.
    """
    seen: set[str] = set()
    for step in steps:
        if step.id in seen:
            raise WorkflowCompilationError(f"Duplicate step ID: '{step.id}'")
        seen.add(step.id)


def validate_wait_for_refs(steps: list[StepSpec] | list[CompiledStep]) -> None:
    """Ensure all dependencies reference existing step IDs.

    Args:
        steps: List of step specifications.
    """
    step_ids = {s.id for s in steps}
    for step in steps:
        for dep in step.wait_for:
            if dep not in step_ids:
                raise WorkflowCompilationError(f"Step '{step.id}' has wait_for dependency on unknown step '{dep}'.")


def validate_dag(steps: list[StepSpec] | list[CompiledStep]) -> None:
    """Check for cyclic dependencies using Kahn's algorithm.

    Args:
        steps: List of step specifications.
    """
    in_degree = {s.id: len(s.wait_for) for s in steps}
    queue = [sid for sid, deg in in_degree.items() if deg == 0]
    visited = 0

    while queue:
        current = queue.pop(0)
        visited += 1
        for s in steps:
            if current in s.wait_for:
                in_degree[s.id] -= 1
                if in_degree[s.id] == 0:
                    queue.append(s.id)

    if visited != len(steps):
        raise WorkflowCompilationError("Cycle detected in workflow step dependencies.")


def topological_sort(steps: list[CompiledStep]) -> list[CompiledStep]:
    """Sort compiled steps into valid execution order.

    Args:
        steps: List of compiled steps.

    Returns:
        Topologically ordered list of steps.
    """
    step_map = {s.id: s for s in steps}
    in_degree = {s.id: len(s.wait_for) for s in steps}
    queue = [step_map[sid] for sid, deg in in_degree.items() if deg == 0]
    ordered: list[CompiledStep] = []

    while queue:
        current = queue.pop(0)
        ordered.append(current)
        for s in steps:
            if current.id in s.wait_for:
                in_degree[s.id] -= 1
                if in_degree[s.id] == 0:
                    queue.append(step_map[s.id])

    return ordered
