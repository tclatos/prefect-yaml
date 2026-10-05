"""Pipeline expander handling sub-workflow inlining and dependency leaf rewrites."""

from __future__ import annotations

from typing import Any

from prefect_yaml.models.authoring import PipelineStep, StepSpec, WorkflowDef
from prefect_yaml.models.compiled import InvokeSpec, StepKind
from prefect_yaml.registry import WorkflowRegistry


class WorkflowResolutionError(ValueError):
    """Exception raised when workflow resolution fails."""


def _resolve_run_to_path(
    run_target: str,
    all_workflows: dict[str, WorkflowDef],
    reg: WorkflowRegistry,
) -> str:
    reg_entry = reg.get(run_target)
    if reg_entry is not None:
        return reg_entry.dotted_path
    return run_target


def _resolve_deps(
    deps: list[str],
    terminal_map: dict[str, list[str]],
) -> list[str]:
    result: list[str] = []
    for dep in deps:
        if dep in terminal_map:
            result.extend(terminal_map[dep])
        else:
            result.append(dep)
    return result


def expand_pipeline(
    pipeline: list[PipelineStep],
    all_workflows: dict[str, WorkflowDef],
    reg: WorkflowRegistry,
    *,
    _ancestors: frozenset[str] = frozenset(),
    extra_values: dict[str, Any] | None = None,
) -> list[StepSpec]:
    """Recursively expand a list of pipeline steps into a flat StepSpec list.

    Args:
        pipeline: List of authoring pipeline steps.
        all_workflows: Mapping of all known workflow definitions.
        reg: Workflow registry for decorated callables.
        _ancestors: Set of ancestor workflow names for cycle detection.
        extra_values: Optional values dict to forward.

    Returns:
        Expanded list of normalized step specifications.
    """
    result: list[StepSpec] = []
    terminal_map: dict[str, list[str]] = {}

    for ps in pipeline:
        is_sub_wf = ps.run in all_workflows and ps.run not in _ancestors

        if is_sub_wf and ps.run is not None:
            sub_name = ps.run
            if sub_name in _ancestors:
                raise WorkflowResolutionError(
                    f"Workflow composition cycle: {' -> '.join(sorted(_ancestors))} -> {sub_name}"
                )

            sub_wf = all_workflows[sub_name]
            if sub_wf.run:
                auto_with: dict[str, Any] = {k: f"${{values.{k}}}" for k in sub_wf.defaults}
                for param_name in sub_wf.inputs:
                    if param_name not in auto_with:
                        auto_with[param_name] = f"${{values.{param_name}}}"
                if extra_values:
                    for k in ("force", "dry_run"):
                        if k in extra_values and k not in auto_with:
                            auto_with[k] = f"${{values.{k}}}"

                sub_pipeline = [PipelineStep(id="run", run=sub_wf.run, with_=auto_with)]
                sub_pipeline[0].cache = sub_wf.resolved_cache()
            else:
                sub_pipeline = sub_wf.pipeline

            sub_steps = expand_pipeline(
                sub_pipeline,
                all_workflows,
                reg,
                _ancestors=_ancestors | {sub_name},
                extra_values=extra_values,
            )

            parent_deps = _resolve_deps(ps.dependencies, terminal_map)
            step_id = ps.id
            sub_ids = {s.id for s in sub_steps}
            depended_on = {dep for s in sub_steps for dep in s.wait_for}
            leaf_ids = [s.id for s in sub_steps if s.id not in depended_on]
            terminal_map[step_id] = (
                [f"{step_id}.{sid}" for sid in leaf_ids]
                if leaf_ids
                else ([f"{step_id}.{sub_steps[-1].id}"] if sub_steps else [step_id])
            )

            for sub_step in sub_steps:
                is_root = not (set(sub_step.wait_for) & sub_ids)
                new_wait_for = [f"{step_id}.{n}" for n in sub_step.wait_for]
                if is_root and parent_deps:
                    new_wait_for = parent_deps + new_wait_for

                merged_with = {**sub_step.with_, **ps.with_} if is_root else dict(sub_step.with_)

                result.append(
                    StepSpec(
                        id=f"{step_id}.{sub_step.id}",
                        invoke=sub_step.invoke,
                        wait_for=new_wait_for,
                        with_=merged_with,
                        cache=sub_step.cache,
                        execution=sub_step.execution,
                        artifacts=sub_step.artifacts,
                        foreach=sub_step.foreach,
                        inline=sub_step.inline or ps.inline,
                        router=sub_step.router,
                        switch=sub_step.switch,
                    )
                )
        else:
            target = _resolve_run_to_path(ps.run, all_workflows, reg) if ps.run else ""
            deps = _resolve_deps(ps.dependencies, terminal_map)
            result.append(
                StepSpec(
                    id=ps.id,
                    invoke=InvokeSpec(kind=StepKind.callable, target=target),
                    wait_for=deps,
                    with_=dict(ps.with_),
                    cache=ps.resolved_cache(),
                    execution=ps.execution,
                    artifacts=ps.artifacts,
                    foreach=ps.foreach,
                    inline=ps.inline,
                    router=ps.router,
                    switch=ps.switch,
                )
            )

    return result
