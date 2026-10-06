"""PrefectFlowFactory: dynamically builds and executes Prefect flows from compiled models."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger
from pydantic import BaseModel

from prefect_yaml.cache.fingerprint import compute_step_fingerprint
from prefect_yaml.cache.manifest import ManifestCache, default_manifest_path
from prefect_yaml.compiler.dag import topological_sort
from prefect_yaml.models.compiled import CompiledWorkflow
from prefect_yaml.runtime.step_factory import PrefectStepFactory

if TYPE_CHECKING:
    from prefect import Flow


class WorkflowExecutionError(RuntimeError):
    """Exception raised when workflow step execution fails."""


def _resolve_step_ref(value: Any, results: dict[str, Any]) -> Any:
    if not isinstance(value, str):
        return value

    pattern = r"\$\{steps\.([a-zA-Z0-9_.-]+)\.result(?:\.([a-zA-Z0-9_.-]+))?\}"
    exact = re.fullmatch(pattern, value)
    if exact:
        step_id = exact.group(1)
        attr = exact.group(2)
        step_res = results.get(step_id)
        if attr:
            if isinstance(step_res, dict):
                return step_res.get(attr)
            return getattr(step_res, attr, None)
        return step_res

    def _replace(match: re.Match) -> str:
        s_id = match.group(1)
        a_id = match.group(2)
        s_res = results.get(s_id)
        if a_id:
            val = s_res.get(a_id) if isinstance(s_res, dict) else getattr(s_res, a_id, "")
        else:
            val = s_res
        return str(val if val is not None else "")

    return re.sub(pattern, _replace, value)


def _prepare_inputs(with_dict: dict[str, Any], results: dict[str, Any]) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for k, v in with_dict.items():
        val = _resolve_step_ref(v, results)
        if val is not None:
            resolved[k] = val
    return resolved


class PrefectFlowFactory(BaseModel):
    """Builder generating Prefect flow objects from CompiledWorkflow representations."""

    compiled: CompiledWorkflow
    max_workers: int = 4
    manifest_path: Path | None = None

    model_config = {"arbitrary_types_allowed": True}

    def get(self) -> Flow[[], dict[str, Any]]:
        """Construct and return the Prefect Flow function without executing it.

        Returns:
            Prefect Flow callable.
        """
        return self._build_prefect_flow()

    def run(self) -> dict[str, Any]:
        """Execute the flow in the current process and return step results.

        Returns:
            Dictionary mapping step IDs to their outputs.
        """
        flow_fn = self.get()
        return flow_fn()

    def serve(self, *, name: str | None = None, **serve_kwargs: Any) -> None:
        """Register the flow as a persistent deployment with the Prefect server.

        Args:
            name: Deployment name override.
            serve_kwargs: Additional deployment parameters.
        """
        flow_fn = self.get()
        deploy_name = name or self.compiled.name
        flow_fn.serve(
            name=deploy_name,
            parameters=self.compiled.values,
            **serve_kwargs,
        )

    @classmethod
    def from_profile(
        cls,
        name_or_preset: str,
        *,
        values: dict[str, Any] | None = None,
        source: Path | str | dict[str, Any] | None = None,
        max_workers: int = 4,
    ) -> PrefectFlowFactory:
        """Instantiate a flow factory from a workflow name or preset identifier.

        Args:
            name_or_preset: Target workflow or preset.
            values: Overriding values dict.
            source: Workflow definitions source.
            max_workers: Parallel concurrency limit.

        Returns:
            Configured PrefectFlowFactory instance.
        """
        from prefect_yaml.compiler.compiler import WorkflowCompiler
        from prefect_yaml.resolver.resolver import resolve_workflow_invocation

        invocation = resolve_workflow_invocation(name_or_preset, cli_overrides=values or {}, source=source)
        compiled = WorkflowCompiler().compile(invocation.workflow, invocation.values)
        return cls(compiled=compiled, max_workers=max_workers)

    def _build_prefect_flow(self) -> Flow[[], dict[str, Any]]:
        from prefect import flow
        from prefect.task_runners import ThreadPoolTaskRunner

        compiled = self.compiled
        sorted_steps = topological_sort(compiled.steps)
        step_factory = PrefectStepFactory()
        step_tasks = {step.id: step_factory.create(step) for step in compiled.steps}
        manifest_file = self.manifest_path or default_manifest_path(compiled.name)
        force_flag = bool(compiled.values.get("force") or compiled.values.get("force_rebuild"))

        @flow(
            name=compiled.name,
            task_runner=ThreadPoolTaskRunner(max_workers=self.max_workers),
        )
        def _flow_body() -> dict[str, Any]:
            manifest = ManifestCache.load(manifest_file)
            results: dict[str, Any] = {}
            futures: dict[str, Any] = {}

            for step in sorted_steps:
                step_inputs = _prepare_inputs(step.with_, results)
                fp = compute_step_fingerprint(step.id, step_inputs)

                if step.cache.backend in ("manifest", "hybrid") and manifest.is_fresh(
                    step.id, fingerprint=fp, force=force_flag
                ):
                    cached_out = manifest.get_output(step.id, "result")
                    results[step.id] = cached_out
                    logger.info("Step '{}' fresh in manifest cache — skipped", step.id)
                    continue

                dep_futures = [futures[dep_id] for dep_id in step.wait_for if dep_id in futures]

                if step.router is not None:
                    from prefect_yaml.routing.partitioner import execute_partition_router

                    items_expr = step.router.items
                    items_val = _resolve_step_ref(items_expr, results)
                    if isinstance(items_val, str) and items_val.startswith("${"):
                        items_val = []
                    items_list = list(items_val) if isinstance(items_val, (list, tuple)) else [items_val]
                    router_res = execute_partition_router(step.router, items_list, compiled.values)
                    results[step.id] = router_res
                    continue

                if step.foreach is not None:
                    raw_items = _resolve_step_ref(step.foreach.from_ref, results)
                    if not isinstance(raw_items, (list, tuple)):
                        raise WorkflowExecutionError(
                            f"Step '{step.id}' foreach 'from' must resolve to list, got {type(raw_items).__name__}"
                        )
                    sub_task = step_tasks[step.id]
                    foreach_futures = [
                        sub_task.submit(**{**step_inputs, step.foreach.as_var: item}, wait_for=dep_futures)
                        for item in raw_items
                    ]
                    batch_res = [f.result() for f in foreach_futures]
                    results[step.id] = batch_res
                    if step.cache.backend in ("manifest", "hybrid"):
                        manifest.record_success(step.id, fp, outputs={"result": batch_res})
                    continue

                sub_task = step_tasks[step.id]
                try:
                    fut = sub_task.submit(**step_inputs, wait_for=dep_futures)
                    futures[step.id] = fut
                    res = fut.result()
                    results[step.id] = res
                    if step.cache.backend in ("manifest", "hybrid"):
                        manifest.record_success(step.id, fp, outputs={"result": res})
                except Exception as exc:
                    if step.execution.on_failure == "abort":
                        manifest.record_failure(step.id, fp, error=str(exc))
                        manifest.save(manifest_file)
                        raise WorkflowExecutionError(f"Step '{step.id}' failed: {exc}") from exc
                    if step.execution.on_failure == "skip":
                        logger.warning("Step '{}' failed, skipping: {}", step.id, exc)
                        results[step.id] = None
                    elif step.execution.on_failure == "continue":
                        logger.warning("Step '{}' failed, continuing: {}", step.id, exc)
                        results[step.id] = {"error": str(exc)}

            if any(step.cache.backend in ("manifest", "hybrid") for step in sorted_steps) and manifest.records:
                manifest.save(manifest_file)
            return results

        return _flow_body


def flow_from_yaml(
    source: str | Path | dict[str, Any],
    *,
    workflow_name: str | None = None,
    values: dict[str, Any] | None = None,
    max_workers: int = 4,
) -> Flow[[], dict[str, Any]]:
    """Parse a workflow definition from YAML and return a Prefect flow.

    Args:
        source: YAML string, path to YAML file, or dictionary.
        workflow_name: Name of workflow to select if multiple exist.
        values: Runtime value overrides.
        max_workers: Parallel execution worker count.

    Returns:
        Callable Prefect Flow object.
    """
    from prefect_yaml.compiler.compiler import WorkflowCompiler
    from prefect_yaml.resolver.resolver import (
        _to_workflow_spec,
        default_registry,
        parse_workflows_from_dict,
    )

    if isinstance(source, Path):
        import yaml

        raw = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
    elif isinstance(source, str):
        import yaml

        raw = yaml.safe_load(source) or {}
    elif isinstance(source, dict):
        raw = source
    else:
        raise TypeError(f"source must be Path, str, or dict, got {type(source).__name__}")

    raw_workflows = raw.get("workflows", raw) if isinstance(raw, dict) else {}
    candidates = parse_workflows_from_dict(raw_workflows)

    if not candidates:
        raise ValueError("No valid workflow definitions found in source.")

    if workflow_name is None:
        if len(candidates) > 1:
            raise ValueError(f"Multiple workflows found ({', '.join(candidates)}). Specify workflow_name.")
        workflow_name = next(iter(candidates))

    wf = candidates[workflow_name]
    from prefect_yaml.resolver.resolver import _merge_values

    merged_values = _merge_values(wf.defaults, {}, values or {})
    from prefect_yaml.contracts.validator import validate_workflow_inputs

    validated_values = validate_workflow_inputs(wf.inputs, merged_values, workflow_name=wf.name)

    spec = _to_workflow_spec(wf, candidates, default_registry, extra_values=validated_values)
    compiled = WorkflowCompiler().compile(spec, validated_values)
    factory = PrefectFlowFactory(compiled=compiled, max_workers=max_workers)
    return factory.get()
