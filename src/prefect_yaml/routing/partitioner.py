"""Partition router: classifies collections of items and dispatches them in parallel."""

from __future__ import annotations

import json
from typing import Any

from loguru import logger
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner

from prefect_yaml.models.compiled import RouterRule, RouterSpec
from prefect_yaml.routing.matcher import matches_pattern


class RoutingError(ValueError):
    """Exception raised when an item cannot be matched to any route."""


def partition_items(
    items: list[Any],
    rules: list[RouterRule],
    default: str | RouterRule | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Partition items into buckets according to matching rules.

    Args:
        items: List of items to classify.
        rules: Ordered list of routing rules.
        default: Fallback workflow or rule when no pattern matches.

    Returns:
        Mapping of bucket key tuple to bucket information dictionary.
    """
    buckets: dict[tuple[str, str], dict[str, Any]] = {}

    for item in items:
        item_str = str(item)
        matched_rule: RouterRule | None = None

        for rule in rules:
            if matches_pattern(rule.match, item_str):
                matched_rule = rule
                break

        if matched_rule is not None:
            target_wf = matched_rule.run
            params = dict(matched_rule.with_)
        elif default is not None:
            if isinstance(default, RouterRule):
                target_wf = default.run
                params = dict(default.with_)
            else:
                target_wf = str(default)
                params = {}
        else:
            raise RoutingError(f"No route matched item '{item_str}' and no default fallback was provided.")

        key = (target_wf, json.dumps(params, sort_keys=True))
        bucket = buckets.setdefault(key, {"workflow": target_wf, "params": params, "items": []})
        bucket["items"].append(item)

    return buckets


@task
def _run_bucket_task(
    workflow_target: str,
    bucket_values: dict[str, Any],
) -> dict[str, Any]:
    import importlib

    from prefect_yaml.resolver.expander import WorkflowResolutionError
    from prefect_yaml.runtime.flow_factory import PrefectFlowFactory

    try:
        factory = PrefectFlowFactory.from_profile(workflow_target, values=bucket_values)
        res = factory.run()
    except WorkflowResolutionError:
        module_path, _, attr_name = workflow_target.rpartition(".")
        if not module_path:
            raise
        mod = importlib.import_module(module_path)
        fn = getattr(mod, attr_name)
        res = fn(**bucket_values)

    if isinstance(res, dict):
        if set(res) == {"run"}:
            unwrapped = res["run"]
            return unwrapped if isinstance(unwrapped, dict) else {"result": unwrapped}
        return res
    return {"result": res}


@flow(name="partition_dispatch", task_runner=ThreadPoolTaskRunner(max_workers=8))
def execute_partition_router(
    router_spec: RouterSpec,
    resolved_items: list[Any],
    base_values: dict[str, Any],
) -> dict[str, Any]:
    """Execute parallel fan-out routing across bucketed items.

    Args:
        router_spec: Specification of rules and settings.
        resolved_items: Evaluated collection of items to partition.
        base_values: Global values to pass along.

    Returns:
        Summary dictionary with per-bucket results and any failures.
    """
    if not resolved_items:
        return {"buckets": {}, "failures": [], "total_items": 0}

    buckets = partition_items(
        resolved_items,
        router_spec.rules,
        default=router_spec.default,
    )

    futures: dict[tuple[str, str], Any] = {}
    for bucket_key, bucket in buckets.items():
        call_values = {
            **base_values,
            **bucket["params"],
            router_spec.as_var: bucket["items"],
        }
        futures[bucket_key] = _run_bucket_task.submit(bucket["workflow"], call_values)

    results: dict[str, Any] = {}
    failures: list[str] = []

    for (wf_name, _params), future in futures.items():
        try:
            results[wf_name] = future.result()
        except Exception as exc:
            logger.error("Routed workflow '{}' failed: {}", wf_name, exc)
            failures.append(f"{wf_name}: {exc}")

    return {
        "buckets": results,
        "failures": failures,
        "total_items": len(resolved_items),
    }
