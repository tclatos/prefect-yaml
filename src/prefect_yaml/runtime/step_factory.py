"""Prefect step factory creating tasks for compiled steps."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

from loguru import logger
from pydantic import BaseModel

from prefect_yaml.models.compiled import CompiledStep, StepKind

if TYPE_CHECKING:
    from prefect import Task


def _import_target(dotted_path: str) -> Any:
    module_path, _, attr_name = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(f"Invalid dotted path '{dotted_path}'")
    module = importlib.import_module(module_path)
    if not hasattr(module, attr_name):
        raise AttributeError(f"Module '{module_path}' has no attribute '{attr_name}'")
    return getattr(module, attr_name)


class PrefectStepFactory(BaseModel):
    """Factory generating Prefect tasks from compiled step specifications."""

    def create(self, step: CompiledStep) -> Task:
        """Create a Prefect task object for a compiled step.

        Args:
            step: Fully compiled step specification.

        Returns:
            Configured Prefect Task object.
        """
        from prefect import task as prefect_task

        step_id = step.id
        target = step.invoke.target
        kind = step.invoke.kind
        retries = step.execution.retries
        retry_delay = step.execution.retry_delay_seconds
        tags = list(step.execution.tags or [])

        if kind == StepKind.deployment:

            def _deployment_fn(**kwargs: Any) -> Any:
                from prefect.deployments import run_deployment

                logger.info("Triggering deployment '{}' for step '{}'", target, step_id)
                return run_deployment(target, parameters=kwargs)

            _fn = _deployment_fn

        elif kind == StepKind.factory:

            def _factory_fn(**kwargs: Any) -> Any:
                factory_cls = _import_target(target)
                instance = factory_cls(**kwargs)
                return instance.get() if hasattr(instance, "get") else instance.create()

            _fn = _factory_fn

        else:
            if target:
                callable_obj = _import_target(target)
                if step.inline:
                    try:
                        from prefect.flows import Flow as PrefectFlow

                        if isinstance(callable_obj, PrefectFlow):
                            callable_obj = callable_obj.fn
                    except ImportError:
                        pass

                def _callable_fn(**kwargs: Any) -> Any:
                    return callable_obj(**kwargs)

                _fn = _callable_fn
            else:

                def _noop_fn(**kwargs: Any) -> Any:
                    return kwargs

                _fn = _noop_fn

        _fn.__name__ = step_id
        _fn.__qualname__ = f"workflow.{step_id}"
        _fn.__module__ = __name__

        cache_policy = self._cache_policy_for_step(step)

        return prefect_task(
            _fn,
            name=step_id,
            retries=retries,
            retry_delay_seconds=retry_delay,
            tags=tags or None,
            cache_policy=cache_policy,
        )

    def _cache_policy_for_step(self, step: CompiledStep) -> Any:
        backend = step.cache.backend
        if backend in ("prefect_result", "hybrid"):
            from prefect.cache_policies import INPUTS

            return INPUTS
        from prefect.cache_policies import NO_CACHE

        return NO_CACHE
