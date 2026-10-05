"""Unit tests for Prefect runtime factories and step execution."""

from __future__ import annotations

from typing import Any

import pytest

from prefect_yaml.models.compiled import (
    CacheSpec,
    CompiledStep,
    CompiledWorkflow,
    ExecutionSpec,
    InvokeSpec,
    StepKind,
)
from prefect_yaml.runtime.executor import _preflight_check_signatures
from prefect_yaml.runtime.flow_factory import (
    PrefectFlowFactory,
    WorkflowExecutionError,
    _prepare_inputs,
)


def test_prepare_inputs_resolves_step_results() -> None:
    """Inputs resolve step results and object attributes."""
    results = {
        "step_a": {"total": 42},
        "step_b": "hello",
    }
    with_dict = {
        "count": "${steps.step_a.result.total}",
        "msg": "${steps.step_b.result}",
        "raw": 99,
    }
    resolved = _prepare_inputs(with_dict, results)
    assert resolved == {"count": 42, "msg": "hello", "raw": 99}


def test_preflight_signature_check() -> None:
    """Preflight check identifies unexpected keyword arguments before execution."""
    step = CompiledStep(
        id="test_step",
        invoke=InvokeSpec(kind=StepKind.callable, target="prefect_yaml.cache.fingerprint.compute_buffer_hash"),
        with_={"unexpected_arg": "invalid"},
    )
    wf = CompiledWorkflow(name="wf", steps=[step])
    with pytest.raises(WorkflowExecutionError, match="unexpected argument"):
        _preflight_check_signatures(wf)


class _MockFuture:
    """Mock future returning pre-computed values synchronously."""

    def __init__(self, value: Any = None, exception: BaseException | None = None) -> None:
        self._value = value
        self._exc = exception

    def result(self) -> Any:
        if self._exc is not None:
            raise self._exc
        return self._value


class _MockTask:
    """Mock Prefect task executing synchronously."""

    def __init__(self, fn: Any) -> None:
        self._fn = fn

    def submit(self, *args: Any, wait_for: Any = None, **kwargs: Any) -> _MockFuture:
        try:
            return _MockFuture(value=self._fn(*args, **kwargs))
        except Exception as exc:
            return _MockFuture(exception=exc)


def test_flow_factory_execution(tmp_path) -> None:
    """FlowFactory compiles and executes plain functions through Prefect flow.fn() with mock task runner."""
    from unittest.mock import patch

    from prefect_yaml.cache.fingerprint import compute_buffer_hash

    steps = [
        CompiledStep(
            id="hash_step",
            invoke=InvokeSpec(kind=StepKind.callable, target="prefect_yaml.cache.fingerprint.compute_buffer_hash"),
            with_={"data": b"hello"},
            cache=CacheSpec(backend="none"),
        )
    ]
    wf = CompiledWorkflow(name="test_hash", steps=steps)
    factory = PrefectFlowFactory(compiled=wf, manifest_path=tmp_path / "manifest.json")

    with patch("prefect_yaml.runtime.step_factory.PrefectStepFactory.create") as mock_create:
        mock_create.return_value = _MockTask(compute_buffer_hash)
        flow_fn = factory.get()
        results = flow_fn.fn()

    assert "hash_step" in results
    assert len(results["hash_step"]) == 16


def test_flow_factory_failure_policy_continue(tmp_path) -> None:
    """Steps with on_failure='continue' record error and allow execution to proceed."""
    from unittest.mock import patch

    def _failing_step() -> None:
        raise ValueError("step failure")

    steps = [
        CompiledStep(
            id="bad_step",
            invoke=InvokeSpec(kind=StepKind.callable, target="dummy"),
            execution=ExecutionSpec(on_failure="continue"),
            cache=CacheSpec(backend="none"),
        )
    ]
    wf = CompiledWorkflow(name="test_fail", steps=steps)
    factory = PrefectFlowFactory(compiled=wf, manifest_path=tmp_path / "manifest.json")

    with patch("prefect_yaml.runtime.step_factory.PrefectStepFactory.create") as mock_create:
        mock_create.return_value = _MockTask(_failing_step)
        flow_fn = factory.get()
        results = flow_fn.fn()

    assert "bad_step" in results
    assert "error" in results["bad_step"]
