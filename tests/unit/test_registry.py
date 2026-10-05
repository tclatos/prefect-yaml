"""Unit tests for Python workflow callable decorator and registry."""

from __future__ import annotations

from prefect_yaml.registry import WorkflowRegistry, workflow


def test_workflow_decorator_registers_callable() -> None:
    """Test registering functions via @workflow decorator."""
    reg = WorkflowRegistry()

    @workflow(name="custom_step", description="Custom step description")
    def my_fn(x: int, y: str = "default") -> None:
        pass

    reg.register(my_fn, name="custom_step", description="Custom step description")

    entry = reg.get("custom_step")
    assert entry is not None
    assert entry.name == "custom_step"
    assert entry.description == "Custom step description"
    assert entry.callable_ is my_fn

    schema = entry.get_params_schema()
    assert "x" in schema
    assert schema["x"]["required"] is True
    assert "y" in schema
    assert schema["y"]["required"] is False
    assert schema["y"]["default"] == "default"
