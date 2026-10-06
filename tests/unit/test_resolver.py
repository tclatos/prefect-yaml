"""Unit tests for workflow resolver and sub-workflow expansion."""

from __future__ import annotations

from prefect_yaml.resolver.resolver import (
    load_workflows,
    parse_cli_overrides,
    parse_workflows_from_dict,
    resolve_workflow_invocation,
)


def test_parse_cli_overrides() -> None:
    """Test parsing CLI key=value pairs into nested dictionaries."""
    res = parse_cli_overrides(["batch_size=10", "nested.key=val", "flag=true"])
    assert res == {"batch_size": 10, "nested": {"key": "val"}, "flag": True}


def test_parse_workflows_from_dict() -> None:
    """Test loading WorkflowDef from dictionary."""
    raw = {
        "simple": {
            "run": "json.dumps",
            "defaults": {"root": "/tmp"},
            "inputs": {"root": {"type": "path"}},
        }
    }
    wfs = parse_workflows_from_dict(raw)
    assert "simple" in wfs
    assert wfs["simple"].run == "json.dumps"


def test_resolve_preset_merging() -> None:
    """Preset values override defaults and CLI overrides override presets."""
    yaml_src = """
workflows:
  pipeline:
    run: json.dumps
    defaults:
      batch: 10
      dest: /tmp/default
    presets:
      dev:
        batch: 20
        dest: /tmp/dev
"""
    wfs = load_workflows(yaml_src)
    inv = resolve_workflow_invocation(
        "pipeline/dev",
        cli_overrides={"batch": 50},
        workflows=wfs,
    )
    assert inv.values["batch"] == 50
    assert inv.values["dest"] == "/tmp/dev"


def test_subworkflow_inlining_and_deps() -> None:
    """Sub-workflows are inlined and dependency edges rewritten to leaf steps."""
    yaml_src = """
workflows:
  sub_wf:
    pipeline:
      - id: step_a
        run: json.dumps
      - id: step_b
        run: json.dumps
        after: [step_a]

  main_wf:
    pipeline:
      - id: run_sub
        run: sub_wf
      - id: final_step
        run: json.dumps
        after: [run_sub]
"""
    wfs = load_workflows(yaml_src)
    inv = resolve_workflow_invocation("main_wf", workflows=wfs)
    step_ids = [s.id for s in inv.workflow.steps]
    assert step_ids == ["run_sub.step_a", "run_sub.step_b", "final_step"]

    final_step = next(s for s in inv.workflow.steps if s.id == "final_step")
    assert final_step.wait_for == ["run_sub.step_b"]


def test_resolve_workflow_with_python_pydantic_model() -> None:
    """Workflow YAML directly referencing a Python Pydantic model for inputs."""
    yaml_src = """
workflows:
  hybrid_etl:
    inputs: tests.unit.test_contracts.PipelineConfig
    defaults:
      environment: dev
    run: json.dumps
"""
    wfs = load_workflows(yaml_src)
    inv = resolve_workflow_invocation("hybrid_etl", cli_overrides={"retries": "10"}, workflows=wfs)
    assert inv.values["environment"] == "dev"
    assert inv.values["retries"] == 10
    # Single step auto-wiring wires fields from the Python Pydantic model
    assert inv.workflow.steps[0].with_["environment"] == "${values.environment}"
    assert inv.workflow.steps[0].with_["retries"] == "${values.retries}"
