"""prefect-yaml: Declarative YAML DSL and orchestration runtime for Prefect."""

from __future__ import annotations

import os


def _ensure_local_no_proxy() -> None:
    current = os.environ.get("NO_PROXY", os.environ.get("no_proxy", ""))
    parts = [p.strip() for p in current.split(",") if p.strip()]
    for host in ("localhost", "127.0.0.1", "*********"):
        if host not in parts:
            parts.append(host)
    joined = ",".join(parts)
    os.environ["NO_PROXY"] = joined
    os.environ["no_proxy"] = joined


_ensure_local_no_proxy()

from prefect_yaml.cache.manifest import ManifestCache, default_manifest_path
from prefect_yaml.compiler.compiler import WorkflowCompiler
from prefect_yaml.contracts.validator import InputSpec
from prefect_yaml.dynamic_models import (
    DynamicBaseModel,
    ModelRegistry,
    build_model_from_spec,
    load_models_from_dict,
    load_models_from_yaml,
    resolve_type_hint,
)
from prefect_yaml.models.authoring import (
    ParamSpec,
    PipelineStep,
    ResolvedWorkflowInvocation,
    StepSpec,
    WorkflowDef,
    WorkflowSpec,
)
from prefect_yaml.models.compiled import (
    ArtifactSpec,
    CacheSpec,
    CompiledStep,
    CompiledWorkflow,
    ExecutionSpec,
    ForeachSpec,
    InvokeSpec,
    RouterRule,
    RouterSpec,
    StepKind,
    SwitchSpec,
)
from prefect_yaml.registry import WorkflowRegistry, registry, workflow
from prefect_yaml.resolver.resolver import (
    load_workflows,
    parse_cli_overrides,
    parse_workflows_from_dict,
    resolve_workflow_invocation,
)
from prefect_yaml.runtime.executor import execute_workflow
from prefect_yaml.runtime.flow_factory import PrefectFlowFactory, WorkflowExecutionError, flow_from_yaml
from prefect_yaml.runtime.step_factory import PrefectStepFactory
from prefect_yaml.server import PrefectServer

__version__ = "0.1.0"

__all__ = [
    "ArtifactSpec",
    "CacheSpec",
    "CompiledStep",
    "CompiledWorkflow",
    "DynamicBaseModel",
    "ExecutionSpec",
    "ForeachSpec",
    "InputSpec",
    "InvokeSpec",
    "ManifestCache",
    "ModelRegistry",
    "ParamSpec",
    "PipelineStep",
    "PrefectFlowFactory",
    "PrefectServer",
    "PrefectStepFactory",
    "ResolvedWorkflowInvocation",
    "RouterRule",
    "RouterSpec",
    "StepKind",
    "StepSpec",
    "SwitchSpec",
    "WorkflowCompiler",
    "WorkflowDef",
    "WorkflowExecutionError",
    "WorkflowRegistry",
    "WorkflowSpec",
    "build_model_from_spec",
    "default_manifest_path",
    "execute_workflow",
    "flow_from_yaml",
    "load_models_from_dict",
    "load_models_from_yaml",
    "load_workflows",
    "parse_cli_overrides",
    "parse_workflows_from_dict",
    "registry",
    "resolve_type_hint",
    "resolve_workflow_invocation",
    "workflow",
]
