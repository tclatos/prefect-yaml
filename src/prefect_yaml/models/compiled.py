"""Normalized runtime models for compiled workflow graphs."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class StepKind(str, Enum):
    """Execution strategy for a compiled step."""

    task = "task"
    flow = "flow"
    workflow = "workflow"
    deployment = "deployment"
    factory = "factory"
    callable = "callable"
    router = "router"
    switch = "switch"


class InvokeSpec(BaseModel):
    """Specification of an invocation target."""

    kind: StepKind = StepKind.callable
    target: str = ""


class ForeachSpec(BaseModel):
    """Fan-out loop specification."""

    from_ref: str = Field(alias="from")
    as_var: str = Field(default="item", alias="as")
    concurrency_limit: int | None = None

    model_config = {"populate_by_name": True}


class ExecutionSpec(BaseModel):
    """Runtime execution policy for a step."""

    retries: int = 0
    retry_delay_seconds: float = 0.0
    tags: list[str] = Field(default_factory=list)
    work_pool: str | None = None
    work_queue: str | None = None
    concurrency_limit: int | None = None
    on_failure: Literal["abort", "skip", "continue"] = "abort"


class CacheSpec(BaseModel):
    """Cache reuse policy for a step."""

    backend: Literal["none", "prefect_result", "manifest", "hybrid"] = "none"
    key_include: list[str] = Field(default_factory=list)


class ArtifactSpec(BaseModel):
    """Artifact publication policy for a step."""

    publish_result: bool = False


class RouterRule(BaseModel):
    """Routing rule matching item patterns to target workflows."""

    match: str
    run: str
    with_: dict[str, Any] = Field(default_factory=dict, alias="with")

    model_config = {"populate_by_name": True}


class RouterSpec(BaseModel):
    """Specification for partitioned fan-out routing."""

    items: str | list[Any]
    rules: list[RouterRule] = Field(default_factory=list)
    default: str | RouterRule | None = None
    as_var: str = Field(default="items", alias="as")
    concurrency_limit: int | None = None

    model_config = {"populate_by_name": True}


class SwitchSpec(BaseModel):
    """Specification for conditional branch execution."""

    value: str
    cases: dict[str, str] = Field(default_factory=dict)
    default: str | None = None


class CompiledStep(BaseModel):
    """Normalized, fully-resolved step ready for execution."""

    id: str
    invoke: InvokeSpec = Field(default_factory=InvokeSpec)
    wait_for: list[str] = Field(default_factory=list)
    with_: dict[str, Any] = Field(default_factory=dict, alias="with")
    execution: ExecutionSpec = Field(default_factory=ExecutionSpec)
    cache: CacheSpec = Field(default_factory=CacheSpec)
    artifacts: ArtifactSpec = Field(default_factory=ArtifactSpec)
    foreach: ForeachSpec | None = None
    inline: bool = False
    router: RouterSpec | None = None
    switch: SwitchSpec | None = None

    model_config = {"populate_by_name": True}


class CompiledWorkflow(BaseModel):
    """Normalized workflow graph ready for execution."""

    name: str
    description: str = ""
    steps: list[CompiledStep] = Field(default_factory=list)
    values: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, Any] = Field(default_factory=dict)
