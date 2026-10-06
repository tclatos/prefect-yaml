"""Authoring and intermediate representation models for workflows."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator

from prefect_yaml.contracts.validator import InputSpec
from prefect_yaml.models.compiled import (
    ArtifactSpec,
    CacheSpec,
    ExecutionSpec,
    ForeachSpec,
    InvokeSpec,
    RouterSpec,
    SwitchSpec,
)


class ParamSpec(InputSpec):
    """Declarative parameter specification with backwards compatibility."""


class PipelineStep(BaseModel):
    """Step definition within a workflow pipeline."""

    id: str
    run: str | None = None
    after: list[str] = Field(default_factory=list)
    wait_for: list[str] = Field(default_factory=list)
    with_: dict[str, Any] = Field(default_factory=dict, alias="with")
    cache: CacheSpec | str = Field(default_factory=CacheSpec)
    execution: ExecutionSpec = Field(default_factory=ExecutionSpec)
    artifacts: ArtifactSpec = Field(default_factory=ArtifactSpec)
    foreach: ForeachSpec | None = None
    inline: bool = False
    router: RouterSpec | None = None
    switch: SwitchSpec | None = None

    model_config = {"populate_by_name": True}

    @property
    def dependencies(self) -> list[str]:
        """All unique predecessor step IDs."""
        return list(dict.fromkeys(self.after + self.wait_for))

    def resolved_cache(self) -> CacheSpec:
        """Resolve shorthand cache strings to CacheSpec."""
        if isinstance(self.cache, str):
            return CacheSpec(backend=self.cache)  # type: ignore[arg-type]
        return self.cache


class WorkflowDef(BaseModel):
    """Authoring definition of a workflow from YAML."""

    name: str
    description: str = ""
    run: str | None = None
    pipeline: list[PipelineStep] = Field(default_factory=list)
    cache: CacheSpec | str = Field(default_factory=CacheSpec)
    defaults: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, Any] | str | type[BaseModel] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    presets: dict[str, dict[str, Any]] = Field(default_factory=dict)
    hidden: bool = False

    model_config = {"populate_by_name": True, "arbitrary_types_allowed": True}

    def model_post_init(self, context: Any, /) -> None:
        """Merge legacy params into inputs."""
        if self.params and not self.inputs:
            self.inputs = dict(self.params)

    @model_validator(mode="after")
    def _validate_run_or_pipeline(self) -> WorkflowDef:
        if self.run is None and not self.pipeline:
            raise ValueError(
                f"Workflow '{self.name}' must specify either 'run:' (single-step) or 'pipeline:' (multi-step)."
            )
        if self.run is not None and self.pipeline:
            raise ValueError(f"Workflow '{self.name}' cannot specify both 'run:' and 'pipeline:'.")
        return self

    def resolved_cache(self) -> CacheSpec:
        """Resolve shorthand cache strings to CacheSpec."""
        if isinstance(self.cache, str):
            return CacheSpec(backend=self.cache)  # type: ignore[arg-type]
        return self.cache

    def get_input_model(self) -> type[BaseModel]:
        """Return the compiled Pydantic input model for this workflow."""
        from prefect_yaml.dynamic_models.builder import build_model_from_spec

        return build_model_from_spec(f"{self.name}Inputs", self.inputs)


class StepSpec(BaseModel):
    """Normalized step specification ready for compilation."""

    id: str
    invoke: InvokeSpec = Field(default_factory=InvokeSpec)
    wait_for: list[str] = Field(default_factory=list)
    with_: dict[str, Any] = Field(default_factory=dict, alias="with")
    cache: CacheSpec = Field(default_factory=CacheSpec)
    execution: ExecutionSpec = Field(default_factory=ExecutionSpec)
    artifacts: ArtifactSpec = Field(default_factory=ArtifactSpec)
    foreach: ForeachSpec | None = None
    inline: bool = False
    router: RouterSpec | None = None
    switch: SwitchSpec | None = None

    model_config = {"populate_by_name": True}


class WorkflowSpec(BaseModel):
    """Intermediate representation of a workflow."""

    name: str
    description: str = ""
    defaults: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, Any] | str | type[BaseModel] = Field(default_factory=dict)
    steps: list[StepSpec] = Field(default_factory=list)

    model_config = {"populate_by_name": True, "arbitrary_types_allowed": True}

    def get_input_model(self) -> type[BaseModel]:
        """Return the compiled Pydantic input model for this workflow."""
        from prefect_yaml.dynamic_models.builder import build_model_from_spec

        return build_model_from_spec(f"{self.name}Inputs", self.inputs)


class ResolvedWorkflowInvocation(BaseModel):
    """Fully resolved workflow invocation context."""

    requested_name: str
    workflow_name: str
    workflow: WorkflowSpec
    profile_name: str | None = None
    values: dict[str, Any] = Field(default_factory=dict)
    step_overrides: dict[str, Any] = Field(default_factory=dict)
    cli_overrides: dict[str, Any] = Field(default_factory=dict)
    force: bool = False
