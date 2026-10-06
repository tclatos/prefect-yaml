"""Workflow resolver: loads workflow definitions, merges values, and validates contracts."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from omegaconf import DictConfig, OmegaConf
from omegaconf.errors import InterpolationKeyError

from prefect_yaml.contracts.validator import validate_workflow_inputs
from prefect_yaml.models.authoring import (
    ResolvedWorkflowInvocation,
    StepSpec,
    WorkflowDef,
    WorkflowSpec,
)
from prefect_yaml.models.compiled import ArtifactSpec, ExecutionSpec, InvokeSpec, StepKind
from prefect_yaml.registry import WorkflowRegistry
from prefect_yaml.registry import registry as default_registry
from prefect_yaml.resolver.expander import WorkflowResolutionError, _resolve_run_to_path, expand_pipeline

_LEGACY_KEYS = frozenset({"step_templates", "definitions", "profiles"})


def parse_cli_overrides(values: list[str] | None) -> dict[str, Any]:
    """Parse key=value string pairs into a nested dictionary.

    Args:
        values: List of string pairs formatted as KEY=VALUE.

    Returns:
        Dictionary of parsed overrides.
    """
    if not values:
        return {}

    overrides = OmegaConf.create({})
    for item in values:
        if "=" not in item:
            raise WorkflowResolutionError(f"Invalid override '{item}'. Expected KEY=VALUE format.")
        key, raw_value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise WorkflowResolutionError(f"Invalid override '{item}'. Missing key before '='.")
        try:
            parsed_holder = OmegaConf.create(f"value: {raw_value}")
            parsed_value = parsed_holder.get("value")
        except Exception:
            parsed_value = raw_value
        OmegaConf.update(overrides, key, parsed_value, merge=True)

    try:
        resolved = OmegaConf.to_container(overrides, resolve=True)
    except InterpolationKeyError as exc:
        raise WorkflowResolutionError(f"Configuration interpolation error in CLI overrides: {exc}") from exc
    if not isinstance(resolved, dict):
        raise WorkflowResolutionError("CLI overrides must resolve to a mapping.")
    return resolved


def _parse_name(name_or_preset: str) -> tuple[str, str | None]:
    if "/" in name_or_preset:
        parts = name_or_preset.split("/", 1)
        return parts[0].strip(), parts[1].strip()
    return name_or_preset.strip(), None


def _merge_values(
    defaults: dict[str, Any],
    preset: dict[str, Any],
    cli_overrides: dict[str, Any],
    config_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged = OmegaConf.create({})
    merged = OmegaConf.merge(merged, OmegaConf.create(defaults))
    merged = OmegaConf.merge(merged, OmegaConf.create(preset))
    merged = OmegaConf.merge(merged, OmegaConf.create(cli_overrides))

    if config_context:
        try:
            ctx = OmegaConf.create(config_context)
            combined = OmegaConf.merge(ctx, OmegaConf.create({"_merged_values_": merged}))
            resolved_section = OmegaConf.select(combined, "_merged_values_")
            container = OmegaConf.to_container(resolved_section, resolve=True)
            if isinstance(container, dict):
                return container
        except Exception:
            pass

    try:
        container = OmegaConf.to_container(merged, resolve=True)
        return container if isinstance(container, dict) else {}
    except InterpolationKeyError as exc:
        missing_key = re.search(r"Interpolation key '([^']+)' not found", str(exc))
        hint = missing_key.group(1) if missing_key else str(exc)
        raise WorkflowResolutionError(f"Configuration interpolation error: key '{hint}' not found.") from exc


def parse_workflows_from_dict(raw: dict[str, Any]) -> dict[str, WorkflowDef]:
    """Parse a dictionary of raw definitions into WorkflowDef objects.

    Args:
        raw: Dictionary mapping workflow names to definitions.

    Returns:
        Mapping of workflow names to validated WorkflowDef objects.
    """
    workflows: dict[str, WorkflowDef] = {}
    for name, data in raw.items():
        if name.startswith("_") or name in _LEGACY_KEYS:
            continue
        if not isinstance(data, dict):
            continue
        if "run" not in data and "pipeline" not in data:
            continue
        try:
            workflows[name] = WorkflowDef.model_validate({"name": name, **data})
        except Exception as exc:
            raise WorkflowResolutionError(f"Invalid workflow definition '{name}': {exc}") from exc
    return workflows


def load_workflows(
    source: Path | str | dict[str, Any] | DictConfig | None = None,
    reg: WorkflowRegistry | None = None,
) -> dict[str, WorkflowDef]:
    """Load workflow definitions from a file, directory, or dictionary.

    Args:
        source: Configuration path, YAML string, or raw dictionary.
        reg: Workflow registry for registered callables.

    Returns:
        Mapping of workflow name to WorkflowDef.
    """
    raw_dict: dict[str, Any] = {}
    active_reg = reg or default_registry

    if source is not None:
        if isinstance(source, Path):
            if source.is_dir():
                import yaml

                for yaml_file in sorted(source.glob("**/*.yaml")):
                    try:
                        content = yaml.safe_load(yaml_file.read_text(encoding="utf-8")) or {}
                        raw_dict.update(content.get("workflows", content) if isinstance(content, dict) else {})
                    except Exception:
                        pass
                for yml_file in sorted(source.glob("**/*.yml")):
                    try:
                        content = yaml.safe_load(yml_file.read_text(encoding="utf-8")) or {}
                        raw_dict.update(content.get("workflows", content) if isinstance(content, dict) else {})
                    except Exception:
                        pass
            elif source.is_file():
                import yaml

                content = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
                raw_dict = content.get("workflows", content) if isinstance(content, dict) else {}
        elif isinstance(source, str):
            import yaml

            content = yaml.safe_load(source) or {}
            raw_dict = content.get("workflows", content) if isinstance(content, dict) else {}
        elif isinstance(source, DictConfig):
            container = OmegaConf.to_container(source, resolve=False)
            raw_dict = container.get("workflows", container) if isinstance(container, dict) else {}
        elif isinstance(source, dict):
            raw_dict = source.get("workflows", source)

    workflows = parse_workflows_from_dict(raw_dict)

    for entry in active_reg.list_all():
        if entry.name not in workflows:
            sig_params = entry.get_params_schema()
            workflows[entry.name] = WorkflowDef(
                name=entry.name,
                description=entry.description,
                run=entry.dotted_path,
                hidden=entry.hidden,
                inputs={p: {"required": info.get("required", False)} for p, info in sig_params.items()},
            )

    return workflows


def _to_workflow_spec(
    wf: WorkflowDef,
    all_workflows: dict[str, WorkflowDef],
    reg: WorkflowRegistry,
    *,
    extra_values: dict[str, Any] | None = None,
) -> WorkflowSpec:
    if wf.run:
        target = _resolve_run_to_path(wf.run, all_workflows, reg)
        auto_with = {k: f"${{values.{k}}}" for k in wf.defaults}
        for param_name in wf.inputs:
            if param_name not in auto_with:
                auto_with[param_name] = f"${{values.{param_name}}}"
        if extra_values:
            for k in ("force", "dry_run"):
                if k in extra_values and k not in auto_with:
                    auto_with[k] = f"${{values.{k}}}"

        steps = [
            StepSpec(
                id="run",
                invoke=InvokeSpec(kind=StepKind.callable, target=target),
                wait_for=[],
                with_=auto_with,
                cache=wf.resolved_cache(),
                execution=ExecutionSpec(),
                artifacts=ArtifactSpec(),
            )
        ]
    else:
        steps = expand_pipeline(
            wf.pipeline,
            all_workflows,
            reg,
            _ancestors=frozenset({wf.name}),
            extra_values=extra_values,
        )

    return WorkflowSpec(
        name=wf.name,
        description=wf.description,
        defaults=wf.defaults,
        inputs=wf.inputs,
        steps=steps,
    )


def resolve_workflow_invocation(
    name_or_preset: str,
    *,
    cli_overrides: dict[str, Any] | None = None,
    workflows: dict[str, WorkflowDef] | None = None,
    source: Path | str | dict[str, Any] | None = None,
    config_context: dict[str, Any] | None = None,
    force: bool = False,
    reg: WorkflowRegistry | None = None,
) -> ResolvedWorkflowInvocation:
    """Resolve workflow name or preset into an invocation with validated values.

    Args:
        name_or_preset: Workflow identifier, optionally with preset format 'name/preset'.
        cli_overrides: Explicit key-value overrides.
        workflows: Optional preloaded workflow definitions.
        source: Source to load workflows from when not provided.
        config_context: Optional context for interpolation.
        force: Whether force execution flag is set.
        reg: Registry for Python workflows.

    Returns:
        The fully resolved workflow invocation context.
    """
    workflow_name, preset_name = _parse_name(name_or_preset)
    all_workflows = workflows if workflows is not None else load_workflows(source, reg=reg)
    active_reg = reg or default_registry

    if workflow_name not in all_workflows:
        available = ", ".join(sorted(all_workflows)) or "<none>"
        raise WorkflowResolutionError(f"Workflow '{workflow_name}' not found. Available: {available}")

    wf = all_workflows[workflow_name]

    if preset_name is not None and preset_name not in wf.presets:
        available_presets = ", ".join(sorted(wf.presets)) or "<none>"
        raise WorkflowResolutionError(
            f"Preset '{preset_name}' not found in workflow '{workflow_name}'. Available: {available_presets}"
        )

    preset_values = wf.presets.get(preset_name, {}) if preset_name else {}
    cli_values = cli_overrides or {}
    raw_values = _merge_values(wf.defaults, preset_values, cli_values, config_context=config_context)

    if force:
        raw_values["force"] = True

    from prefect_yaml.contracts.validator import ContractValidationError

    try:
        validated_values = validate_workflow_inputs(wf.inputs, raw_values)
    except ContractValidationError as exc:
        raise WorkflowResolutionError(
            f"Workflow '{name_or_preset}' is missing required parameter(s): {exc}."
        ) from exc

    workflow_spec = _to_workflow_spec(wf, all_workflows, active_reg, extra_values=validated_values)

    return ResolvedWorkflowInvocation(
        requested_name=name_or_preset,
        workflow_name=workflow_name,
        workflow=workflow_spec,
        profile_name=preset_name,
        values=validated_values,
        cli_overrides=cli_values,
        force=force,
    )
