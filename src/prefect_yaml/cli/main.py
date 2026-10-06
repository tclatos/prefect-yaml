"""Command line interface for prefect-yaml."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from prefect_yaml.cache.fingerprint import compute_step_fingerprint
from prefect_yaml.cache.manifest import ManifestCache, default_manifest_path
from prefect_yaml.compiler.compiler import WorkflowCompiler
from prefect_yaml.models.authoring import ResolvedWorkflowInvocation
from prefect_yaml.resolver.resolver import (
    load_workflows,
    parse_cli_overrides,
    resolve_workflow_invocation,
)
from prefect_yaml.runtime.executor import execute_workflow
from prefect_yaml.runtime.flow_factory import PrefectFlowFactory, _prepare_inputs
from prefect_yaml.server import PrefectServer

app = typer.Typer(
    name="prefect-yaml",
    help="Declarative YAML DSL and orchestration runtime for Prefect.",
    no_args_is_help=True,
)
server_app = typer.Typer(name="server", help="Local Prefect server management.", no_args_is_help=True)
app.add_typer(server_app)


def _render_workflow_summary(invocation: ResolvedWorkflowInvocation) -> None:
    console = Console()
    summary = Table(title="Workflow Resolution", show_header=True, header_style="bold cyan")
    summary.add_column("Property", style="cyan", no_wrap=True)
    summary.add_column("Value", style="white")
    summary.add_row("Requested", invocation.requested_name)
    summary.add_row("Workflow", invocation.workflow_name)
    summary.add_row("Preset", invocation.profile_name or "<none>")
    summary.add_row("Force", str(invocation.force))
    summary.add_row("Steps", str(len(invocation.workflow.steps)))
    console.print(summary)

    if invocation.values:
        console.print(Panel(json.dumps(invocation.values, indent=2, default=str), title="Effective Values"))

    steps = Table(title="Workflow Steps", show_header=True, header_style="bold green")
    steps.add_column("Id", style="cyan", no_wrap=True)
    steps.add_column("Invoke / Strategy", style="white")
    steps.add_column("Wait For", style="magenta")
    for step in invocation.workflow.steps:
        if step.router:
            target = f"router: {step.router.items}"
        elif step.switch:
            target = f"switch: {step.switch.value}"
        else:
            target = step.invoke.target if step.invoke else "-"
        wait_for = ", ".join(step.wait_for) if step.wait_for else "-"
        steps.add_row(step.id, target, wait_for)
    console.print(steps)


def _render_cache_status(invocation: ResolvedWorkflowInvocation) -> None:
    console = Console()
    try:
        compiled = WorkflowCompiler().compile(invocation.workflow, invocation.values)
    except Exception as exc:
        console.print(f"[yellow]Cache status unavailable (compilation error): {exc}[/yellow]")
        return

    manifest_path = default_manifest_path(compiled.name)
    manifest = ManifestCache.load(manifest_path)
    force = bool(invocation.values.get("force"))

    table = Table(title="Cache Status", show_header=True, header_style="bold blue")
    table.add_column("Step", style="cyan", no_wrap=True)
    table.add_column("Backend", style="dim")
    table.add_column("Status", no_wrap=True)
    table.add_column("Fingerprint", style="dim", no_wrap=True)
    table.add_column("Last Run", style="dim")

    for step in compiled.steps:
        step_inputs = _prepare_inputs(step.with_, {})
        fp = compute_step_fingerprint(step.id, step_inputs)
        backend = step.cache.backend

        if backend == "none":
            status = "[dim]no cache[/dim]"
            last_run = "-"
            fp_display = "-"
        elif force:
            status = "[yellow]FORCED[/yellow]"
            record = manifest.records.get(step.id)
            last_run = record.processed_at.strftime("%Y-%m-%d %H:%M") if record else "never"
            fp_display = fp[:8]
        elif manifest.is_fresh(step.id, fingerprint=fp):
            record = manifest.records.get(step.id)
            status = "[green]FRESH[/green]"
            last_run = record.processed_at.strftime("%Y-%m-%d %H:%M") if record else "-"
            fp_display = fp[:8]
        else:
            record = manifest.records.get(step.id)
            status = "[red]STALE[/red]" if record else "[yellow]UNKNOWN[/yellow]"
            last_run = record.processed_at.strftime("%Y-%m-%d %H:%M") if record else "never"
            fp_display = fp[:8]

        table.add_row(step.id, backend, status, fp_display, last_run)

    console.print(table)


@app.command("run")
def run(
    workflow_name: Annotated[str, typer.Argument(help="Workflow name or 'workflow/preset'")],
    set_values: Annotated[list[str] | None, typer.Option("--set", help="Override values using KEY=VALUE")] = None,
    source: Annotated[Path | None, typer.Option("--source", "-s", help="Path to YAML file or directory")] = None,
    force: Annotated[bool, typer.Option("--force", help="Bypass caches")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Show plan only without executing")] = False,
) -> None:
    """Execute a workflow by name or preset."""
    console = Console()
    try:
        cli_overrides = parse_cli_overrides(set_values)
        lookup_source = source if source is not None else Path.cwd()
        invocation = resolve_workflow_invocation(
            workflow_name,
            cli_overrides=cli_overrides,
            source=lookup_source,
            force=force,
        )
    except Exception as exc:
        console.print(Panel(str(exc), title="Resolution Error", border_style="red"))
        raise typer.Exit(1) from exc

    if dry_run:
        _render_workflow_summary(invocation)
        _render_cache_status(invocation)
        return

    console.print(f"[bold green]Starting workflow '{invocation.workflow_name}'...[/bold green]")
    try:
        results = execute_workflow(invocation)
        console.print(Panel("[bold green]Workflow completed successfully![/bold green]"))
        console.print(Panel(json.dumps(results, indent=2, default=str), title="Results"))
    except Exception as exc:
        console.print(Panel(str(exc), title="Execution Error", border_style="red"))
        raise typer.Exit(1) from exc


@app.command("list")
def list_workflows(
    source: Annotated[Path | None, typer.Option("--source", "-s", help="Path to YAML file or directory")] = None,
) -> None:
    """List all available workflows and presets."""
    console = Console()
    lookup_source = source if source is not None else Path.cwd()
    workflows = load_workflows(lookup_source)

    if not workflows:
        console.print("[dim]No workflows found.[/dim]")
        return

    table = Table(title="Available Workflows", show_header=True, header_style="bold cyan")
    table.add_column("Workflow", style="cyan", no_wrap=True)
    table.add_column("Steps", style="magenta")
    table.add_column("Presets", style="yellow")
    table.add_column("Description", style="white")

    for name in sorted(workflows):
        wf = workflows[name]
        if wf.hidden:
            continue
        step_count = "1 (run)" if wf.run else str(len(wf.pipeline))
        preset_names = ", ".join(sorted(wf.presets.keys())) if wf.presets else "-"
        table.add_row(name, step_count, preset_names, wf.description)

    console.print(table)


@app.command("show")
def show(
    workflow_name: Annotated[str, typer.Argument(help="Name of the workflow to inspect")],
    source: Annotated[Path | None, typer.Option("--source", "-s", help="Path to YAML file or directory")] = None,
) -> None:
    """Inspect a workflow's typed input contracts, presets, and step DAG."""
    console = Console()
    lookup_source = source if source is not None else Path.cwd()
    workflows = load_workflows(lookup_source)

    if workflow_name not in workflows:
        console.print(f"[red]Workflow '{workflow_name}' not found.[/red]")
        raise typer.Exit(1)

    wf = workflows[workflow_name]
    console.print(Panel(f"[bold cyan]{wf.name}[/bold cyan]\n{wf.description}", title="Workflow Overview"))

    if wf.inputs:
        inputs_table = Table(title="Input Contracts", show_header=True, header_style="bold green")
        inputs_table.add_column("Parameter", style="cyan", no_wrap=True)
        inputs_table.add_column("Type", style="yellow")
        inputs_table.add_column("Required", style="magenta")
        inputs_table.add_column("Default", style="white")
        inputs_table.add_column("Choices / Constraints", style="dim")
        inputs_table.add_column("Description", style="white")

        input_model = wf.get_input_model()
        for fname, field_info in input_model.model_fields.items():
            ann = field_info.annotation
            ptype = getattr(ann, "__name__", str(ann)) if ann is not None else "any"
            preq = str(field_info.is_required())
            pdef = str(field_info.default) if not field_info.is_required() and field_info.default is not None else "-"

            constraints_list: list[str] = []
            for meta in field_info.metadata:
                for attr in ("ge", "gt", "le", "lt", "pattern"):
                    val = getattr(meta, attr, None)
                    if val is not None:
                        constraints_list.append(f"{attr}={val}")
            constraints = ", ".join(constraints_list) if constraints_list else "-"
            pdesc = field_info.description or ""
            inputs_table.add_row(fname, ptype, preq, pdef, constraints, pdesc)
        console.print(inputs_table)

    if wf.presets:
        presets_table = Table(title="Presets", show_header=True, header_style="bold yellow")
        presets_table.add_column("Preset", style="yellow", no_wrap=True)
        presets_table.add_column("Values", style="white")
        for pname, pvals in wf.presets.items():
            presets_table.add_row(pname, json.dumps(pvals, default=str))
        console.print(presets_table)


@app.command("validate")
def validate(
    source: Annotated[Path | None, typer.Option("--source", "-s", help="Path to YAML file or directory")] = None,
) -> None:
    """Validate all workflow definitions in the specified source."""
    console = Console()
    lookup_source = source if source is not None else Path.cwd()
    try:
        workflows = load_workflows(lookup_source)
        for name in workflows:
            resolve_workflow_invocation(name, workflows=workflows)
        console.print(f"[bold green]All {len(workflows)} workflow definitions validated successfully.[/bold green]")
    except Exception as exc:
        console.print(Panel(str(exc), title="Validation Failed", border_style="red"))
        raise typer.Exit(1) from exc


@app.command("serve")
def serve(
    workflow_name: Annotated[str, typer.Argument(help="Workflow name or 'workflow/preset'")],
    name: Annotated[str | None, typer.Option("--name", help="Deployment name")] = None,
    cron: Annotated[str | None, typer.Option("--cron", help="Cron schedule")] = None,
    interval: Annotated[float | None, typer.Option("--interval", help="Interval schedule in seconds")] = None,
    source: Annotated[Path | None, typer.Option("--source", "-s", help="Path to YAML file or directory")] = None,
) -> None:
    """Serve a workflow as a persistent Prefect deployment."""
    lookup_source = source if source is not None else Path.cwd()
    factory = PrefectFlowFactory.from_profile(workflow_name, source=lookup_source)
    kwargs: dict[str, Any] = {}
    if cron:
        kwargs["cron"] = cron
    if interval:
        kwargs["interval"] = interval
    factory.serve(name=name, **kwargs)


@server_app.command("start")
def server_start(
    foreground: Annotated[bool, typer.Option("--foreground", "-f", help="Run in foreground")] = False,
) -> None:
    """Start local Prefect server."""
    console = Console()
    srv = PrefectServer()
    if srv.is_running():
        console.print(f"[green]Prefect server is already running at {srv.ui_url}[/green]")
        return
    srv.start(foreground=foreground)
    if not foreground:
        if srv.is_running():
            console.print(f"[bold green]Prefect server started at {srv.ui_url}[/bold green]")
        else:
            console.print("[yellow]Server started but health check pending. Verify UI shortly.[/yellow]")


@server_app.command("stop")
def server_stop() -> None:
    """Stop local Prefect server."""
    console = Console()
    srv = PrefectServer()
    srv.stop()
    console.print("[yellow]Prefect server stopped.[/yellow]")


@server_app.command("status")
def server_status() -> None:
    """Check running status of local Prefect server."""
    console = Console()
    srv = PrefectServer()
    if srv.is_running():
        console.print(f"[green]Running[/green] at [cyan]{srv.ui_url}[/cyan] (API: {srv.resolved_api_url})")
    else:
        console.print("[red]Not running[/red]")


@server_app.command("ui")
def server_ui() -> None:
    """Open Prefect UI in default browser."""
    srv = PrefectServer()
    srv.open_ui()
