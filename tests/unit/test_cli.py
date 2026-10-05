"""Unit tests for prefect-yaml CLI."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from prefect_yaml.cli.main import app

runner = CliRunner()


def test_cli_list_empty(tmp_path: Path) -> None:
    """Test listing workflows in an empty directory."""
    res = runner.invoke(app, ["list", "--source", str(tmp_path)])
    assert res.exit_code == 0
    assert "No workflows found" in res.output


def test_cli_show_and_dry_run(tmp_path: Path) -> None:
    """Test show command and dry run execution."""
    wf_file = tmp_path / "workflows.yaml"
    wf_file.write_text(
        """
workflows:
  demo:
    description: "Sample demo workflow"
    inputs:
      title:
        type: str
        default: "hello"
    run: json.dumps
""",
        encoding="utf-8",
    )

    # Show command
    show_res = runner.invoke(app, ["show", "demo", "--source", str(tmp_path)])
    assert show_res.exit_code == 0
    assert "Input Contracts" in show_res.output
    assert "title" in show_res.output

    # Dry-run command
    run_res = runner.invoke(app, ["run", "demo", "--source", str(tmp_path), "--dry-run"])
    assert run_res.exit_code == 0
    assert "Workflow Resolution" in run_res.output
    assert "Cache Status" in run_res.output
