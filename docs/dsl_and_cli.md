# `prefect-yaml` User Guide: Workflow DSL & CLI Reference

## 1. Overview

`prefect-yaml` lets you author, validate, and execute complex Prefect workflows using declarative YAML files. It eliminates Python orchestration boilerplate while giving you:
- Strongly typed input contracts with automatic CLI type coercion.
- Dependency chaining (`after:`), parallel branch execution, and `foreach` fan-out.
- xxHash3 manifest caching to skip re-processing unchanged data.
- Flexible partitioning and conditional branching.
- Built-in CLI for inspecting schemas, running dry-runs, and managing deployments.

---

## 2. YAML DSL Reference

A workflow definition lives under the top-level `workflows:` key in any YAML file.

### 2.1 Single-Step Workflow (`run:`)

Use single-step workflows when you want to wrap a single function or sub-workflow with parameters and presets:

```yaml
workflows:
  export_summary:
    description: "Export summary metrics to JSON or CSV"
    inputs:
      data_file:
        type: path
        required: true
        description: "Path to raw input file"
      format:
        type: enum
        choices: [json, csv]
        default: json
    defaults:
      data_file: "/data/raw.csv"
      format: json
    presets:
      csv_export:
        format: csv
    run: my_package.exporters.export_summary
```

- When using single-step shorthand, all declared `defaults` and `inputs` are automatically forwarded as keyword arguments to the function.

### 2.2 Multi-Step Pipeline (`pipeline:`)

For multi-step DAG workflows, declare steps in a `pipeline:` list:

```yaml
workflows:
  data_etl:
    description: "Extract, transform, and load pipeline"
    inputs:
      source_dir:
        type: path
        required: true
      threshold:
        type: float
        default: 0.8
        minimum: 0.0
        maximum: 1.0
      workers:
        type: int
        default: 4
        minimum: 1
    defaults:
      source_dir: "/data/landing"
    pipeline:
      - id: extract
        run: my_app.tasks.extract_data
        with:
          source_dir: "${values.source_dir}"

      - id: transform
        run: my_app.tasks.clean_and_score
        after: [extract]
        with:
          records: "${steps.extract.result.records}"
          threshold: "${values.threshold}"
        cache: manifest

      - id: load
        run: my_app.tasks.load_db
        after: [transform]
        with:
          scored_records: "${steps.transform.result.cleaned}"
```

### 2.3 Step Fields

| Field | Purpose |
|---|---|
| `id` | Unique identifier for the step in the pipeline. |
| `run` | Dotted Python path (e.g. `myapp.step`) or name of another workflow or `@workflow`-decorated function. |
| `after` / `wait_for` | List of predecessor step IDs this step depends on. Independent steps run in parallel. |
| `with:` | Arguments passed to the callable. Supports `${values.KEY}` and `${steps.<id>.result.<attr>}` interpolation. |
| `cache` | Cache reuse policy: `none` (default), `manifest`, `hybrid`, `prefect_result`. |
| `execution` | Policy block: `retries`, `retry_delay_seconds`, `tags`, `on_failure` (`abort`, `skip`, `continue`). |
| `foreach` | Fan-out mapping over an iterable result. |
| `inline` | If `true`, suppresses Prefect subflow creation and runs tasks under the parent flow context. |
| `router` | Partition collections across parallel workflows. |
| `switch` | Conditional branch execution based on dynamic expressions. |

---

## 3. Typed Input Contracts

Declare inputs under `inputs:` to define schema contracts and constraints:

```yaml
inputs:
  count:
    type: int
    default: 10
    minimum: 1
    maximum: 100
  dataset:
    type: path
    required: true
  environment:
    type: enum
    choices: [dev, staging, prod]
    default: dev
  tags:
    type: list
    items_type: str
    default: ["daily", "core"]
  version:
    type: str
    regex: "^v[0-9]+(\\.[0-9]+)?$"
    default: "v1.0"
```

### Supported Types
- `string` / `str`
- `int` / `integer`
- `float` / `number`
- `bool` / `boolean` (`"true"`, `"false"`, `"1"`, `"0"`, `"yes"`, `"no"`)
- `path` (coerced to `pathlib.Path`)
- `enum` (validated against `choices`)
- `list` (supports `items_type` coercion)
- `dict`
- `any`

### Parameter Precedence
Values are resolved in strict priority:
```
Workflow defaults  →  Named preset values  →  CLI --set overrides
```

---

## 4. Sub-Workflows, Fan-Out & Routing

### 4.1 Sub-Workflow Inlining
A pipeline step can reference another workflow by name:
```yaml
pipeline:
  - id: sub_job
    run: child_workflow
    with:
      custom_param: 42
  - id: next_step
    run: my_pkg.finish
    after: [sub_job]  # Automatically connects to the leaf steps of child_workflow
```

### 4.2 Fan-Out (`foreach:`)
Execute a step once per item in a list concurrently:
```yaml
pipeline:
  - id: get_files
    run: my_app.find_files
  - id: process_file
    run: my_app.process_single
    after: [get_files]
    foreach:
      from: "${steps.get_files.result.files}"
      as: file_path
    with:
      target: "${file_path}"
```

### 4.3 Partition Router
Classify a collection of items and run matching workflows in parallel:
```yaml
pipeline:
  - id: partition_sources
    router:
      items: "${values.input_items}"
      rules:
        - match: "**/*.pdf"
          run: pdf_converter_flow
          with:
            files: "${items}"
        - match: "https://**"
          run: web_fetcher_flow
          with:
            urls: "${items}"
      default:
        run: generic_processor
        with:
          items: "${items}"
```

### 4.4 Switch Branching
Execute specific workflows conditionally:
```yaml
pipeline:
  - id: check_status
    run: my_app.check_system
  - id: branch_step
    switch: "${steps.check_status.result.status}"
    cases:
      green: run_fast_pipeline
      amber: run_diagnostic_pipeline
    default: run_error_handler
```

---

## 5. CLI Reference

```bash
# General help
prefect-yaml --help
```

### 5.1 `prefect-yaml list`
Lists all available workflows, presets, step counts, and descriptions.

```bash
uv run prefect-yaml list
uv run prefect-yaml list --source ./my_config
```

### 5.2 `prefect-yaml show <workflow>`
Inspects a workflow's schema contracts, parameter types, choices, defaults, presets, and step dependency DAG.

```bash
uv run prefect-yaml show data_etl
```

### 5.3 `prefect-yaml run <workflow>[/<preset>]`
Executes a workflow.

```bash
# Execute using default values
uv run prefect-yaml run data_etl

# Execute a named preset
uv run prefect-yaml run data_etl/production

# Override parameters on CLI (automatically coerced to typed values)
uv run prefect-yaml run data_etl --set batch_size=25 --set environment=staging

# Dry-run: resolve values, display DAG and cache status without executing
uv run prefect-yaml run data_etl/production --dry-run

# Force re-execution bypassing cache manifests
uv run prefect-yaml run data_etl --force
```

### 5.4 `prefect-yaml validate`
Validates all workflow YAML files in the given directory: checks schema syntax, input contracts, imports, and cyclic dependencies.

```bash
uv run prefect-yaml validate --source ./config
```

### 5.5 `prefect-yaml serve <workflow>`
Registers and serves a long-running Prefect deployment with optional schedules:

```bash
# Serve with a cron schedule
uv run prefect-yaml serve data_etl --cron "0 2 * * *"

# Serve with an interval schedule in seconds
uv run prefect-yaml serve data_etl --interval 3600
```

### 5.6 `prefect-yaml server`
Control the local background Prefect server daemon:

```bash
uv run prefect-yaml server start       # Start detached daemon
uv run prefect-yaml server start -f    # Start in foreground
uv run prefect-yaml server status      # Check health & URLs
uv run prefect-yaml server ui          # Open UI in browser
uv run prefect-yaml server stop        # Stop background daemon
```
