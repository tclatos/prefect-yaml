# `prefect-yaml` Developer & Maintainer Guide: Design and API Architecture

## 1. System Vision & Architecture

`prefect-yaml` is a standalone, Python-first orchestration library that compiles declarative YAML pipelines into Prefect 3 `@flow` and `@task` DAGs.

Unlike monolithic orchestration platforms or framework-specific plugins, `prefect-yaml`:
- Has **zero dependencies on AI frameworks or specific application stacks**.
- Relies on **Pydantic v2** for strongly typed schemas and contracts.
- Uses **OmegaConf** for parameter merging, defaults, presets, and `${values.*}` interpolation.
- Employs **xxHash3 (64-bit)** for high-performance deterministic step fingerprinting and manifest-based incremental execution.
- Maps cleanly onto Prefect 3 primitives (`FlowRunContext`, `ThreadPoolTaskRunner`, `Task`, `Flow`).

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           YAML Definition / Dict                        │
│   (inputs: ..., defaults: ..., presets: ..., pipeline: ... / run: ...) │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│               1. Resolver (prefect_yaml.resolver)                       │
│  - Merges defaults → presets → CLI --set overrides via OmegaConf        │
│  - Coerces & validates inputs against InputSpec (prefect_yaml.contracts)│
│  - Inlines sub-workflows recursively & rewrites terminal dependencies   │
│  - Outputs: ResolvedWorkflowInvocation                                  │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│               2. Compiler (prefect_yaml.compiler)                       │
│  - Validates uniqueness of step IDs                                     │
│  - Validates wait_for references & detects DAG cycles (Kahn's algorithm)│
│  - Interpolates ${values.*} placeholders into step 'with' mappings      │
│  - Auto-detects callable vs Flow vs Task vs Router vs Switch            │
│  - Outputs: CompiledWorkflow & CompiledStep                             │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│               3. Runtime (prefect_yaml.runtime)                         │
│  - Preflight checks step kwargs against Python callable signatures      │
│  - PrefectStepFactory builds @task callables (supports inline mode)     │
│  - PrefectFlowFactory builds dynamic @flow with ThreadPoolTaskRunner   │
│  - Checks xxHash3 ManifestCache: skips fresh steps, records successes  │
│  - Handles foreach fan-out, partition router, and switch branching      │
│  - Executes flow or serves deployment listener                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Core Subsystems

### 2.1 Contracts & Input Validation (`prefect_yaml.contracts`)

Located in `prefect_yaml.contracts`, this subsystem provides typed contracts for workflow inputs, closing the gap with standalone workflow platforms like Kestra.

- **`InputSpec`**: Pydantic model defining parameter constraints:
  - `type`: Target data type (`string`, `int`, `float`, `bool`, `path`, `enum`, `list`, `dict`, `any`).
  - `required`: Boolean indicating mandatory input without default fallback.
  - `default`: Fallback value.
  - `choices`: Allowed values for enum/choice validation.
  - `minimum` / `maximum`: Numerical range bounds.
  - `regex`: Regular expression constraint for strings and paths.
  - `items_type`: Inner type constraint for list elements.
- **Type Coercion (`validate_input_value`, `validate_workflow_inputs`)**:
  - Automatically parses strings from CLI arguments or environment overrides into Python primitives (`"10"` → `10`, `"true"` → `True`, `"/tmp/dir"` → `Path("/tmp/dir")`, `"[1,2]"` → `[1, 2]`).
  - Validates constraints before flow compilation, raising `ContractValidationError` with clear hints.

### 2.2 Models & Intermediate Representation (`prefect_yaml.models`)

- **Authoring Models (`models.authoring`)**:
  - `WorkflowDef`: Root authoring model parsing YAML workflows. Enforces mutually exclusive `run:` (single-step) or `pipeline:` (multi-step DAG).
  - `PipelineStep`: Pipeline step with `id`, `run`, `after` / `wait_for`, `with`, `cache`, `execution`, `foreach`, `inline`, `router`, and `switch`.
  - `WorkflowSpec`: Intermediate representation with normalized steps ready for compilation.
  - `ResolvedWorkflowInvocation`: Full context holding workflow name, profile, effective values, CLI overrides, and force flag.
- **Compiled Models (`models.compiled`)**:
  - `StepKind`: Strategy enum (`callable`, `flow`, `task`, `deployment`, `factory`, `router`, `switch`).
  - `InvokeSpec`: Target dotted path and execution kind.
  - `CompiledStep` and `CompiledWorkflow`: Flat, fully-resolved execution graph passed to the runtime.

### 2.3 Compiler & DAG Algorithms (`prefect_yaml.compiler`)

- **`WorkflowCompiler`**:
  - Runs DAG validation: `validate_step_ids`, `validate_wait_for_refs`, and `validate_dag`.
  - Auto-detects whether the target object is a Prefect `Flow`, Prefect `Task`, or plain Python callable using module introspection.
  - Resolves `${values.KEY}` placeholders inside step arguments against the effective values dictionary.
- **`dag.py`**:
  - Implements Kahn's algorithm for cycle detection and topological sorting (`topological_sort`). Ensures all upstream tasks execute before dependents.

### 2.4 Resolver & Value Merging (`prefect_yaml.resolver`)

- **`resolve_workflow_invocation`**:
  1. Splits `workflow_name/preset_name`.
  2. Merges dictionaries in priority order: `defaults → preset → CLI overrides`.
  3. Validates merged values against `inputs:` via `validate_workflow_inputs`.
  4. Inlines sub-workflows recursively via `expand_pipeline`:
     - Sub-step IDs are prefixed with `{parent_id}.`.
     - Parent step dependencies (`after: [sub_wf]`) are rewritten to point to the sub-workflow's terminal (leaf) steps.
     - Detects recursive workflow composition cycles.

### 2.5 Caching & Manifest Storage (`prefect_yaml.cache`)

- **`compute_step_fingerprint`**:
  - Computes a deterministic 64-bit xxHash3 digest over step ID and resolved input arguments.
  - Ignores control flags (`force`, `force_rebuild`, `dry_run`) so forced runs produce the same cache fingerprint for subsequent normal runs.
- **`ManifestCache`**:
  - Manages in-memory and on-disk JSON records (`key`, `fingerprint`, `status`, `code_version`, `outputs`, `processed_at`).
  - Only writes manifest records when steps explicitly configure `cache: manifest` or `cache: hybrid`.

### 2.6 Routing Primitives (`prefect_yaml.routing`)

- **`matches_pattern`**: Evaluates item strings against gitwildmatch rules via `pathspec`, brace expansions (`*.{ppt,pptx}`), regex (`re:^pattern`), and URL prefixes (`https://**`).
- **`partition_items`**: Classifies collections of items into discrete buckets `(target_workflow, params_json)` using first-match-wins rule evaluation.
- **`execute_partition_router`**: Prefect flow that fans out task submissions across all buckets concurrently.
- **`evaluate_switch`**: Matches an expression string against case branches with fallback default.

### 2.7 Prefect Runtime (`prefect_yaml.runtime`)

- **`PrefectStepFactory`**: Generates a Prefect `@task` from a `CompiledStep`.
  - Supports `inline: true`: When the target is a Prefect Flow, extracts `.fn` to run tasks directly under the parent flow context without nested subflow overhead.
  - Configures retries, delays, and task tags.
- **`PrefectFlowFactory`**: Dynamically compiles the top-level `@flow` with `ThreadPoolTaskRunner(max_workers=...)`.
  - Submits tasks with `wait_for` future dependencies.
  - Resolves `${steps.<id>.result.<attr>}` dynamically from predecessor outputs.
  - Handles `foreach` iteration submission and result collection.
  - Manages failure policies: `abort` (fails fast and records error), `skip` (sets result to None), and `continue` (records exception dictionary).

---

## 3. Public Python API Reference

```python
from prefect_yaml import (
    PrefectFlowFactory,
    PrefectServer,
    WorkflowDef,
    execute_workflow,
    flow_from_yaml,
    load_workflows,
    resolve_workflow_invocation,
    workflow,
)
```

### 3.1 `flow_from_yaml(source, *, workflow_name=None, values=None, max_workers=4)`

Parses inline YAML text, a file path, or a dictionary into a ready-to-run Prefect `@flow`.

```python
from prefect_yaml import flow_from_yaml

yaml_code = """
workflows:
  quick_calc:
    inputs:
      x: {type: int, default: 5}
    run: math.sqrt
    with:
      x: "${values.x}"
"""
flow = flow_from_yaml(yaml_code, values={"x": 16})
results = flow()
print(results)  # {'run': 4.0}
```

### 3.2 `@workflow` Decorator

Registers a Python callable in the global registry under a friendly name:

```python
from prefect_yaml import workflow


@workflow(name="compute_metrics", description="Compute summary statistics")
def compute_metrics(*, data_path: str, threshold: float = 0.5) -> dict:
    return {"status": "ok", "processed": 100}
```

Registered functions can be referenced directly in YAML steps via `run: compute_metrics`.

### 3.3 `resolve_workflow_invocation(...)` and `execute_workflow(...)`

Lower-level programmatic execution path:

```python
from prefect_yaml import execute_workflow, resolve_workflow_invocation

invocation = resolve_workflow_invocation(
    "data_pipeline/prod",
    cli_overrides={"batch_size": 25},
    source="./config/workflows.yaml",
)
results = execute_workflow(invocation)
```

### 3.4 `PrefectServer`

Lifecycle daemon manager for local Prefect server development:

```python
from prefect_yaml import PrefectServer

server = PrefectServer(port=4200)
if not server.is_running():
    server.start()  # Detached background daemon
print(server.ui_url)
```

---

## 4. Contributing & Adding Features

- **Adding an Input Type:** Extend `ContractType` in `contracts/types.py` and implement coercion/validation logic in `contracts/validator.py`.
- **Adding Step Kinds:** Add value to `StepKind` in `models/compiled.py`, add resolution in `compiler/compiler.py`, and implement task execution in `runtime/step_factory.py`.
- **Unit Testing:** Tests live in `tests/unit/`. Use `_MockTask` and `_MockFuture` for zero-server test isolation. Run via `uv run --offline pytest -v`.
