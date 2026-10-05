# prefect-yaml

**Declarative YAML DSL, typed contracts, and orchestration runtime for Prefect 3 workflows.**

`prefect-yaml` compiles human-readable, declarative YAML definitions into robust Prefect 3 `@flow` and `@task` DAGs. It eliminates Python orchestration boilerplate while bringing first-class typed contracts, automatic CLI parameter coercion, xxHash3 manifest-based incremental execution, and generic routing.

---

## Key Features

- **Declarative YAML Composition**: Compose complex DAGs without writing Python glue code.
- **Typed Input Contracts (`inputs:`)**: Strongly typed inputs (`int`, `str`, `float`, `bool`, `path`, `enum`, `list`, `dict`) with constraints (`choices`, `min`, `max`, `regex`) and automatic CLI coercion.
- **Predictable Value Precedence**: `defaults → presets → CLI --set KEY=VALUE` with OmegaConf `${values.*}` interpolation.
- **Deterministic Manifest Caching**: Skip re-processing unchanged data with xxHash3 (64-bit) content fingerprints.
- **Generic Routing Primitives**: Partition collections of items across parallel flows with wildcard matching, or conditionally switch branches.
- **Subflow Control & Inlining**: Choose full Prefect UI visibility or `inline: true` subflow flattening.
- **Local Prefect Server Daemon**: Manage background development servers directly from the CLI.

---

## Documentation & Learning Resources

- 📖 **[User Guide: DSL & CLI Reference](docs/dsl_and_cli.md)**: Comprehensive guide on YAML syntax, input contracts, step fields, dependencies, and CLI commands.
- 🛠️ **[Developer & Maintainer Guide: Design and API Architecture](docs/design_and_api.md)**: Deep dive into the internal compiler, DAG validation algorithms, Pydantic models, and runtime architecture.
- 🚀 **[Hands-on Tutorial Notebook](notebooks/prefect_yaml_tutorial.ipynb)**: Step-by-step interactive Jupyter notebook walking through single-step workflows, typed contracts, DAG pipelines, caching, and partitioning.

---

## Quick Example

### 1. Write Workflow YAML (`pipeline.yaml`)

```yaml
workflows:
  data_etl:
    description: "Extract and score data"
    inputs:
      data_path:
        type: path
        required: true
        description: "Path to input file"
      threshold:
        type: float
        default: 0.8
        minimum: 0.0
        maximum: 1.0
    defaults:
      data_path: "/data/raw.csv"
    presets:
      strict:
        threshold: 0.95
    pipeline:
      - id: extract
        run: my_module.extract_records
        with:
          path: "${values.data_path}"

      - id: score
        run: my_module.score_records
        after: [extract]
        with:
          records: "${steps.extract.result.records}"
          threshold: "${values.threshold}"
        cache: manifest
```

### 2. Execute via CLI

```bash
# Dry run: view resolution table, DAG structure, and cache status
uv run prefect-yaml run data_etl --dry-run

# Run with preset and CLI overrides (automatically coerced)
uv run prefect-yaml run data_etl/strict --set threshold=0.9
```

### 3. Execute via Python

```python
from prefect_yaml import flow_from_yaml

flow = flow_from_yaml("pipeline.yaml", values={"threshold": 0.85})
results = flow()
print(results)
```

---

## Installation

```bash
# Add with uv
uv add "prefect-yaml @ git+https://github.com/tclatos/prefect-yaml@main"
```

---

## CLI Overview

```bash
prefect-yaml list                     # List all workflows and presets
prefect-yaml show <workflow>          # Inspect input contracts, presets, and DAG
prefect-yaml run <workflow>           # Execute workflow
prefect-yaml validate                 # Validate all YAML definitions in directory
prefect-yaml serve <workflow>         # Serve as long-running Prefect deployment
prefect-yaml server start             # Start local Prefect server daemon
prefect-yaml server status            # Check local server health
prefect-yaml server stop              # Stop local server daemon
```
