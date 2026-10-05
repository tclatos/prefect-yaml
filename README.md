# prefect-yaml

Declarative YAML DSL, typed contracts, and orchestration runtime for Prefect workflows.

## Features

- **YAML-driven DAG Composition**: Define multi-step pipelines and sub-workflows without Python glue code.
- **Typed Input Contracts**: Strongly typed, validated inputs (`int`, `str`, `float`, `bool`, `path`, `enum`, `list`, `dict`) with constraints (`choices`, `min`, `max`, `regex`) and automatic CLI coercion.
- **Predictable Parameter Precedence**: Defaults → Named Presets → CLI `--set` overrides with OmegaConf `${values.*}` interpolation.
- **Manifest-based Caching**: Incremental execution via deterministic xxHash3 content fingerprints.
- **Generic Workflow Routing**: Partition item collections across parallel workflows via rules (`pathspec`, regex, callable), or switch branches conditionally.
- **Subflow Naming & Inline Mode**: Full Prefect UI observability or `inline: true` subflow flattening.
- **Standalone Prefect Server Management**: Start, stop, and monitor local Prefect server daemons.
