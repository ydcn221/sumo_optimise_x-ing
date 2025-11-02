## 1) Mission & Contract
Always use context7 when I need code generation, setup or configuration steps, or
library/API documentation. This means you should automatically use the Context7 MCP
tools to resolve library id and get library docs without me having to explicitly ask.

## 2) Repository Layout (Agent View)

```
sumo_optimise/
  __init__.py
  conversion/
    pipeline.py            # Orchestrates the build (entry from CLI)
    cli/
      main.py              # CLI entrypoint: python -m sumo_optimise.conversion.cli
    parser/
      spec_loader.py       # Load JSON + schema, map to domain models
    checks/
      semantics.py         # Semantic validation (range, duplicates, signals, etc.)
    planner/
      snap.py              # Grid rounding, grid_max
      lanes.py             # Lane overrides, breakpoints
      crossings.py         # Crossing placement helpers (main/minor/split)
      geometry.py          # Coordinate helpers for nodes (EB/WB Y, minor ends)
    builder/
      ids.py               # ID/name generation for nodes/edges/crossings
    emitters/
      nodes.py             # Write 1-generated.nod.xml
      edges.py             # Write 1-generated.edg.xml
      connections.py       # Write 1-generated.con.xml (vehicle connections + crossings)
    sumo_integration/
      netconvert.py        # Two-step netconvert wrapper (optional)
      netedit.py           # Helper for launching netedit (optional)
    domain/
      models.py            # Dataclasses/Enums: Snap, Main, Templates, Signals, Events, IR
    utils/
      constants.py         # Shared constants (e.g., movement tokens)
      errors.py            # Exception taxonomy (schema/semantic/build/netconvert)
      io.py                # Output dir creation, JSON/Schema loading
      logging.py           # Logger config (file + console)
    config/
      __init__.py          # Defaults/paths/version guard (if any)
    data/
      schema.json          # JSON Schema (v1.3)
data/
  reference/
    schema_v1.3_sample.json# Sample specification for smoke tests
jsonschema/
  __init__.py                      # (Namespace stub; do not confuse with PyPI jsonschema)
sumo_optimise.egg-info/            # Package metadata (editable install)
```