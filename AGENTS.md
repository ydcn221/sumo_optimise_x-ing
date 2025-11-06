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
      main.py              # CLI entrypoint: python -m sumo_optimise.conversion.cli.main
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

## 3) Identifier & Helper Conventions

### Node identifiers
- Main carriageway breakpoints resolve via `builder.ids.main_node_id(pos, half)` and emit `Node.{pos}.{MainN|MainS}`. The suffixes `MainN` and `MainS` replace the old `MainEB` / `MainWB` tokens—always prefer the cardinal halves, even if the caller still speaks in east/west.
- Minor stubs terminate at `Node.{pos}.MinorNEndpoint` / `Node.{pos}.MinorSEndpoint` from `minor_end_node_id`. These are the only dead-end node identifiers emitted for approaches.

### Edge identifiers
- Mainline segments come from `main_edge_id(direction, begin_pos, end_pos)` and render `Edge.Main.{EB|WB}.{begin}-{end}`. `begin_pos` / `end_pos` were renamed from west/east specific names so that callers pass values in **travel order**: eastbound edges require `begin_pos < end_pos`, westbound requires `begin_pos > end_pos`.
- Minor approaches use the bidirectional helper `minor_edge_id(pos, flow, orientation)` with canonical `flow` tokens `to` / `from`. The helper normalises them to `NB` / `SB`, so avoid the legacy `northbound` / `southbound` spellings in new code.

### Cluster identifiers
- Junction joins remain `Cluster.{pos}.Main` via `cluster_id`. When attaching traffic signals, reuse the same ID as the TLS name.

### Crossing identifiers
- Intersection crossings use `Cross.{pos}.{cardinal}` with the optional split half emitted as a third token: `Cross.{pos}.{cardinal}.{N|S|E|W}` via `crossing_id_main_split`. This supersedes the older `{pos}_{side}` hyphenation—do not reintroduce the dashed form.
- Minor approaches rely on `crossing_id_minor`, emitting the same `Cross.{pos}.{cardinal}` pattern with `{cardinal}` constrained to `{N,S}`.
- Mid-block helpers were renamed to `crossing_id_midblock` / `crossing_id_midblock_split` and generate the `CrossMid.{pos}` or `CrossMid.{pos}.{N|S}` forms respectively. Use these instead of the retired `midblock_crossing_id*` utilities.

### Quick reference
- Prefer orientation-neutral wording in new comments/docstrings (`north/south halves`, `begin/end positions`).
- When documenting a new helper, include the literal identifier pattern and the validation rule (e.g., `main_edge_id` raising when direction/order clash) so downstream CLI and docs stay in sync.
