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
      main.py              # Combined CLI entrypoint (network + demand)
      network.py           # Network-only CLI: python -m sumo_optimise.conversion.cli.network
      demand.py            # Demand-only CLI: python -m sumo_optimise.conversion.cli.demand
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
      models.py            # Dataclasses/Enums: Snap, Main, Junction configs, Signals, Events, IR
    utils/
      constants.py         # Shared constants (e.g., movement tokens)
      errors.py            # Exception taxonomy (schema/semantic/build/netconvert)
      io.py                # Output dir creation, JSON/Schema loading
      logging.py           # Logger config (file + console)
    config/
      __init__.py          # Defaults/paths/version guard (if any)
    data/
      schema.json          # JSON Schema (v1.4)
data/
  reference/
    SUMO_OPTX_v1.4_sample.json# Sample specification for smoke tests
jsonschema/
  __init__.py                      # (Namespace stub; do not confuse with PyPI jsonschema)
sumo_optimise.egg-info/            # Package metadata (editable install)
```

## 3) Identifier & Helper Conventions

### Node identifiers
- Main carriageway breakpoints resolve via `builder.ids.main_node_id(pos, half)` and emit `Node.Main.{pos}.{N|S}`. The helper still accepts EB/WB tokens but normalises to the cardinal halves.
- Minor stubs terminate at `Node.Minor.{pos}.{N|S}_end` from `minor_end_node_id`. These remain the only dead-end node identifiers emitted for approaches.
- Pedestrian-only sidewalk endpoints live under `PedEnd.*`. Mainline endpoints follow `PedEnd.Main.{E|W}_end.{N|S}_sidewalk`; minor approaches follow `PedEnd.Minor.{pos}.{N|S}_end.{E|W}_sidewalk`. No other `PedEnd` spellings are valid.

### Edge identifiers
- Mainline segments come from `main_edge_id(direction, begin_pos, end_pos)` and render `Edge.Main.{EB|WB}.{begin}-{end}`. `begin_pos` / `end_pos` were renamed from west/east specific names so that callers pass values in **travel order**: eastbound edges require `begin_pos < end_pos`, westbound requires `begin_pos > end_pos`.
- Minor approaches use the bidirectional helper `minor_edge_id(pos, flow, orientation)` with canonical `flow` tokens `to` / `from`. The helper normalises them to `NB` / `SB` and emits `Edge.Minor.{N_arm|S_arm}.{NB|SB}.{pos}`.

### Cluster identifiers
- Junction joins remain `Cluster.{pos}` via `cluster_id`. When attaching traffic signals, reuse the same ID as the TLS name.

### Crossing identifiers
- Intersection crossings use `Xwalk.{pos}.{cardinal}` with the optional split half emitted as a third token: `Xwalk.{pos}.{cardinal}.{N|S|E|W}_half` via `crossing_id_main_split`. This supersedes the older `{pos}_{side}` hyphenation—do not reintroduce the dashed form.
- Minor approaches rely on `crossing_id_minor`, emitting the same `Xwalk.{pos}.{cardinal}` pattern with `{cardinal}` constrained to `{N,S}`.
- Mid-block helpers (`crossing_id_midblock*`) now generate `Xwalk.{pos}` or `Xwalk.{pos}.{N|S}_half`. Use these instead of the retired `midblock_crossing_id*` utilities.

### Quick reference
- Prefer orientation-neutral wording in new comments/docstrings (`north/south halves`, `begin/end positions`).
- When documenting a new helper, include the literal identifier pattern and the validation rule (e.g., `main_edge_id` raising when direction/order clash) so downstream CLI and docs stay in sync.

## 4) Demand & Config Outputs

- The converter now emits a merged `demandflow.rou.xml` containing `<personFlow>` and `<flow>`
  elements whenever pedestrian and/or vehicle demand CSVs are supplied.
- Vehicle demand files mirror the pedestrian ones:
  `veh_EP_demand_sampleUpd.csv` (signed `EndID` + `vehFlow`) and
  `veh_jct_turn_weight_sampleUpd.csv` (`ToNorth|ToWest|ToSouth|ToEast` weights).
  Main endpoints accept the `Node.Main.{E|W}_end` aliases; canonical IDs are resolved to the boundary nodes.
- A minimal `config.sumocfg` is written alongside the PlainXML artefacts whenever a routes file exists.
  It references `3-assembled.net.xml` (produced by the optional two-step `netconvert`) and `demandflow.rou.xml`.

## 5) Codex Virtual Environment

- Codex performs all local work inside `.codex-venv` (Python 3.12). If it is missing or stale, recreate it with `python3 -m venv --upgrade-deps .codex-venv` (add `--clear` when you need to blow away a broken env).
- Immediately install project + test dependencies into that env via `.codex-venv/bin/pip install -e '.[test]'` so CLI runs, pytest, etc. all share the same toolchain.
- Activate the environment for every Codex command sequence using `source .codex-venv/bin/activate`; leave any user-managed envs such as `.venv` untouched.
- When documenting repro steps or scripts, assume `.codex-venv` is already active so paths like `python -m sumo_optimise.conversion.cli` resolve against the editable install.

## 6) Workspace helpers

- The `workspace/` directory intentionally keeps standalone scripts/examples that interact with the converter **only through the published CLI entry points**. Do not import `sumo_optimise` modules from those helpers; shell out with `python -m sumo_optimise.conversion.cli.*` (or equivalent) so they stay decoupled from internal APIs.
