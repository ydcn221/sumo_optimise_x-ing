## 1) Mission & Contract

This repository builds **SUMO PlainXML** (nodes/edges/connections) for a **single straight main road** ("Main") with orthogonal **minor roads** at intersections and **mid-block pedestrian crossings**, from a JSON specification (schema v1.3).

**Agent contract:**

* Produce **deterministic outputs** for the same input spec and code version.
* Keep the pipeline **pure and modular**: parsing → validation → planning → IR → emitters → (optional) SUMO integration.
* Do not introduce I/O into logic layers; keep file/CLI concerns in adapters.
* Prefer **small, testable functions** with explicit inputs/outputs over large classes.

---

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
      nodes.py             # Write net.nod.xml
      edges.py             # Write net.edg.xml
      connections.py       # Write net.con.xml (vehicle connections + crossings)
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

**Key invariant:** `conversion/*` is the source of truth for corridor planning and emission.

---

## 3) Supported Input & Outputs

* **Input:** JSON spec validated against `sumo_optimise/conversion/data/schema.json` (v1.3).
* **Outputs (PlainXML):**

  * `net.nod.xml` — nodes (main EB/WB breakpoints; cluster/join nodes at interior breakpoints; minor road dead-ends).
  * `net.edg.xml` — edges (main segments between breakpoints; minor to/from main).
  * `net.con.xml` — vehicle `<connection>` and pedestrian `<crossing>` elements.
* **Optional:** `base.net.xml` and `network.net.xml` via SUMO `netconvert` (two-step).

**Assumptions:** left-hand traffic (LHT), main and minor roads are straight and orthogonal.

---

## 4) Build & Runtime (Non-Interactive)

**Environment (Windows/Unix):**

* Python 3.11 or 3.12 recommended (3.8+ supported).
* SUMO tools on `PATH` if you need final `network.net.xml`:

  * `netconvert` (required for two-step integration).
  * `netedit` (optional, visualization).
* Python dependency: `jsonschema` (Draft-07). Install via pip.

**Setup (editable install):**

```bash
python -m pip install --upgrade pip
python -m pip install -e .
python -m pip install jsonschema
```

**CLI usage (root of repo):**

```bash
python -m sumo_optimise.conversion.cli --input path/to/spec.json
# Add --schema path/to/schema.json to override packaged schema if needed.
# Run with --help to discover available flags.
```

**Outputs:**

* Created under `plainXML_out/<mmdd_###>/`.
* Files: `net.nod.xml`, `net.edg.xml`, `net.con.xml`, `build.log`.
* If `netconvert` found: `base.net.xml`, `network.net.xml`.

---

## 5) Agent Execution Recipes

### A. End-to-end build (no modifications)

1. Ensure editable install (above).
2. Run:

   ```bash
   python -m sumo_optimise.conversion.cli --input path/to/spec.json
   ```
3. Verify outputs in `plainXML_out/.../` and inspect `build.log` (no ERROR).

### B. Two-step `netconvert` (if skipped automatically)

From the output directory:

```bash
netconvert --lefthand --node-files net.nod.xml --edge-files net.edg.xml \
  --sidewalks.guess --output-file base.net.xml

netconvert --lefthand --sumo-net-file base.net.xml \
  --connection-files net.con.xml --output-file network.net.xml
```

### C. Smoke test (no tests folder present)

* Use sample spec (if provided by repo) and verify:

  * Node count matches expected breakpoints (0, grid_max, each cluster pos, and overlay boundaries).
  * Minor edges exist per intersection side.
  * Crossings are present at intersections and mid-block positions; split where configured.
  * No `[VAL]` ERRORs in log; warnings like endpoint (`W201`) acceptable if intended.

---

## 6) Semantic Rules (High-Level)

Agents must preserve or extend these checks in `conversion/checks/semantics.py`:

* **Range:** events within `[0, main_road.length]` (raw) and `[0, grid_max]` (snapped).
* **Duplicates:** same-kind events at same snapped position are invalid.
* **Junction × Mid-block collision:** exact/nearby overlaps are either **absorbed** into the junction’s crossing flags or rejected with error (policy is in code).
* **Signals:**

  * If `signalized=true` ⇒ `signal.profile_id` must exist in the appropriate kind (`tee/cross/xwalk_midblock`).
  * If `signalized=false` ⇒ no `signal` object should be present.
* **Templates:** all referenced `junction_templates` must exist; ID uniqueness across tee/cross.
* **Approach overlays:** lane override intervals are grid-aligned; overlaps resolve to **max lane count**.

**Error/Warning taxonomy:** codes like `E101–E108` and `W201` are used; do not remove or repurpose existing codes without updating documentation and callers.

---

## 7) Planner & Emitters (Deterministic Expectations)

* **Snapping:** `round_to_grid(value, step, tie_break)` with midpoint tie-breaking (`toward_west`/`toward_east`).
* **Grid upper bound:** `grid_max = floor(length/step)*step`; all planning constrained within `[0, grid_max]`.
* **Breakpoints:** union of `{0, grid_max} ∪ cluster positions ∪ overlay start/end}` with reason tags.
* **IR to XML:** emitters must be **pure** (return strings or write exactly the provided paths) and deterministic.

**Connections:**

* For each approach, enumerate allowed L/T/R based on kind and `median_continuous`.
* Map lane ranges using a deterministic band partition; never produce zero connections where movement is allowed (else error).

**Crossings:**

* Intersections: always minor-side crossings; main-side per placement flags; split if `refuge_island_on_main`.
* Mid-block: single or split per spec; side selection follows `tie_break`.

---

## 8) Conventions (Agents must follow)

* **File encoding:** write text as UTF-8. If generating CSV artifacts (e.g., warnings), **use `utf-8-sig`** for Excel compatibility on Windows.
* **IDs and names (in `builder/ids.py`):**

  * Nodes: `Node.EB.{pos}`, `Node.WB.{pos}`, `Cluster.Main.{pos}`, `Node.Minor.{pos}.{N|S}`.
  * Edges: `Edge.Main.{EB|WB}.{start}-{end}`, `Edge.Minor.{pos}.{to|from}.{N|S}`.
  * Crossings: stable prefixes for main/minor; include position and side.
* **Enums & tokens:** directions (`EB/WB`, `NORTH/SOUTH`), movements (`L/T/R/PED`), kinds (`TEE/CROSS/XWALK_MIDBLOCK`), median (`CONTINUOUS/NONE`), tie-break (`TOWARD_WEST/TOWARD_EAST`).
* **Logging:** use project logger; levels: INFO (milestones), WARNING (non-fatal), ERROR (abort). Include category tags like `[SCH]`, `[VAL]`, `[NETCONVERT]`.
* **No I/O in logic:** only `cli/*`, `utils/io.py`, and `sumo_integration/*` should touch the filesystem or `subprocess`.

---

## 9) Safe Modification Checklist

Before editing:

1. Locate the correct layer:

   * Input/schema → `parser/*`, `data/schema.json`.
   * Validation → `checks/semantics.py`.
   * Snapping/lanes → `planner/*`.
   * Names/format → `builder/ids.py`, `emitters/*`.
   * CLI/UX → `conversion/cli/main.py`.
2. Add unit-level assertions inside functions (if no tests folder present).
3. Run the sample build; inspect `build.log` and the XML outputs.
4. If changing schema or behavior, update comments and docstrings accordingly.

After editing:

* Ensure outputs remain deterministic for identical inputs.
* Log messages must be actionable (who/what/where): include event index, position, template/signal IDs.
* Do not silently “fix” invalid input unless an explicit repair policy is implemented and logged as WARNING.

---

## 10) SUMO Integration Notes

* The two-step `netconvert` wrapper **must**:

  * Pass `--lefthand`.
  * Use `--sidewalks.guess` on the first pass (nodes+edges ⇒ `base.net.xml`).
  * Add connections in the second pass to produce `network.net.xml`.
* If `netconvert` is not on PATH:

  * Log a clear WARNING and skip; do not fail the whole pipeline.
* `netedit.py` can optionally launch the GUI; keep this optional and non-blocking.

---

## 11) Troubleshooting (for Agents)

* **Module import errors:** ensure `pip install -e .` was executed; run from repo root.
* **Schema missing:** verify `sumo_optimise/conversion/data/schema.json` exists or pass `--schema` pointing to a valid path.
* **No outputs:** check `build.log` for `[SCH]` or `[VAL]` errors; fix input spec accordingly.
* **No `network.net.xml`:** `netconvert` likely missing; install SUMO or run the two-step commands manually.
* **Crossing duplication at junction:** confirm absorption logic in semantics and crossing planner; mid-block at same pos as junction should not double-output.

---

## 12) Extension Points

* **Signal logic:** extend `signal_profiles` mapping and, if needed, enrich `<tlLogic>` emission (currently driven by profiles).
* **Right-hand traffic:** add a switch in CLI/config; geometry and connection directions must swap consistently.
* **Templates:** support richer lane patterns per approach; keep overlay merge as **max lanes**.
* **Validation:** add new codes rather than reusing existing ones; document behavior in comments and messages.

---

## 13) Minimal CI Hook (suggested)

If setting up CI for agents:

```bash
python -m pip install -e .
python -m pip install jsonschema
python - <<'PY'
from pathlib import Path
from sumo_optimise.conversion.cli import main  # or call pipeline directly
# Run a smoke build on the bundled sample if available:
# main(["--input", "data/reference/schema_v1.3_sample.json"])
PY
# Optionally: check that plainXML_out was created and files exist.
```

(If a `tests/` suite is later added, run `pytest -q`.)

---

## 14) Glossary

* **Breakpoint:** salient main-road position where a segment begins/ends (endpoints, cluster positions, overlay start/end).
* **Cluster:** all events sharing the same snapped position (e.g., junction + crossing).
* **Overlay (lane override):** template-driven lane changes applied over grid-aligned intervals upstream/downstream of junctions.