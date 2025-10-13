# SUMO Linear Corridor Network Converter

Generate a SUMO network (PlainXML) for a **single, straight main road** with orthogonal minor roads (tee/cross intersections) and mid-block pedestrian crossings from a **JSON specification (v1.2)**. The converter validates the JSON, plans lanes and crossings, emits `net.nod.xml` / `net.edg.xml` / `net.con.xml`, and can optionally run `netconvert` to build a runnable `network.net.xml`.

> This repository contains a modular rewrite of the legacy monolithic script. Legacy compatibility shims are provided under `sumo_optimise/legacy`.

---

## Key capabilities

* **Schema-driven input (v1.2)** with draft-07 JSON Schema.
* **Semantic validation** (range checks, duplicate/near-duplicate events, template/profile consistency).
* **Grid snapping** with configurable step and tie-break.
* **Template-based intersections** (tee/cross), lane override regions, continuous median rules.
* **Pedestrian crossings** at intersections and mid-block (single or split).
* **Vehicle turn connections** (L/T/R) with deterministic lane mapping.
* **Three-step `netconvert` integration** (optional) and structured build logs.

---

## Repository layout (summary)

```
sumo_optimise/
  conversion/              # New modular pipeline
    cli/                   # CLI entry point
    parser/                # JSON/schema loading & parsing
    checks/                # Semantic validation
    planner/               # Snapping, breakpoints, lanes, crossings
    emitters/              # XML writers (nodes, edges, connections)
    sumo_integration/      # netconvert / netedit wrappers
    domain/                # Dataclasses & enums
    utils/                 # Logging, IO, constants, errors
    data/schema.json       # Embedded JSON Schema (v1.2)
    pipeline.py            # Orchestration
  legacy/                  # Backward-compatibility interface
jsonschema/                # (Local namespace; *not* the PyPI package)
```

> Detailed, agent-oriented guidance (build steps, conventions, test hooks) lives in **`AGENTS.md`** to keep this README focused.

---

## Requirements

* **Python** 3.11 or 3.12 (3.8+ may work but is not primary).
* **SUMO** (optional, for `netconvert`/`netedit`): ensure the executables are on your `PATH`.
* **Windows, macOS, Linux** supported. Paths below show both PowerShell (`PS>`) and POSIX shells (`$`).

---

## Installation

Create/activate a virtual environment and install the package in editable mode:

**Windows (PowerShell)**

```powershell
PS> .\.venv\Scripts\Activate.ps1
PS> python -m pip install --upgrade pip
PS> python -m pip install -e .
```

**macOS/Linux**

```bash
$ python -m venv .venv && source .venv/bin/activate
$ python -m pip install --upgrade pip
$ python -m pip install -e .
```

> If you install additional tools yourself, avoid naming collisions with the **PyPI `jsonschema`** package: this repo includes a **local** `jsonschema/` namespace for project internals.

---

## Quick start

1. **Prepare input JSON** (v1.2). Use your own file or adapt the provided sample.
2. **Run the CLI** to build PlainXML (nodes/edges/connections).
3. **Optionally run `netconvert`** to produce `network.net.xml`.

### Minimal build

```bash
# POSIX
$ python -m sumo_optimise.conversion.cli --input path/to/spec.json

# PowerShell
PS> python -m sumo_optimise.conversion.cli --input path\to\spec.json
```

**Default behavior**

* The converter validates against **`sumo_optimise/conversion/data/schema.json`** (v1.2).
* Output directory is created under **`plainXML_out/`** (timestamped, e.g., `1012_001`).
* Files written:

  * `net.nod.xml` — nodes
  * `net.edg.xml` — edges
  * `net.con.xml` — vehicle connections and pedestrian crossings
  * `build.log` — structured log

### Build + netconvert (if `netconvert` is on PATH)

Some setups run it automatically. If not, you can run manually:

```bash
# Step 1: build an initial SUMO network
$ netconvert --lefthand \
  --node-files net.nod.xml \
  --edge-files net.edg.xml \
  --sidewalks.guess \
  --output base_raw.net.xml

# Step 2: round-trip to PlainXML
$ netconvert \
  --sumo-net-file base_raw.net.xml \
  --plain-output-prefix base_plain

# Step 3: merge crossings/connections
$ netconvert --lefthand \
  --node-files base_plain.nod.xml \
  --edge-files base_plain.edg.xml \
  --connection-files net.con.xml \
  --output network.net.xml
```

Open `network.net.xml` in **SUMO-GUI** or **netedit** to inspect.

---

## Input specification (v1.2 overview)

* `version`: `"1.2"`
* `snap`: `{ "step_m": int>=1, "tie_break": "toward_west" | "toward_east" }`
* `defaults`: e.g., minor road length, crossing width, speed_kmh
* `main_road`: `{ "length_m": number, "center_gap_m": number, "lanes": int }`
* `junction_templates`: templates for `tee` and `cross` (approach length, lane overrides, `median_continuous`, `split_ped_crossing_on_main`, etc.)
* `signal_profiles`: fixed-time profiles for `tee` / `cross` / `xwalk_midblock` (cycle, phases, allowed movements)
* `layout`: ordered events along the main road:

  * `tee`: `{ pos_m, branch: "north"|"south", template, signalized, signal?, main_ped_crossing_placement }`
  * `cross`: `{ pos_m, template, signalized, signal?, main_ped_crossing_placement }`
  * `xwalk_midblock`: `{ pos_m, signalized, split_ped_crossing_on_main }`

**Rules (selected):**

* All positions round to the grid (`step_m`). Midpoints use `tie_break`.
* Events must lie within `[0, length]`; snapped positions within `[0, grid_max]`.
* A junction and a mid-block crossing at the **same** position will be **absorbed** into the junction’s crossing flags.
* `median_continuous=true` restricts certain turns (e.g., main-road right turn).
* If `signalized=true`, a valid profile reference is required.

---

## CLI usage

Get the full set of options:

```bash
$ python -m sumo_optimise.conversion.cli --help
```

Typical flags (names may vary by release):

* `--input PATH` — path to spec JSON (required).
* `--schema PATH` — override schema path (defaults to packaged `data/schema.json`).
* `--out DIR` — output directory root (default `plainXML_out/`).
* `--keep-output` — keep intermediate files; do not clean on failure.
* `--skip-netconvert` — generate XML only; do not call `netconvert`.

---

## Outputs

* **PlainXML**

  * `net.nod.xml` — main road endpoints and breakpoints; cluster `<join>` nodes at interior junctions; minor dead-ends.
  * `net.edg.xml` — EB/WB segments per breakpoint; minor “to/from” one-way edges; lane counts with overrides; speeds in m/s.
  * `net.con.xml` — `<connection>` for vehicle L/T/R movements; `<crossing>` for pedestrians (junction/min-block; single/split).
* **Build artifacts**

  * `build.log` — structured logs (schema/semantic/IO/netconvert).
  * `base_raw.net.xml`, `base_plain.*`, and `network.net.xml` — if `netconvert` runs.

---

## Troubleshooting

* **Import errors:** Use a venv and `pip install -e .`. Do not name your own folder `jsonschema` at repo root in a way that shadows the PyPI package.
* **`netconvert` not found:** Install SUMO and ensure `netconvert` is on PATH; otherwise use XML outputs directly.
* **Validation fails:** Check `build.log` for `[SCH]` (schema) or `[VAL]` (semantic) messages; the log includes positions, IDs, and fix hints.
* **Windows path issues:** Prefer absolute paths or PowerShell’s `${PWD}`. Avoid running from a different working directory than the spec.

---

## Roadmap

* Right-hand traffic toggle.
* `<tlLogic>` generation enhancements and profile libraries.
* Richer intersection geometry options (future non-linear support).
* Expanded test corpus and canonical XML equivalence tests.

---

## Contributing

* Open issues for bugs or feature requests.
* Keep changes modular; update the schema and semantic checks when the input format changes.
* Add brief notes to **`AGENTS.md`** if you introduce new conventions or build steps.

---

## License

Specify your license here (e.g., MIT). If omitted, the repository is “all rights reserved” by default.

---

## Acknowledgements

* Built for SUMO (Simulation of Urban MObility).
* Thanks to contributors of the legacy script on which this modular rewrite is based.
