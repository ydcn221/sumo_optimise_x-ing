# SUMO Linear Corridor Network Converter

Generate a SUMO network (PlainXML) for a **single, straight main road** with orthogonal minor roads (tee/cross intersections) and mid-block pedestrian crossings from a **JSON specification (v1.3)**. The converter validates the JSON, plans lanes and crossings, emits `1-generated.nod.xml` / `1-generated.edg.xml` / `1-generated.con.xml` / `1-generated.tll.xml`, and can optionally run `netconvert` to build a runnable `3-n+e+c+t.net.xml`.

> This repository hosts the modular corridor converter introduced for schema v1.3.

---

## Key capabilities

* **Schema-driven input (v1.3)** with draft-07 JSON Schema.
* **Semantic validation** (range checks, duplicate/near-duplicate events, template/profile consistency).
* **Grid snapping** with configurable step and tie-break.
* **Template-based intersections** (tee/cross), lane override regions, continuous median rules.
* **Pedestrian crossings** at intersections and mid-block (single or split).
* **Vehicle turn connections** (L/T/R) with deterministic lane mapping (left-to-left, straight fan-out, rightmost sharing).
* **Two-step `netconvert` integration** (optional) and structured build logs.

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
    data/schema.json       # Embedded JSON Schema (v1.3)
    pipeline.py            # Orchestration
data/reference/             # Sample specifications (e.g., schema_v1.3_sample.json)
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

1. **Prepare input JSON** (v1.3). Use your own file or adapt the provided sample under `data/reference/`.
2. **Run the CLI** to build PlainXML (nodes/edges/connections).
3. **Optionally run `netconvert`** to produce `3-n+e+c+t.net.xml`.

### Minimal build

```bash
# POSIX
$ python -m sumo_optimise.conversion.cli --input path/to/spec.json

# PowerShell
PS> python -m sumo_optimise.conversion.cli --input path\to\spec.json
```

**Default behavior**

* The converter validates against **`sumo_optimise/conversion/data/schema.json`** (v1.3).
* Output directory is created under **`plainXML_out/`** (timestamped, e.g., `1012_001`).
* Files written:

  * `1-generated.nod.xml` — nodes
  * `1-generated.edg.xml` — edges
  * `1-generated.con.xml` — vehicle connections and pedestrian crossings
  * `1-generated.tll.xml` — traffic-light logic programmes
  * `build.log` — structured log

  **Identifier schema (excerpt)**

  * Main nodes: `Node.{pos}.MainN` (northern carriageway, formerly EB) / `Node.{pos}.MainS` (southern carriageway, formerly WB)
  * Minor dead-ends: `Node.{pos}.MinorNEdge` / `Node.{pos}.MinorSEdge`
  * Cluster joins: `Cluster.{pos}.Main`
  * Main edges: `Edge.Main.{EB|WB}.{begin}-{end}` (westbound segments list `begin > end`)
  * Minor edges: `Edge.Minor{N|S}.{NB|SB}.{pos}`
  * Junction crossings: `Cross.{pos}.{cardinal}` (cardinal in `{N|E|S|W}`). Split approaches use `Cross.{pos}.{cardinal}.{half}`,
    where the half token resolves to `{N|S}` for east/west crossings and `{E|W}` for north/south crossings.
  * Mid-block crossings: `CrossMid.{pos}` or `CrossMid.{pos}.{N|S}`

### Build + netconvert (if `netconvert` is on PATH)

Some setups run it automatically. If not, you can run manually:

```bash
# Step 1: derive cleaned PlainXML geometry (no internal links)
$ netconvert --lefthand \
  --sidewalks.guess \
  --no-internal-links \
  --node-files 1-generated.nod.xml \
  --edge-files 1-generated.edg.xml \
  --plain-output-prefix 2-cooked

# Step 2: merge crossings/connections/signals into the final net
$ netconvert --lefthand \
  --node-files 2-cooked.nod.xml \
  --edge-files 2-cooked.edg.xml \
  --connection-files 1-generated.con.xml \
  --tllogic-files 1-generated.tll.xml \
  --output 3-n+e+c+t.net.xml
```

Open `3-n+e+c+t.net.xml` in **SUMO-GUI** or **netedit** to inspect.

---

## Input specification (v1.3 overview)

* `version`: `"1.3"`
* `snap`: `{ "step_m": int>=1, "tie_break": "toward_west" | "toward_east" }`
* `defaults`: e.g., minor road length, crossing width, speed_kmh
* `main_road`: `{ "length_m": number, "center_gap_m": number, "lanes": int }`
* `junction_templates`: templates for `tee` and `cross` (approach length, lane overrides, `median_continuous`, etc.)
* `signal_profiles`: fixed-time profiles for `tee` / `cross` / `xwalk_midblock` (cycle, phases, allowed movements).
  * `cycle_s` must equal the sum of the listed phase durations.
  * `yellow_duration_s` defines the yellow interval that **replaces** the tail of the last continuous green stretch for a vehicle
    movement before it turns red. If the movement remains green across multiple successive phases, total their durations until
    the phase that first turns the movement red, then treat the final `yellow_duration_s` seconds of that combined green time as
    yellow before entering the red phase.
  * `ped_early_cutoff_s` (intersections only) shortens the pedestrian clearance: pedestrians turn red `ped_early_cutoff_s` seconds
    before the next phase begins.
* `layout`: ordered events along the main road:

  * `tee`: `{ pos_m, branch: "north"|"south", template, main_u_turn_allowed, refuge_island_on_main, signalized, two_stage_tll_control?, signal?, main_ped_crossing_placement }`
  * `cross`: `{ pos_m, template, main_u_turn_allowed, refuge_island_on_main, signalized, two_stage_tll_control?, signal?, main_ped_crossing_placement }`
  * `xwalk_midblock`: `{ pos_m, refuge_island_on_main, signalized, two_stage_tll_control?, signal? }`

    * `two_stage_tll_control` — required boolean field that is present only when `signalized=true` and `refuge_island_on_main=true`.
    * `main_u_turn_allowed` — required boolean for intersections. `false` prohibits main-road U-turns in both directions; `true` keeps them.

**Rules (selected):**

* All positions round to the grid (`step_m`). Midpoints use `tie_break`.
* Events must lie within `[0, length]`; snapped positions within `[0, grid_max]`.
* A junction and a mid-block crossing at the **same** position will be **absorbed** into the junction’s crossing flags.
* `median_continuous=true` restricts certain turns (e.g., main-road right turn).
* `main_u_turn_allowed=false` removes U-turn connections on the main road (both EB and WB) at that junction.
* If `signalized=true`, a valid profile reference is required.

---

## Demand CSV specification

Demand files supplement the JSON specification when generating SUMO flows. Two
UTF-8 CSVs are expected (headers required):

### Vehicles

Columns: `endpoint_id`, any column containing `generated`, and any column
containing `attracted`. Additional tokens may be present (e.g.
`generated_veh_per_h`) as long as the keywords remain intact.

* `endpoint_id` — identifier returned by the endpoint catalogue
  (:class:`sumo_optimise.conversion.domain.models.VehicleEndpoint`).
* `generated*` — vehicles leaving the corridor via the endpoint (veh/h).
* `attracted*` — vehicles entering the corridor via the endpoint (veh/h).

### Pedestrians

The pedestrian CSV serves two distinct scopes: **catalogued endpoints** and
**main-road frontage segments**. Column names must contain `generated`/
`attracted`; per-metre rates additionally require the suffix `per_m`.

* `endpoint_id` — direct reference to a pedestrian endpoint (side inferred from
  the catalogue metadata; both EB/WB main-road endpoints are listed separately).
* `location_id` — reference to a main-road frontage. Two schemas are allowed and
  strictly validated:
  * Point frontage: `Walk.Main.<side>.P<pos>` (e.g. `Walk.Main.EB.P050`).
  * Range frontage: `Walk.Main.<side>.R<start>-<end>` (e.g.
    `Walk.Main.WB.R000-200`).
* `generated*` / `attracted*` — absolute pedestrian volumes per hour.
* `generated*_per_m` / `attracted*_per_m` — distributed rates per metre.

Each row must match **exactly one** of the supported layouts:

1. `endpoint_id`, `generated*`, `attracted*`
   *Demand tied directly to a pedestrian endpoint.*
2. `location_id` (point), `generated*`, `attracted*`
   *Point frontage along the main road. The `location_id` encodes both the side
   (`EB` = north, `WB` = south) and the snapped position. If `position_m` is
   provided it must match the ID.*
3. `location_id` (range), `generated*_per_m`, `attracted*_per_m`
   *Distributed frontage demand. The `location_id` encodes the side and
   inclusive range. Optional `start_m`/`end_m` columns must match the ID.*

Side matching relies on the endpoint catalogue produced by
``sumo_optimise.conversion.demand.catalog``. Structural mistakes (missing
columns, ambiguous positions, unknown IDs, range mismatches) are collected and
raised as a single ``DemandValidationError`` per CSV after the entire file has
been inspected.

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

  * `1-generated.nod.xml` — main road endpoints and breakpoints; cluster `<join>` nodes at interior junctions; minor dead-ends.
  * `1-generated.edg.xml` — EB/WB segments per breakpoint; minor “to/from” one-way edges; lane counts with overrides; speeds in m/s.
  * `1-generated.con.xml` — `<connection>` for vehicle L/T/R movements with left turns mapped from the inside out, straight lanes paired left-to-left (fanning the rightmost lane when targets exceed sources), and right/U turns sharing the outermost lane when needed; `<crossing>` for pedestrians (junction/min-block; single/split).
  * `1-generated.tll.xml` — `<tlLogic>` definitions assembled from the signal profiles referenced by each cluster.
* **Build artifacts**

  * `build.log` — structured logs (schema/semantic/IO/netconvert).
  * `2-cooked.*` PlainXML and `3-n+e+c+t.net.xml` — if `netconvert` runs.

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
