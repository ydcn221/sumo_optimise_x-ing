# SUMO Linear Corridor Network Converter

Generate a SUMO network (PlainXML) for a **single, straight main road** with orthogonal minor roads (tee/cross intersections) and mid-block pedestrian crossings from a **JSON specification (v1.4)**. The converter validates the JSON, plans lanes and crossings, emits `1-generated.nod.xml` / `1-generated.edg.xml` / `1-generated.con.xml` / `1-generated.tll.xml`, and can optionally run `netconvert` to build a runnable `3-assembled.net.xml`.

> This repository hosts the modular corridor converter introduced for schema v1.4.

---

## Key capabilities

* **Schema-driven input (v1.4)** with draft-07 JSON Schema.
* **Semantic validation** (range checks, duplicate/near-duplicate events, template/profile consistency).
* **Grid snapping** with configurable step and tie-break.
* **Per-junction geometry controls** (tee/cross) embedded directly in layout events: approach-lane tapers, minor in/out lane counts, `median_continuous`, and `main_u_turn_allowed`.
* **Pedestrian crossings** at intersections and mid-block (single or split).
* **Vehicle turn connections** (L/T/R/U) with deterministic one-to-one mapping (compacts overflow to the edge-side lane, no fan-out into extra target lanes) and optional per-approach manual lane movement overrides.
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
    data/schema.json       # Embedded JSON Schema (v1.4)
    pipeline.py            # Orchestration
data/reference/             # Sample specifications (e.g., SUMO_OPTX_v1.4_sample.json)
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

1. **Prepare input JSON** (v1.4). Use your own file or adapt the provided sample under `data/reference/` (e.g., `SUMO_OPTX_v1.4_sample.json`).
2. **Run the CLI** best suited for your task (network, demand, or both).
3. **Optionally run `netconvert`/`netedit`/`sumo-gui`** via the CLI flags.

### Minimal build

```bash
# POSIX
$ python -m sumo_optimise.conversion.cli.main --input path/to/spec.json

# PowerShell
PS> python -m sumo_optimise.conversion.cli.main --input path\to\spec.json
```

**Default behavior**

* The converter validates against **`sumo_optimise/conversion/data/schema.json`** (v1.4).
* Output directory is created under **`plainXML_out/`** (timestamped, e.g., `1012_001`).
* Files written:

  * `1-generated.nod.xml` — nodes
  * `1-generated.edg.xml` — edges
  * `1-generated.con.xml` — vehicle connections and pedestrian crossings
  * `1-generated.tll.xml` — traffic-light logic programmes
  * `config.sumocfg` — minimal SUMO config referencing the final net/routes (written when demand is emitted)
  * `build.log` — structured log

  **Identifier schema (excerpt)**

  * Main nodes: `Node.Main.{pos}.{N|S}` for the north/south halves. Helpers accept legacy EB/WB tokens but always emit the cardinal suffix.
  * Main edges: `Edge.Main.{EB|WB}.{begin}-{end}`. `begin` / `end` must follow the travel direction (`begin < end` for EB, `begin > end` for WB) and the helper raises when callers pass mismatched order.
  * Minor approach nodes: `Node.Minor.{pos}.{N|S}_end` from `minor_end_node_id`.
  * Sidewalk endpoints (pedestrian demand only): `PedEnd.Main.{E|W}_end.{N|S}_sidewalk` and `PedEnd.Minor.{pos}.{N|S}_end.{E|W}_sidewalk`. No other `PedEnd.*` forms are valid.
  * Minor edges: `Edge.Minor.{N_arm|S_arm}.{NB|SB}.{pos}` generated from `minor_edge_id(pos, flow, orientation)`; pass `flow="to"` / `"from"` and let the helper normalise to `NB` / `SB`.
  * Cluster joins / TLS IDs: `Cluster.{pos}`.
  * Junction crossings: `Xwalk.{pos}.{cardinal}`, with optional split halves yielding `Xwalk.{pos}.{cardinal}.{N|S|E|W}_half` via `crossing_id_main_split`.
  * Mid-block crossings: `Xwalk.{pos}` or `Xwalk.{pos}.{N|S}_half` produced by `crossing_id_midblock` / `crossing_id_midblock_split`.

### Traffic-signal movement tokens

Traffic-light programmes now consume explicit movement identifiers that align with the
shared conflict table in `src/sumo_optimise/conversion/data/conflict_table(prototype).txt`.
Tokens fall into two groups:

* **Vehicle:** `{N|E|S|W}B_{L|T|R|U}` with an optional `_p{r|g}` suffix (e.g., `NB_R_pg`).
  These reference the direction the vehicles travel *before* entering the junction and
  the manoeuvre (left/straight/right/U). The `_p{r|g}` suffix specifies whether the
  adjacent crosswalk should be red or green while that movement is active, letting the
  conflict table differentiate between yielding versus protected turns.
* **Pedestrian:** `PedX_{N|E|S|W…}` to address one or more crosswalks at once (order-free, e.g., `PedX_NS`, `PedX_NESW`), or the more granular
  `XE_N-half` / `XW_S-half` tokens emitted by the replace table for two-stage crossings. Single-direction tokens can optionally target one half via `PedX_E_south`, etc.; however, for single-stage crossings (no refuge island or `two_stage_tll_control=false`) both halves must be granted in the same phase or the converter will keep the entire crossing red.

Pedestrian early cut-off handling follows SUMO’s expectations: `ped_early_cutoff_s`
only trims pedestrian walks that overlap an approaching vehicle green. Phases that
are exclusively pedestrian (including two-stage half crossings) keep their full
duration even when the cut-off is longer than the phase length.

Internally the converter maps any legacy tokens (`main_L`, `EB_T`, `pedestrian`, etc.)
onto the new identifiers so existing specs continue to load, but the resolver now
computes the final `G/g/r` state purely from the table:

* All conflicts yielding `"P"` promote the movement to `G`.
* A mix containing `"Y"`/`"X"` (but no `"S"`) becomes a permissive `g`.
* Any `"S"` forces the movement red (`r`).

Right turns automatically add their matching U-turn movement when the approach
provides that connection, preserving the previous shortcut.

### Build + netconvert (if `netconvert` is on PATH)

Some setups run it automatically. If not, you can run manually:

```bash
# Step 1: derive cleaned PlainXML geometry (no internal links)
$ netconvert --lefthand \
  --sidewalks.guess \
  --no-internal-links \
  --no-turnarounds.fringe true \
  --node-files 1-generated.nod.xml \
  --edge-files 1-generated.edg.xml \
  --plain-output-prefix 2-cooked

# Step 2: merge crossings/connections/signals into the final net
$ netconvert --lefthand \
  --node-files 2-cooked.nod.xml \
  --edge-files 2-cooked.edg.xml \
  --connection-files 1-generated.con.xml \
  --tllogic-files 1-generated.tll.xml \
  --output 3-assembled.net.xml
```

Open `3-assembled.net.xml` in **SUMO-GUI** or **netedit** to inspect.

---

## Demand-driven pedestrian flows

`v0.3.0` ships a first-class routed demand generator for both pedestrians
 (`personFlow`) and vehicles (`flow`). Supply signed endpoint demand and junction
 turning ratios for whichever modes you need; the converter merges the results
into a single `demandflow.rou.xml`. The feature is documented in
 **`docs/demand_personflow_spec.md`** and activated via the CLI:

```bash
$ python -m sumo_optimise.conversion.cli.main path/to/spec.json \
    --ped-endpoint-demand data/reference/ped_EP_demand_sampleUpd.csv \
    --ped-junction-turn-weight data/reference/ped_jct_turn_weight_sampleUpd.csv \
    --veh-endpoint-demand data/reference/veh_EP_demand_sampleUpd.csv \
    --veh-junction-turn-weight data/reference/veh_jct_turn_weight_sampleUpd.csv \
    --demand-sim-end 3600
```

Key points:

- `ped_EP_demand_sampleUpd.csv` declares the flow pattern on the first row
  (`Pattern,steady` / `poisson`), followed by the header
  row (`SidewalkEndID,PedFlow,Label`) and one endpoint per subsequent row with signed
  persons/hour volumes (positive = origin, negative = sink).
- Minor approaches expose separate east/west sidewalk endpoints:
  `PedEnd.Minor.{pos}.{N|S}_end.{E|W}_sidewalk`. Use these to balance
  approach-specific demand.
- West-side endpoints resolve to the northbound minor sidewalk (`Edge.Minor{N|S}.NB.{pos}`),
  while east-side endpoints resolve to the southbound sidewalk (`Edge.Minor{N|S}.SB.{pos}`),
  keeping the demand export aligned with the physical sidewalk placement.
- `ped_jct_turn_weight_sampleUpd.csv` provides raw weights for each direction/side
  combination; U-turn branches are suppressed automatically and the remainder
  re-normalised.
- Vehicle demand mirrors the pedestrian interface: `veh_EP_demand_sampleUpd.csv`
  lists signed endpoint flows (aliasing `Node.Main.E_end`/`W_end` is supported).
  `veh_jct_turn_weight_sampleUpd.csv` carries the 4-way turn ratios
  (`ToNorth|ToWest|ToSouth|ToEast`). When both pedestrian and vehicle data are
  provided the converter emits a single `routes` document containing `<personFlow>`
  and `<flow>` entries side by side.
- A NetworkX-backed pedestrian graph models sidewalks, crosswalks, and minor
  approaches. Each OD pair expands into one `<personFlow>` + `<personTrip>` in
  `plainXML_out/.../demandflow.rou.xml`. The configured `defaults.ped_endpoint_offset_m`
  (corridor JSON) is applied at the lane start for west-end north halves, east-end south halves,
  minor north east-side endpoints, and minor south west-side endpoints, and at
  `length - offset` for their opposite halves so that flows spawn and terminate
  at the physically correct sidewalk ends.
- The emitted `config.sumocfg` references the cooked net (`3-assembled.net.xml`) and
  the merged routes file so you can immediately launch simulations once the net
  exists (via the two-step `netconvert` or any other pipeline).
- Need placeholder spreadsheets? Add `--generate-demand-templates` to emit
  `template_ped_dem.csv`, `template_ped_turn.csv`, `template_veh_dem.csv`, and
  `template_veh_turn.csv` prefilled with the discovered endpoints/junctions.

### Choose the right CLI for your workflow

| Task | Entry point | Example |
| --- | --- | --- |
| Network build only | `python -m sumo_optimise.conversion.cli.network SPEC.json [flags]` | `python -m sumo_optimise.conversion.cli.network data/.../SUMO_OPTX_v1.4_sample.json --run-netconvert --run-netedit` |
| Demand build only (reusing an existing network) | `python -m sumo_optimise.conversion.cli.demand SPEC.json --ped-endpoint-demand ... --ped-junction-turn-weight ... --veh-endpoint-demand ... --veh-junction-turn-weight ... [--network-input path/to/3-assembled.net.xml] [--run-sumo-gui]` | `python -m sumo_optimise.conversion.cli.demand spec.json --ped-endpoint-demand ped.csv --ped-junction-turn-weight ped_turn.csv --veh-endpoint-demand veh.csv --veh-junction-turn-weight veh_turn.csv --network-input ./networks/base.net.xml --run-sumo-gui` |
| Full pipeline (network + demand) | `python -m sumo_optimise.conversion.cli.main SPEC.json [demand flags] [--run-netconvert] [--run-sumo-gui]` | `python -m sumo_optimise.conversion.cli.main spec.json --ped-endpoint-demand ped.csv --ped-junction-turn-weight ped_turn.csv --veh-endpoint-demand veh.csv --veh-junction-turn-weight veh_turn.csv --run-netconvert --run-netedit --run-sumo-gui` |

Newly added options:

* `--network-input PATH` — reuse an existing `net.xml` during demand runs (the file is copied into the output directory so you can iterate on routes without rebuilding the network).
* `--run-sumo-gui` — launch `sumo-gui -c config.sumocfg` once demand outputs are written (requires a network, either freshly built or provided via `--network-input`).
* Every long-form CLI flag also has a short alias (e.g. `-s` for `--schema`, `-c` for `--run-netconvert`, `-P` for `--ped-endpoint-demand`, etc.) so you can keep command lines concise.

See the spec for data schemas, propagation rules, and output semantics.

---

## Input specification (v1.4 overview)

* `version`: `"1.4"`
* `snap`: `{ "step_m": int>=1, "tie_break": "toward_west" | "toward_east" }`
* `defaults`: e.g., minor road length, crossing width, speed_kmh (with optional sidewalk width/endpoint offsets)
* `main_road`: `{ "length_m": number, "center_gap_m": number, "lanes": int }`
* `signal_profiles`: fixed-time profiles for `tee` / `cross` / `xwalk_midblock` (cycle, phases, allowed movements).
  * `cycle_s` must equal the sum of listed phase durations.
  * `yellow_duration_s` replaces the tail of the last continuous green stretch for a vehicle movement before it turns red. If the movement stays green across multiple phases, total their durations and treat the final `yellow_duration_s` seconds as yellow.
  * `ped_early_cutoff_s` (intersections only) shortens pedestrian clearance so pedestrians turn red `ped_early_cutoff_s` seconds before the next phase.
* `layout`: ordered events along the corridor. Intersections now carry their geometry inline—no more `junction_templates` section.

  * Shared geometry fields for `tee` and `cross`:
    * `main_approach_begin_m`: snapped distance upstream to start the lane override.
    * `main_approach_lanes`: lane count to apply within the override (0 keeps the corridor default).
    * `minor_lanes_approach`: lanes for minor traffic travelling **toward** the main road.
    * `minor_lanes_departure`: lanes for movements leaving the main road onto the minor leg.
    * `median_continuous`: whether the median stays closed through the junction (blocks main right + U turns, minor straight-throughs, etc.).
    * `lane_movements` (optional): manual lane-movement templates per approach (`main`/`EB`/`WB`/`minor`/`NB`/`SB`). Provide one or more arrays of lane labels per approach (labels combine `L`/`T`/`R`/`U`; template length = lane count). If no template matches the actual lane count, the automatic allocation runs as before.
  * `tee`: `{ pos_m, branch: "north"|"south", <geometry>, main_u_turn_allowed, refuge_island_on_main, signalized, two_stage_tll_control?, signal?, main_ped_crossing_placement }`
  * `cross`: `{ pos_m, <geometry>, main_u_turn_allowed, refuge_island_on_main, signalized, two_stage_tll_control?, signal?, main_ped_crossing_placement }`
  * `xwalk_midblock`: `{ pos_m, refuge_island_on_main, signalized, two_stage_tll_control?, signal? }`

    * `two_stage_tll_control` — required boolean present only when `signalized=true` **and** `refuge_island_on_main=true`.
    * `main_u_turn_allowed` — required boolean for intersections. `false` removes main-road U-turns in both directions; `true` keeps them.

**Rules (selected):**

* All positions round to the grid (`step_m`). Midpoints use `tie_break`.
* Events must lie within `[0, length]`; snapped positions within `[0, grid_max]`.
* A junction and a mid-block crossing at the **same** position will be **absorbed** into the junction’s crossing flags.
* `median_continuous=true` restricts certain turns (e.g., main-road right turn).
* Manual `lane_movements` drop impossible movements (e.g., blocked turn) but otherwise override the automatic allocation. Do not specify both `main` **and** `EB`/`WB` (or both `minor` **and** `NB`/`SB`) at the same junction.
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
$ python -m sumo_optimise.conversion.cli.main --help
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
  * `1-generated.con.xml` — `<connection>` for vehicle L/T/R/U movements with left turns mapped from the inside out, straight lanes paired left-to-left (overflow collapses into the rightmost target lane; extra target lanes remain unused), and right/U turns sharing the innermost target lane when approaches exceed targets; `<crossing>` for pedestrians (junction/min-block; single/split).
  * `1-generated.tll.xml` — `<tlLogic>` definitions assembled from the signal profiles referenced by each cluster.
* **Build artifacts**

  * `build.log` — structured logs (schema/semantic/IO/netconvert).
* `2-cooked.*` PlainXML and `3-assembled.net.xml` — if `netconvert` runs.

---

## Batch runner outputs (`results.csv`)

Columns are grouped for readability:

* **Scenario inputs:** `scenario_id`, `seed`, `scale`, `begin_filter`, `end_time`, `demand_dir`.
* **Trip stats:** `vehicle_count`, `person_count`, `vehicle_mean_timeLoss`, `person_mean_timeLoss`, `person_mean_routeLength`.
* **Queue durability:** `queue_threshold_steps`, `queue_threshold_length`, `queue_first_over_saturation_time`, `queue_is_durable`.
  * `queue_first_over_saturation_time` = first timestep where the waiting/running ratio stays at or above `queue_threshold_length` for at least `queue_threshold_steps` consecutive seconds; blank means durable (no over-saturation detected).
* **Scale probe metadata:** `scale_probe_enabled`, `scale_probe_max_durable_scale`, `scale_probe_attempts`.
* **Notes:** `fcd_note`, `error_note`.

Only the over-saturation timing is retained for queues; legacy waiting-threshold metrics and max waiting values have been removed.

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
