# SUMO Linear Corridor Network Converter

Generate a SUMO network (PlainXML) for a **single, straight main road** with orthogonal minor roads (tee/cross intersections) and mid-block pedestrian crossings from a **JSON specification (v1.3)**.
The converter **validates** the JSON, **plans** lanes and crossings, **emits** `net.nod.xml` / `net.edg.xml` / `net.con.xml`, and can **optionally run** `netconvert` to build a runnable `network.net.xml`.

---

## Scope and Network Model (assumptions and constraints)

This converter targets a **linear corridor** with controlled variability. The following items define the **intended geometry**, **traffic control**, and **explicit limits**.

### Geometry and coordinates

* The converter **constructs one main road** as a straight line laid **west → east**.

  * The converter **sets chainage 0 m at the west end** and **increases chainage to the east**.
  * The converter **uses meters** for all distances and **seconds** for all durations.
* The converter **represents opposing directions** as separate carriageways (EB and WB) with a **center gap**.

  * The JSON field `center_gap_m` **controls the lateral separation** between EB and WB.
* Minor roads **run strictly north/south** and intersect the main road at **right angles**.

  * Tee intersections include exactly one minor branch (`north` **or** `south`).
  * Cross intersections include **both** minor branches.
* The converter **snaps event positions** to a grid before building geometry.

  * The JSON `snap.step_m` **defines the grid step**.
  * The JSON `snap.tie_break` (**`toward_west`** or **`toward_east`**) **resolves midpoint ties**.

### Intersections and crossings

* At every intersection, the converter **always creates** the **east–west pedestrian crossing across the minor road** (mandatory crossing across the side street).
* The converter **optionally creates** pedestrian crossings **across the main road** on the **west** and/or **east** side of the intersection as requested by the event.

  * If a template requests a **split crossing** across the main road, the converter **creates a two-stage crossing** with a median refuge.
* The converter can **create mid-block crossings** along the main road using **dedicated events**.

  * Mid-block crossings may be **unsignalized** or **signalized** and may be **single** or **split** as defined by the template.

### Lanes, medians, and approach treatments

* The JSON defines **base lane counts** for EB and WB.
* A junction template may **override lane counts upstream of the node** over a specified length (approach region).

  * If `approach_main_lanes` is **0**, the converter **does not change** the base lane count on the approach.
  * The converter **splits edges** to apply lane changes deterministically within the approach region.
* If a template sets `median_continuous=true`, the converter **treats the center median as continuous through the intersection** and **restricts** movements that would cross it (e.g., certain right/left turns), by **omitting** such `<connection>` elements.

### Signals and coordination

* The converter **supports fixed-time signals only** (`type="static"`).

  * A signalized event **must reference** a valid `signal_profile`.
  * The sum of phase durations **must equal** the profile’s cycle length.
* The JSON may **define a coordination group** with a **cycle** and **base time**.

  * The converter **applies each event’s `offset_s`** relative to the group’s base to **phase-align** multiple controllers.

### Event model and precedence

* The JSON `layout` lists **events** along the main road (`tee`, `cross`, `xwalk_midblock`) with **positions in meters**.
* After snapping, **two or more events at the same position** are **resolved deterministically**:

  1. The converter **creates the intersection** (if any),
  2. then the converter **absorbs** a co-located mid-block crossing into the **intersection’s crossing flags**,
  3. then the converter **rejects any remaining duplicate mid-block crossing** at that position and **logs** the decision.

### Out of scope (not supported)

* **Curved main roads**, skewed intersections, or **non-orthogonal** approaches.
* **Roundabouts**, **slip lanes**, channelized islands other than median refuges for split crossings.
* **Actuated** or **traffic-responsive** signal control, priority rules, or detectors.
* **Bus lanes**, **cycle tracks**, or lane-level modal restrictions beyond standard `<allow>/<disallow>` usage.
* **Multi-corridor networks** or non-linear grids.

---

## Key capabilities

* **Schema-driven input (v1.3)** with draft-07 JSON Schema.
* **Deterministic grid snapping** with configurable step and tie-break.
* **Semantic validation** (value ranges, duplicate/near-duplicate events after snapping, template/profile consistency).
* **Template-based intersections** (tee/cross), **approach lane overrides**, **continuous median rules**.
* **Pedestrian crossings** at intersections and mid-block (single or split).
* **Vehicle connections** (L/T/R) with explicit and repeatable lane mapping:

  * **Left**: map from **inner lanes outward**.
  * **Through**: **left-aligned pairing**; fan out remaining target lanes from the right.
  * **Right** and **U-turns** (if legal): map to the **outermost applicable** lane(s).
* **Optional `netconvert` integration** with a clear three-step workflow and structured logs.

---

## Terminology (used consistently in this README)

* **Event**: a `tee`, `cross`, or `xwalk_midblock` entry in `layout` with a position in meters.
* **Approach region**: upstream segment on the main road where a junction template overrides lane count.
* **Split crossing**: a two-stage pedestrian crossing across the main road with a median refuge.
* **Absorb** (events): the converter **replaces** a redundant mid-block crossing with the **intersection’s** crossing configuration at the same position.

---

## Repository layout (summary)

```
sumo_optimise/
  conversion/
    cli/                 # CLI entry point
    parser/              # JSON/schema loading & parsing
    checks/              # Schema & semantic validation
    planner/             # Snapping, breakpoints, lanes, crossings
    emitters/            # XML writers (nodes, edges, connections)
    sumo_integration/    # netconvert / netedit wrappers
    domain/              # Dataclasses & enums
    utils/               # Logging, IO, constants, errors
    data/schema.json     # Embedded JSON Schema (v1.3)
    pipeline.py          # Orchestration
data/reference/           # Sample specifications (e.g., schema_v1.3_sample.json)
jsonschema/               # Local namespace (project-internal)
```

> Detailed, agent-oriented guidance (build steps, conventions, test hooks) lives in **`AGENTS.md`** to keep this README focused.

---

## Requirements

* **Python** 3.11 or 3.12 (3.8+ may work but is not primary).
* **SUMO** (optional for `netconvert`/`netedit`): ensure the executables are on `PATH`.

---

## Installation

Create and activate a virtual environment, then install in editable mode.

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

> This repository includes a **local** `jsonschema/` namespace used by the project. Avoid naming conflicts with the PyPI package.

---

## Quick start

1. **Prepare input JSON** (v1.3). Use a file of your own or a sample in `data/reference/`.
2. **Run the CLI** to build PlainXML (nodes/edges/connections).
3. **Optionally run `netconvert`** to produce `network.net.xml`.

**Minimal build**

```bash
# POSIX
$ python -m sumo_optimise.conversion.cli --input path/to/spec.json

# PowerShell
PS> python -m sumo_optimise.conversion.cli --input path\to\spec.json
```

**Default behavior**

* The converter **validates** against `sumo_optimise/conversion/data/schema.json` (v1.3).
* The converter **creates** a timestamped output directory under `plainXML_out/`.
* The converter **writes**:

  * `net.nod.xml` — nodes
  * `net.edg.xml` — edges
  * `net.con.xml` — vehicle connections and pedestrian crossings
  * `build.log` — structured log with schema/semantic/IO stages

---

## Build with `netconvert`

You can build a runnable SUMO network in three explicit steps. Replace `--lefthand` with `--no-lefthand` if you model right-hand traffic.

```bash
# Step 1: build a SUMO net from the PlainXML you emitted
$ netconvert --lefthand \
  --node-files net.nod.xml \
  --edge-files net.edg.xml \
  --sidewalks.guess \
  --output base_raw.net.xml

# Step 2: round-trip to PlainXML so SUMO computes geometry consistently
$ netconvert \
  --sumo-net-file base_raw.net.xml \
  --plain-output-prefix base_plain

# Step 3: merge your connection & crossing logic back in
$ netconvert --lefthand \
  --node-files base_plain.nod.xml \
  --edge-files base_plain.edg.xml \
  --connection-files net.con.xml \
  --output network.net.xml
```

Open `network.net.xml` in **SUMO-GUI** or **netedit**.

---

## Input specification (v1.3 overview)

* `version`: `"1.3"`
* `snap`: `{ "step_m": int>=1, "tie_break": "toward_west" | "toward_east" }`
* `defaults`: typical values include minor road length (m), crossing width (m), default speeds (km/h)
* `main_road`: `{ "length_m": number, "center_gap_m": number, "carriageways": { "EB": {...}, "WB": {...} } }`
* `junction_templates`: reusable definitions for `tee`/`cross` (approach length, lane overrides, `median_continuous`, `refuge_island_on_main`, `two_stage_ped_crossing_on_main`)
* `signal_profiles`: fixed-time profiles for intersections and mid-block signals (cycle, phases)

  * The converter **requires** `sum(phase.durations) == cycle_s`.
  * If defined, `yellow_duration_s` **shortens the effective green** for vehicles by the stated tail; the converter **places yellow** before red.
  * If defined (intersections only), `ped_red_offset_s` **ends pedestrian green earlier** by the stated offset.
* `layout`: ordered events along the main road:

  * `tee`: `{ pos_m, branch: "north"|"south", template, signalized, signal?, main_ped_crossing_placement }`
  * `cross`: `{ pos_m, template, signalized, signal?, main_ped_crossing_placement }`
  * `xwalk_midblock`: `{ pos_m, signalized, refuge_island_on_main, two_stage_ped_crossing_on_main }`

**Rules (converter enforces):**

* The converter **snaps all `pos_m`** to the grid and **sorts** events by position.
* Events must lie within `[0, main_road.length_m]` after snapping.
* At a shared position, the converter **builds the intersection** and **absorbs** the mid-block crossing into its flags.
* If `signalized=true`, the event **must** reference a valid `signal_profile`.

---

## How the converter maps JSON → PlainXML (deterministic behavior)

### Nodes and edges

* The converter **creates** main road endpoints at `x=0` and `x=length_m` and **inserts** interior nodes at each event position.
* The converter **splits** the main road into sub-edges between consecutive nodes.
* The converter **creates** minor road stubs for all present branches.
* The converter **applies** base lane counts to EB/WB and **overrides** them within approach regions defined by the chosen junction template.

### Connections and restrictions

* The converter **emits** `<connection>` elements for legal movements only.

  * If `median_continuous=true`, the converter **omits** movements that cross the median.
  * The converter **maps lanes** left-to-left for through flows; it **fans out** excess target lanes from the right; it **uses outer lanes** for right/U turns when legal.

### Crossings and walking areas

* The converter **creates** `<crossing>` elements at intersections according to placement flags and **always** across the side street.
* For split crossings across the main road, the converter **creates two crossings** with a **median refuge**.

### Signals

* The converter **instantiates** `<tlLogic type="static">` per signalized event and **derives state strings** from the profile’s phases and the intersection’s set of controlled links.
* The converter **uses** coordination `cycle_s`/`base_time_s` and event `offset_s` to **align controllers**.

---

## CLI usage

```bash
$ python -m sumo_optimise.conversion.cli --help
```

Common flags:

* `--input PATH` — path to the v1.3 JSON (required)
* `--schema PATH` — override schema path (defaults to packaged `data/schema.json`)
* `--out DIR` — output directory root (default `plainXML_out/`)
* `--skip-netconvert` — do not call `netconvert`
* `--keep-output` — keep intermediate files on failure
* `--lefthand/--no-lefthand` — pass through to `netconvert` (default `--lefthand`; set explicitly if you require right-hand traffic)

---

## Outputs

**PlainXML**

* `net.nod.xml` — endpoints, interior event nodes, minor dead-ends
* `net.edg.xml` — EB/WB edges split at events; approach lane overrides; speeds in m/s
* `net.con.xml` — `<connection>` for vehicle movements and `<crossing>` for pedestrians

**Build artifacts**

* `build.log` — schema errors (`[SCH]`), semantic errors (`[VAL]`), IO and `netconvert` steps
* `base_raw.net.xml`, `base_plain.*`, `network.net.xml` — if you run the 3-step `netconvert` workflow

---

## Validation and error handling

The converter **stops** on schema errors and **reports** the failing JSON pointer.
The converter **continues** on recoverable semantic issues and **logs** the action.

Examples:

* **Out-of-range position** after snapping → **error** with position and event index.
* **Duplicate event** at same snapped position → **warning**; mid-block crossing **absorbed** into intersection.
* **Signalized event** without a valid profile → **error** with the missing profile ID.
* **Approach length** that overlaps beyond corridor start → **error** with offending node and length.

---

## Troubleshooting

* **`netconvert` not found**: install SUMO and ensure `netconvert` is on `PATH`; otherwise use the PlainXML outputs directly.
* **Windows path issues**: prefer absolute paths or PowerShell `${PWD}`; avoid mixing shells within one build.
* **Validation fails**: read `build.log`; look for `[SCH]` (schema) and `[VAL]` (semantic) entries; each entry **names** the object, the **field**, and the **required fix**.

---

## Roadmap

* Reusable `<tlLogic>` libraries and explicit movement→phase mapping tables.
* Extended tests for **XML equivalence** under geometry round-trip.
* Optional exporters for **routes** and **pedestrian flows** aligned with crossing inventory.

---

## Contributing

* Open issues for bugs and feature requests.
* Keep changes modular and update both **schema** and **semantic checks** when you change input fields.
* If you add build conventions or test hooks, **document them** in `AGENTS.md` and reference them from this README.
