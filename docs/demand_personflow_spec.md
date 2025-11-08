# Person Flow Demand Generation

This document summarises the design implemented for converting endpoint demand
and junction turn weights into SUMO `personFlow` definitions.

## Inputs

- **Endpoint demand CSV (`DemandPerEndpoint.csv`)**
  - Encoding: `utf-8-sig`
  - Row 1 declares the flow pattern: `Pattern,persons_per_hour` (or `period`, `poisson`)
  - Row 2 is the header: `SidewalkEndID`, `PedFlow`, optional `Label`
  - `PedFlow` is signed (`+` origin, `-` sink) in persons/hour
  - Rows are processed independently and never aggregated

- **Junction turn-weight CSV (`JunctionTurnWeight.csv`)**
  - Encoding: `utf-8-sig`
  - Columns: `JunctionID`, `ToNorth_EastSide`, `ToNorth_WestSide`,
    `ToWest_NorthSide`, `ToWest_SouthSide`, `ToSouth_WestSide`,
    `ToSouth_EastSide`, `ToEast_SouthSide`, `ToEast_NorthSide`
  - Weights are raw values (not automatically normalised)
  - U-turn branches (`To{Same}`) are zeroed at runtime and the remaining
    weights are re-normalised per visit

## Processing pipeline

1. **CSV parsing** – structural validation, numeric conversion, and aggregation
   of errors using the existing `DemandValidationError` hierarchy.
2. **Pedestrian graph building** – creates a NetworkX undirected multigraph
   representing main-road sidewalks, crosswalks, and minor approaches. Nodes
   carry coordinates, breakpoint metadata, and endpoint flags.
3. **Flow propagation** – each demand row is expanded independently using a
   breadth-first walk over the graph. At junction nodes the provided turn weights are
   applied with U-turn suppression; at simple nodes flux continues straight.
   Negative (sink) rows are propagated as positive sources and flipped at the
   end.
4. **Route emission** – each OD pair becomes a `<personFlow>` with a matching
   `<personTrip>`. Supported patterns: `personsPerHour`, fixed `period`, or
   Poisson arrival (`period="exp(λ)"`). The configured `defaults.ped_endpoint_offset_m`
   applies relative to the correct end of the lane: start-side endpoints
   (main-road north halves at the west terminus, main-road south halves at the
   east terminus, minor north east-side endpoints, minor south west-side
   endpoints) use `departPos/arrivalPos = offset`, while end-side endpoints
   (main-road south halves at the west terminus, main-road north halves at the
   east terminus, minor north west-side endpoints, minor south east-side
   endpoints) use `departPos/arrivalPos = length - offset`.

## Output

- Routes are written to `plainXML_out/.../demandflow.rou.xml`
- The document is valid against `routes_file.xsd` and uses `begin="0"`
  / `end="T"` from the CLI options.

## CLI options

- `--ped-endpoint-demand` – path to the pedestrian endpoint demand CSV
- `--ped-junction-turn-weight` – path to the pedestrian turn-weight CSV
- `--veh-endpoint-demand` / `--veh-junction-turn-weight` – reserved for the upcoming vehicle flow inputs
- `--demand-sim-end` – simulation end time (seconds) shared by pedestrian and future vehicle flows
- `--generate-demand-templates` – emit blank CSVs with all known IDs for rapid
  spreadsheet preparation (`DemandPerEndpoint_template.csv` and `JunctionTurnWeight_template.csv`)
- Endpoint IDs distinguish sidewalk sides along both mainline and minor approaches:
  - Mainline endpoints follow `PedEnd.Main.{E|W}_end.{N|S}_sidewalk`. The `E_end` / `W_end`
    tokens map to the extreme main breakpoints, allowing CSV authors to speak in cardinal halves
    instead of raw positions. No other `PedEnd.Main` spellings are considered valid.
  - Minor approaches use `PedEnd.Minor.{pos}.{N|S}_end.{E|W}_sidewalk`. This keeps the demand rows
    aligned with the side-specific junction turn-weight entries. West-side endpoints resolve to the
    northbound minor sidewalk (`Edge.Minor.{N|S}.NB.{pos}`) and east-side endpoints resolve to the
    southbound sidewalk (`Edge.Minor.{N|S}.SB.{pos}`), so exported routes stay consistent with the
    physical sidewalk layout. Likewise, the `.N_end` / `.S_end` and `.E_sidewalk` / `.W_sidewalk`
    tokens are the only supported combinations.

Both CSV options are required to activate the demand pipeline.

- The spawn/arrival offset is defined within the corridor JSON (`defaults.ped_endpoint_offset_m`).
- The flow pattern must be declared in the first row of the endpoint CSV; the CLI no longer accepts
  `--demand-pattern`.

## Vehicle demand overview

- `veh_EP_demand_sampleUpd.csv` follows the same two-row preamble as the pedestrian
  dataset (`Pattern,<value>` then `EndID,vehFlow,Label`). Signed values are supported and the same
  aliasing rules apply (`Node.Main.E_end` / `Node.Main.W_end` resolve to the proper corridor nodes).
- `veh_jct_turn_weight_sampleUpd.csv` lists four columns per junction: `ToNorth`, `ToWest`, `ToSouth`,
  `ToEast`. U-turn shares are automatically zeroed during propagation.
- When vehicle inputs are supplied alongside pedestrian CSVs the converter merges both modalities into
  a single routes document (and writes `config.sumocfg` referencing the standard net + routes files).

## File naming

- The generated routes file is emitted as `demandflow.rou.xml` (mirroring the default manifest entry)
  and is referenced in manifests for downstream tooling.
