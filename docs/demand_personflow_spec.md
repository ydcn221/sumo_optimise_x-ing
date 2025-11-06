# Person Flow Demand Generation

This document summarises the design implemented for converting endpoint demand
and junction turning ratios into SUMO `personFlow` definitions.

## Inputs

- **Endpoint demand CSV (`DemandPerEndpoint.csv`)**
  - Encoding: `utf-8-sig`
  - Columns: `EndpointID`, `PedFlow`, optional `Label`
  - `PedFlow` is signed (`+` origin, `-` sink) in persons/hour
  - Rows are processed independently and never aggregated

- **Junction ratio CSV (`JunctionDirectionRatio.csv`)**
  - Encoding: `utf-8-sig`
  - Columns: `JunctionID`, `ToNorth_EastSide`, `ToNorth_WestSide`,
    `ToWest_NorthSide`, `ToWest_SouthSide`, `ToSouth_WestSide`,
    `ToSouth_EastSide`, `ToEast_SouthSide`, `ToEast_NorthSide`
  - Ratios are raw weights (not automatically normalised)
  - U-turn branches (`To{Same}`) are zeroed at runtime and the remaining
    weights are re-normalised per visit

## Processing pipeline

1. **CSV parsing** – structural validation, numeric conversion, and aggregation
   of errors using the existing `DemandValidationError` hierarchy.
2. **Pedestrian graph building** – creates a NetworkX undirected multigraph
   representing main-road sidewalks, crosswalks, and minor approaches. Nodes
   carry coordinates, breakpoint metadata, and endpoint flags.
3. **Flow propagation** – each demand row is expanded independently using a
   breadth-first walk over the graph. At junction nodes the provided ratio is
   applied with U-turn suppression; at simple nodes flux continues straight.
   Negative (sink) rows are propagated as positive sources and flipped at the
   end.
4. **Route emission** – each OD pair becomes a `<personFlow>` with a matching
   `<personTrip>`. Supported patterns: `personsPerHour`, fixed `period`, or
   Poisson arrival (`period="exp(λ)"`). `departPos`/`arrivalPos` default to
   0.10 m from the start/end of the chosen edge.

## Output

- Routes are written to `plainXML_out/.../1-generated.rou.xml`
- The document is valid against `routes_file.xsd` and uses `begin="0"`
  / `end="T"` from the CLI options.

## CLI options

- `--demand-endpoints` – path to the endpoint demand CSV
- `--demand-junctions` – path to the junction ratio CSV
- `--demand-pattern {persons_per_hour,period,poisson}`
- `--demand-sim-end` – simulation end time (seconds)
- `--demand-endpoint-offset` – spawn/arrival offset (metres)
- `--generate-demand-templates` – emit blank CSVs with all known IDs for rapid
  spreadsheet preparation
- Endpoint IDs distinguish sidewalk sides on minor approaches:
  `Node.{pos}.MinorNEndpoint.{EastSide|WestSide}` and
  `Node.{pos}.MinorSEndpoint.{EastSide|WestSide}`. This allows demand to target
  the specific entrance side used in the junction ratio CSV. West-side endpoints
  resolve to the northbound minor sidewalk (`Edge.Minor{N|S}.NB.{pos}`) and
  east-side endpoints resolve to the southbound sidewalk (`Edge.Minor{N|S}.SB.{pos}`),
  so exported routes stay consistent with the physical sidewalk layout.

Both CSV options are required to activate the demand pipeline.

## File naming

- The generated routes file follows the same numbering convention as the other
  PlainXML artefacts and is referenced in manifests for downstream tooling.
