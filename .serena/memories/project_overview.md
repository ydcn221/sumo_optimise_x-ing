# Project Overview
- Purpose: generate SUMO PlainXML (nodes, edges, connections, TLL) for a single straight main corridor with orthogonal minor roads and mid-block pedestrian crossings from a JSON spec (schema v1.3).
- Core flow: parse JSON → validate semantics → plan snapped geometry, lanes, crossings → emit PlainXML → optional netconvert/netedit integration.
- Key package: `src/sumo_optimise/conversion/` with submodules for parser, checks, planner, builder, emitters, sumo integration, domain models, utils, and pipeline orchestrator.
- Entry point: `python -m sumo_optimise.conversion.cli` invoking `pipeline.build_and_persist`.
- Data: embedded schema at `sumo_optimise/conversion/data/schema.json`; sample input under `data/reference/schema_v1.3_sample.json`.
- Tests: `pytest` suite under `tests/` exercising conversion pipeline pieces.