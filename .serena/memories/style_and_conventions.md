# Style and Conventions
- Python 3.11+ with type hints, `dataclasses`, and `Enum` classes for schema concepts.
- Keep pipeline layers pure/deterministic; no filesystem or subprocess I/O outside CLI/utils/sumo_integration adapters.
- Small, testable functions preferred over large classes; explicit inputs/outputs.
- Preserve deterministic ID/filename conventions defined in `builder.ids` and emitters.
- Respect semantic checks and error codes (e.g., `E101` range, `W201` warnings) when extending validation.
- Logging handled via `conversion/utils/logging.py`; emit actionable messages with category tags like `[VAL]`/`[SCH]`.
- JSON schema version pinned at v1.3; update schema and validators together if bumped.