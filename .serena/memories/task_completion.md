# Task Completion Checklist
- Run `pytest -q` (or targeted modules) to confirm unit coverage after changes.
- Execute a smoke build with `python -m sumo_optimise.conversion.cli --input data/reference/schema_v1.3_sample.json` when relevant and inspect `plainXML_out/.../build.log` for `[ERROR]` entries.
- Verify generated PlainXML files (`1-generated.*.xml`) and manifest paths are deterministic for identical inputs.
- If netconvert behavior is touched, rerun the two-step commands and confirm `3-n+e+c+t.net.xml` integrity.
- Review console/log output to ensure new warnings/errors use existing taxonomy and informative wording.