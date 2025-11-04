# Suggested Commands
- `python -m pip install --upgrade pip && python -m pip install -e .` – set up editable install.
- `python -m pip install jsonschema` – ensure Draft-07 validator dependency.
- `python -m sumo_optimise.conversion.cli --input data/reference/schema_v1.3_sample.json` – run corridor build against bundled sample.
- `python -m sumo_optimise.conversion.cli --input <spec.json> --run-netconvert` – emit PlainXML and call two-step netconvert.
- `pytest -q` – execute unit tests.
- `netconvert --lefthand --node-files 1-generated.nod.xml --edge-files 1-generated.edg.xml --plain-output-prefix 2-cooked` (plus follow-up command) – manual two-step SUMO integration when needed.
- `ls`, `rg '<pattern>' -n`, `python -m pytest tests/conversion/...` – common Linux utilities for navigation, search, and focused tests.