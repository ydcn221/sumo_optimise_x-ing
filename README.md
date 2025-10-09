# SUMO PlainXML Converter

A cleaned up Python package that converts corridor JSON specifications into SUMO PlainXML artefacts. The
repository now ships with a modern `src/` layout and a clear separation between the maintained
implementation and the backwards-compatible legacy bridge.

## Package layout

```
src/sumo_optimise/
    conversion/   # Modern, fully modular converter implementation
    legacy/       # Wrapper around the legacy v1.2.11 converter
```

The `sumo_optimise.conversion` package exposes the public API for building PlainXML artefacts, while
`sumo_optimise.legacy` contains an opt-in compatibility layer for invoking the original
`plainXML_converter_0927_1.2.11.py` script from Python.

## Running the converter

```bash
pip install -e .[test]
python -m sumo_optimise.conversion.cli.main path/to/spec.json --schema path/to/schema.json
```

The legacy bridge can be executed via:

```bash
python -m sumo_optimise.legacy.cli path/to/spec.json --output-dir out/
```

## Tests

All regression tests that rely on the legacy converter now live under `tests/legacy/`. Removing that
single directory will skip the compatibility suite altogether. The remaining tests (if any) continue
working without touching the legacy assets.
