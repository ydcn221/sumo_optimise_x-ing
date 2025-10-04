# SUMO PlainXML Converter (JSON v1.2)

## Overview
- 自前 JSON 仕様 (v1.2) から SUMO PlainXML (nodes/edges/connections) を生成するツール。

## Getting Started
1. Python 3.11+ を用意
2. `pip install -r requirements.txt`（必要に応じて）
3. `python plainXML_converter.py --input path/to/input.json --out outdir/`

## Repository Layout
- `parser/` `planner/` `builder/` `emitters/` `sumo_integration/` など（将来分割予定）
- `tests/`：ゴールデン比較とユニットテスト

## Notes
- Windows 11 前提
- CSV を出力する場合のエンコーディングは `utf-8-sig`
