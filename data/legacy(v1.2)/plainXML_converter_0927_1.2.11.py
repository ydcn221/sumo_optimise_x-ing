"""Changelog
=========
- ver 1.2.11 (2025-10-01)
  - connections.xml に車両向け <connection> を出力。
    - 各 Cluster.Main.{pos} で流入アプローチ（Main.EB, Main.WB, Minor.N.to, Minor.S.to）を列挙し、L/T/R の行先エッジを確定。
    - レーン数差は動作別の決定的割当で対応（左折=左端起点、直進=左詰め＋右端扇出、右折=右端起点）。
    - index=0 は歩道、車道は 1..本数（s, l, t, r）で固定。fromLane/toLane には 1..本数を使用。
    - median_continuous による禁止を強制：主道路流入は右折禁止、従道路流入は直進・右折禁止。
    - T 字路や端点による行先不存在はレーン本数 0 として扱う。
    - 生成結果が 0 本となる異常を追加検査：E401/E402/E405 を出力。
  - 関数の入出力整理：
    - emit_connections_with_crossings(...) の引数に main_road / lane_overrides を追加し、歩行者 <crossing> と車両 <connection> を同ファイルに出力。

- ver 1.2.10 (2025-09-29)
  - 交差点×単路横断の同位置規則:
    - validate_semantics(): E108 を追加。丸め後に同一 pos に junction と xwalk_midblock が共存する場合、
      midblock を交差点の主横断設置要求として「吸収」する。ただし、吸収側が既に交差点で設置済みなら重複とみなしてエラー。
      衝突側（West/East）は raw と snap の比較、および tie_break で決定。
    - emit_connections_with_crossings(): 吸収ロジックを反映。吸収が発生した pos の midblock <crossing> 出力を抑止し、
      交差点側の west/east フラグに統合することで二重 <crossing> を防止。
  - 検証: junction×midblock の同位置時に両側追加が可能なケースで E106 を発火させないように整理。
    - E106: junction が存在する pos の midblock は E106 判定対象から除外（junction 同居は E108 で扱う）。
    - E108: 拡張。以下を検出してエラー化。
      (a) 同位置衝突で同側に 2 本以上の midblock（側別重複）
      (b) 非衝突でも「近接」（±step_m, ±2·step_m の snapped 位置）で同側に 2 本以上の midblock
          例: junction=1000, midblock=1001,999,998 → West 側の近接二重に該当しエラー。

- ver 1.2.09 (2025-09-29)
  - 検証の一貫性:
    - スキーマ integer 指定項目を整数として受理・保持（defaults.minor_road_length_m / speed_kmh、junctionTemplate.main_approach_begin_m 等）。
    - schema 検証と semantic 検証での範囲上限を grid_max に統一（既存方針を徹底）。
    - 列挙のコード内再定義（Literal 等）を撤廃し、スキーマ主導に一本化。
  - テンプレID重複:
    - tee/cross 横断でのテンプレート ID 重複（同集合内・集合間）を検出してエラー化（全件列挙）。
  - 信号プロファイル検査:
    - phases.duration_s の総和と cycle_s の厳密一致を検査。
    - allow_movements の語彙検査を追加（正規表現に合致しない値を検出）。
    - layout.signal.profile_id の存在検証と種別整合（tee/cross/xwalk_midblock）を追加。

- ver 1.2.08 (2025-09-29)
  - スナップ格子の上限を統一:
    - grid_max = floor(length_m / step_m) * step_m を導入し、範囲・端点・ブレークポイント・検証の上限を統一。
    - これに伴い、length の四捨五入（length_int）依存を廃止。
  - 位置の範囲判定を修正:
    - parse_layout_events で 0 ≤ pos_snapped ≤ grid_max に変更（例: length=100.6, step=5 で pos=105 を正しく除外）。
  - アプローチ距離の格子化:
    - main_approach_begin_m を snap_distance_to_step(d, step) で格子倍数へ丸め、上書き区間境界を常に格子上に配置。
  - ブレークポイント・端点処理の整合:
    - collect_breakpoints_and_reasons / emit_nodes_xml / validate_semantics で端点= {0, grid_max} を使用。
  - ログ:
    - main_road 読み込み後に grid_max を INFO 出力。距離スナップ結果も必要箇所で INFO 出力。

- ver 1.2.07 (2025-09-29)
  - ロギング: ルートの basicConfig を廃止。専用ロガー `sumo_linear_corridor` を導入し、
    configure_logger() でハンドラを明示管理（重複防止・外部設定の上書き回避）。
  - 例外: 統一的な例外階層（BuildError 派生）を定義し、各処理で意味に応じて送出。
    main() は種別ごとに捕捉し、メッセージを区別して出力。
  - I/O: ensure_output_directory() は出力先作成のみを担当（ログ初期化は行わない）。
  - 実行: netconvert 失敗時に NetconvertExecutionError を送出（ログへ詳細を残す）。

- ver 1.2.06 (2025-09-28)
  - 検証: tee/cross を衝突判定で同一カテゴリ "junction" とみなし、同位置の重複を E106 として検出・中断。
  - 入力: snap.step_m を整数(>=1)に限定。防御的に整数化し、非整数は警告または例外。
  - 修正: layout 読み取り時の template_id で None が "None" 文字列化する不具合を是正。
  - 型: SnapRule.step_m を int に変更。round_position の引数も int へ。
  - schemaの修正に対応：`"step_m": { "type": "integer", "minimum": 1, "description": "丸め単位[m]" }`とする
  
- ver 1.2.05 (2025-09-28)
  - 意味検証（semantic validation）を追加（prefix: [VAL]）。
    - E101/E102: main_road 区間外のイベント（raw/snap 後）を検出し全件列挙→ビルド中断。
    - E103: junction template 未指定/未定義。
    - E104: tee の branch 未指定/不正。
    - E105: signalized と signal の有無の不整合。
    - E106: 同タイプ同位置（丸め後 pos）イベントの重複。
    - W201: 丸め後 pos が端点（0 / length）。ビルドは続行。
  - 既存の schema 検証→バージョン確認の後、semantic validation を挿入。

- ver 1.2.04 (2025-09-28)
  - JSON Schema 検証を追加（Draft-07, パス: schema_v1.2.json）。
  - スキーマ不適合は全件列挙してログ出力し、ビルドを中断（fail-fast）。
  - 検証ログは JSON パス／エラーメッセージ／validator／schema_path を表示。
  - ライブラリ未導入・スキーマ欠如も明確化（[SCH] プレフィックスで警告→例外）。

- ver 1.2.03 (2025-09-28)
  - スキーマ v1.2 準拠の 1.2.02 を機能拡張し、命名とコメントを整理。
  - アプローチ区間のレーン数上書き（junction_templates.*.main_approach_*）を実装。
    - 区間境界（start/end）をブレークポイントとして追加し、edges を分割。
    - 重複する上書き区間では **最大レーン数**を採用。
    - ブレークポイントの**理由タグ**（junction / xwalk_midblock / lane_change / endpoint）を保持。
  - すべてのブレークポイント（端点を除く）で EB/WB を `<join>` し、`Cluster.Main.{pos}` を生成。
  - connections に **<crossing> を出力**：
    - 交差点：従道路（minor）横断を常設。
    - 交差点：主道路（main）横断を `main_ped_crossing_placement.west/east` に従って設置。
      - テンプレートの `split_ped_crossing_on_main` が真のとき、EB/WB で**分割**出力。
    - 単路横断（xwalk_midblock）：イベントの `split_ped_crossing_on_main` に従って 1 本 or EB/WB 分割。
      - **どちら側のエッジに接続するかは `snap.tie_break` に従う**（優先側が無い場合は反対側へフォールバック）。
    - crossing の `node` は `Cluster.Main.{pos}`（2 段 netconvert 前提）。
  - **2 段 netconvert** を自動実行（PATH 非検出時は警告してスキップ）。
  - 変数名・関数名を平易化（共同開発を想定）、関数冒頭に仕様コメントを整備。
  - 出力ディレクトリの連番表記を 3 桁ゼロ埋めに統一。
  - 備考：レーン上書き・crossing 出力・2 段ビルドの考え方は、旧実装 plainxml_generator_0906_ver2.1 の内容を スキーマv1.2 仕様へ**適合移植**。

- ver 1.2.02 (2025-09-28)
  - **スキーマ v1.2 対応の最小実装** (plainxml_generator_0906_ver1.3.py をv1.2に対応)：
    - `main_road.lanes/speed_kmh` を EB/WB 共通値として解釈。
    - `junction_templates` を `tee` / `cross` 別集合として読み込み。
    - `layout` の `tee / cross / xwalk_midblock` をパースし、`snap` を適用後に `pos_m` でクラスタ化。
    - **nodes/edges のみ**出力。交差点位置で EB/WB を `<join>`（`Cluster.Main.{pos}`）。

- ver 1.2.01 (2025-09-28)
  - 環境構築
  - version id: <schema version>.<serial number>
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import datetime
import itertools
import json
import logging
import math
import re
from typing import Dict, List, Tuple, Optional, Set
import subprocess
import shutil
from jsonschema import Draft7Validator  # type: ignore

# =================
# ロガー（専用）
# =================
LOGGER_NAME = "sumo_linear_corridor"
LOG = logging.getLogger(LOGGER_NAME)

def configure_logger(log_path: Path, console: bool = True, level: int = logging.INFO) -> logging.Logger:
    """
    専用ロガーを構成する。ルートロガーは変更しない。
    - 既存ハンドラを全削除（重複防止）
    - FileHandler(utf-8) と（任意で）StreamHandler を追加
    - propagate=False（外部設定を汚染しない）
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False

    # 既存ハンドラ除去
    for h in list(logger.handlers):
        try:
            h.flush()
            h.close()
        except Exception:
            pass
        logger.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    if console:
        sh = logging.StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    logger.info("=== build start ===")
    logger.info("log file: %s", log_path.resolve())
    return logger

# ================
# 設定（固定前提）
# ================
INPUT_JSON_PATH = Path("v1.2.check.json")
SCHEMA_JSON_PATH = Path("schema_v1.2.json")
OUTPUT_DIR_PREFIX = "plainXML_out"
DATE_DIR_FORMAT = "%m%d"

# ================
# 例外階層
# ================
class BuildError(Exception):
    """ビルド処理における基底例外。"""

class SpecFileNotFound(BuildError):
    pass

class SchemaFileNotFound(BuildError):
    pass

class UnsupportedVersionError(BuildError):
    pass

class SchemaValidationError(BuildError):
    pass

class SemanticValidationError(BuildError):
    pass

class InvalidConfigurationError(BuildError):
    pass

class NetconvertExecutionError(BuildError):
    pass

# ================
# データ構造
# ================
@dataclass(frozen=True)
class SnapRule:
    step_m: int
    tie_break: str  # "toward_west" / "toward_east"（列挙はスキーマに依存）

@dataclass(frozen=True)
class Defaults:
    minor_road_length_m: int   # ← integer（スキーマ準拠）
    ped_crossing_width_m: float
    speed_kmh: int             # ← integer（スキーマ準拠）
    sidewalk_width_m: Optional[float] = None

@dataclass(frozen=True)
class MainRoadConfig:
    length_m: float
    center_gap_m: float
    lanes: int

@dataclass(frozen=True)
class JunctionTemplate:
    id: str
    main_approach_begin_m: int   # ← integer（スキーマ準拠、距離量）
    main_approach_lanes: int
    minor_lanes_to_main: int
    minor_lanes_from_main: int
    split_ped_crossing_on_main: bool
    median_continuous: bool
    kind: str                    # "tee" / "cross"

@dataclass(frozen=True)
class SignalRef:
    profile_id: str
    offset_s: int

@dataclass(frozen=True)
class LayoutEvent:
    type: str                   # "tee" / "cross" / "xwalk_midblock"
    pos_m_raw: float
    pos_m: int                  # snapped
    template_id: Optional[str] = None
    signalized: Optional[bool] = None
    signal: Optional[SignalRef] = None
    main_ped_crossing_placement: Optional[Dict[str, bool]] = None
    branch: Optional[str] = None  # "north"/"south"
    split_ped_crossing_on_main: Optional[bool] = None

@dataclass
class Cluster:
    """丸め後 pos_m ごとのイベント集合（join/Cluster の単位）。"""
    pos_m: int
    events: List[LayoutEvent]

@dataclass
class BuildArtifacts:
    """出力ファイルのパス群。"""
    outdir: Path
    log_path: Path
    nodes_path: Path
    edges_path: Path
    connections_path: Path

@dataclass(frozen=True)
class LaneOverride:
    """主道路アプローチ区間のレーン上書き情報。"""
    start: int
    end: int
    lanes: int

@dataclass(frozen=True)
class BreakpointInfo:
    """ブレークポイントに付与する理由タグ。"""
    pos: int
    reasons: Set[str]  # {"junction","xwalk_midblock","lane_change","endpoint"}

# --- 信号プロファイル（新設） ---
@dataclass(frozen=True)
class SignalPhaseDef:
    name: str
    duration_s: int
    allow_movements: List[str]

@dataclass(frozen=True)
class SignalProfileDef:
    id: str
    cycle_s: int
    phases: List[SignalPhaseDef]
    kind: str  # "tee" / "cross" / "xwalk_midblock"

MOVEMENT_RE = re.compile(r"^(pedestrian|(?:main|minor)_(?:L|T|R))$")

# ========================
# ユーティリティ
# ========================
def ensure_output_directory() -> BuildArtifacts:
    """
    出力先 plainXML_out/%m%d_### を作成する。
    既存の同日ディレクトリを走査し、連番を +1 する。
    ログの初期化はここでは行わない（外部アプリのロガー設定を汚染しない）。
    """
    today_str = datetime.datetime.now().strftime(DATE_DIR_FORMAT)
    base_dir = Path(OUTPUT_DIR_PREFIX)
    base_dir.mkdir(parents=True, exist_ok=True)

    pattern = re.compile(rf"^{re.escape(today_str)}_(\d+)$")
    max_seq = 0
    for entry in base_dir.iterdir():
        if entry.is_dir():
            m = pattern.match(entry.name)
            if m:
                max_seq = max(max_seq, int(m.group(1)))
    outdir = base_dir / f"{today_str}_{max_seq + 1:03}"
    outdir.mkdir(parents=True, exist_ok=False)

    log_path = outdir / "build.log"
    nodes_path = outdir / "net.nod.xml"
    edges_path = outdir / "net.edg.xml"
    connections_path = outdir / "net.con.xml"

    return BuildArtifacts(outdir, log_path, nodes_path, edges_path, connections_path)

def run_two_step_netconvert(artifacts: BuildArtifacts) -> None:
    """
    netconvert を 2 段で実行する。
      1) nod+edg → base.net.xml（--lefthand --sidewalks.guess）
      2) base.net.xml + con → network.net.xml（--lefthand）
    netconvert が PATH に無い場合は警告して終了する。
    失敗時は NetconvertExecutionError を送出する。
    """
    exe = shutil.which("netconvert")
    if exe is None:
        LOG.warning("netconvert not found in PATH. Skip two-step conversion.")
        return

    step1 = [
        exe, "--node-files", artifacts.nodes_path.name,
        "--edge-files", artifacts.edges_path.name,
        "--lefthand", "--sidewalks.guess",
        "--output-file", "base.net.xml",
    ]
    step2 = [
        exe, "--sumo-net-file", "base.net.xml",
        "--connection-files", artifacts.connections_path.name,
        "--lefthand",
        "--output-file", "network.net.xml",
    ]

    for idx, cmd in enumerate((step1, step2), start=1):
        LOG.info("netconvert step %d: %s", idx, " ".join(cmd))
        try:
            proc = subprocess.run(
                cmd, cwd=str(artifacts.outdir),
                capture_output=True, text=True, check=True, encoding="utf-8"
            )
            LOG.info("[netconvert %d] rc=%d", idx, proc.returncode)
            if proc.stdout:
                LOG.info("[netconvert %d STDOUT]\n%s", idx, proc.stdout)
            if proc.stderr:
                LOG.info("[netconvert %d STDERR]\n%s", idx, proc.stderr)
        except subprocess.CalledProcessError as e:
            if e.stdout:
                LOG.error("[netconvert %d STDOUT]\n%s", idx, e.stdout)
            if e.stderr:
                LOG.error("[netconvert %d STDERR]\n%s", idx, e.stderr)
            raise NetconvertExecutionError(f"netconvert step {idx} failed with rc={e.returncode}")

def load_json_file(json_path: Path) -> Dict:
    """JSON を読み込む。ファイルが無ければ SpecFileNotFound。"""
    if not json_path.exists():
        raise SpecFileNotFound(f"JSON not found: {json_path}")
    with json_path.open("r", encoding="utf-8") as f:
        spec_json = json.load(f)
    LOG.info("loaded JSON: %s", json_path)
    return spec_json

def kmh_to_mps(speed_kmh: float) -> float:
    """km/h を m/s に変換する。"""
    return speed_kmh / 3.6

def round_position(value_m: float, step_m: int, tie_break: str) -> int:
    """
    pos_m を step_m 単位で丸め、整数[m]を返す。中間は tie_break に従う。
    例：step=5; 13→15, 12.5→toward_west なら 10。
    """
    if step_m <= 0:
        # step 無効時は実数を最も近い整数に丸めて返す（防御）
        return int(round(value_m))
    q = value_m / float(step_m)
    lo = math.floor(q) * step_m
    hi = math.ceil(q) * step_m
    dl, du = abs(value_m - lo), abs(hi - value_m)
    if dl < du:
        return int(lo)
    if dl > du:
        return int(hi)
    return int(lo if tie_break == "toward_west" else hi)

def grid_upper_bound(length_m: float, step_m: int) -> int:
    """
    主道路長さ length_m と格子 step_m から、到達可能な最大位置（離散端点）grid_max を返す。
    grid_max = floor(length_m / step_m) * step_m
    """
    if step_m <= 0:
        return int(math.floor(length_m))
    return int(math.floor(length_m / float(step_m)) * step_m)

def snap_distance_to_step(distance_m: float, step_m: int) -> int:
    """
    上流距離などの「距離量」を格子倍数へ丸める。tie_break は用いない（距離には方向が無いため）。
    例：step=5, d=17 → 15。d<2.5 のように小さい場合は 0 になり得る（=上書き無し）。
    """
    if distance_m <= 0 or step_m <= 0:
        return 0
    return int(round(distance_m / float(step_m)) * step_m)

def load_schema_file(schema_path: Path) -> Dict:
    """スキーマ JSON を読み込む。存在しない場合は SchemaFileNotFound。"""
    if not schema_path.exists():
        raise SchemaFileNotFound(f"Schema not found: {schema_path}")
    with schema_path.open("r", encoding="utf-8") as f:
        schema_json = json.load(f)
    LOG.info("loaded Schema: %s", schema_path)
    return schema_json

def _format_json_path(path_iterable) -> str:
    """jsonschema の error.path（deque 等）を 'root.a.b[2]' 風の文字列に整形する。"""
    parts: List[str] = ["root"]
    for p in path_iterable:
        if isinstance(p, int):
            parts[-1] = parts[-1] + f"[{p}]"
        else:
            parts.append(str(p))
    return ".".join(parts)

def validate_json_schema(spec_json: Dict, schema_json: Dict) -> None:
    """
    Draft-07 で JSON スキーマ検証を行う。
    不一致があれば全件を ERROR ログに出し、 SchemaValidationError を送出する。
    """
    if Draft7Validator is None:
        LOG.error("[SCH] jsonschema library is not available. `pip install jsonschema` is required.")
        raise SchemaValidationError("jsonschema not installed")

    validator = Draft7Validator(schema_json)
    errors = sorted(validator.iter_errors(spec_json), key=lambda e: (list(e.path), list(e.schema_path)))
    if not errors:
        LOG.info("schema validation: PASSED")
        return

    LOG.error("[SCH] schema validation: FAILED (count=%d)", len(errors))
    for i, err in enumerate(errors, start=1):
        json_path = _format_json_path(err.path)
        schema_path = "/".join(map(str, err.schema_path))
        LOG.error(
            "[SCH] #%d path=%s | msg=%s | validator=%s | schema_path=%s",
            i, json_path, err.message, err.validator, schema_path
        )
    raise SchemaValidationError(f"schema validation failed with {len(errors)} error(s)")

def decide_midblock_side_for_collision(
    raw_pos: float, snapped_pos: int, tie_break: str
) -> str:
    """
    junction と midblock が同一 snapped_pos にあるとき、
    midblock の raw_pos と snapped_pos の関係で West/East を決定する。
      - raw < snapped → "west"
      - raw > snapped → "east"
      - raw == snapped → tie_break に従う
    """
    if raw_pos < float(snapped_pos):
        return "west"
    if raw_pos > float(snapped_pos):
        return "east"
    return "west" if tie_break == "toward_west" else "east"

# ==============================
# JSON → 内部表現（v1.2）
# ==============================
def ensure_supported_version(spec_json: Dict) -> None:
    """version が 1.2.* であることを確認する。不一致なら UnsupportedVersionError。"""
    version = str(spec_json.get("version", ""))
    if not version.startswith("1.2"):
        raise UnsupportedVersionError(f'unsupported "version": {version} (expected 1.2.*)')

def parse_snap_rule(spec_json: Dict) -> SnapRule:
    s = spec_json["snap"]
    raw = s["step_m"]
    # スキーマ更新後は int 想定だが、防御的に扱う
    if isinstance(raw, int):
        step = raw
    elif isinstance(raw, float) and abs(raw - round(raw)) < 1e-9:
        step = int(round(raw))
        LOG.warning("snap.step_m is non-integer float; normalized to integer: %d", step)
    else:
        raise InvalidConfigurationError(f"snap.step_m must be integer >= 1 (got: {raw!r})")
    if step < 1:
        raise InvalidConfigurationError(f"snap.step_m must be >= 1 (got: {step})")
    return SnapRule(step_m=step, tie_break=s["tie_break"])

def parse_defaults(spec_json: Dict) -> Defaults:
    d = spec_json["defaults"]
    return Defaults(
        minor_road_length_m=int(d["minor_road_length_m"]),
        ped_crossing_width_m=float(d["ped_crossing_width_m"]),
        speed_kmh=int(d["speed_kmh"]),
        sidewalk_width_m=float(d["sidewalk_width_m"]) if "sidewalk_width_m" in d else None
    )

def parse_main_road(spec_json: Dict) -> MainRoadConfig:
    mr = spec_json["main_road"]
    main = MainRoadConfig(
        length_m=float(mr["length_m"]),
        center_gap_m=float(mr["center_gap_m"]),
        lanes=int(mr["lanes"]),
    )
    LOG.info("main_road: L=%.2f gap=%.2f lanes=%d", main.length_m, main.center_gap_m, main.lanes)
    return main

def parse_junction_templates(spec_json: Dict) -> Dict[str, JunctionTemplate]:
    """
    junction_templates = { "tee": [...], "cross": [...] } を読み取り、
    id -> JunctionTemplate の辞書を返す。
    """
    result: Dict[str, JunctionTemplate] = {}
    dup_ids: Dict[str, List[str]] = {}  # id -> [kinds]
    jt_root = spec_json.get("junction_templates", {})
    for kind in ("tee", "cross"):
        arr = jt_root.get(kind, [])
        if not isinstance(arr, list):
            continue
        for t in arr:
            tpl_id = str(t["id"])
            tpl = JunctionTemplate(
                id=tpl_id,
                main_approach_begin_m=int(t["main_approach_begin_m"]),
                main_approach_lanes=int(t["main_approach_lanes"]),
                minor_lanes_to_main=int(t["minor_lanes_to_main"]),
                minor_lanes_from_main=int(t["minor_lanes_from_main"]),
                split_ped_crossing_on_main=bool(t["split_ped_crossing_on_main"]),
                median_continuous=bool(t["median_continuous"]),
                kind=kind,
            )
            if tpl_id in result:
                dup_ids.setdefault(tpl_id, [result[tpl_id].kind]).append(kind)
            else:
                result[tpl_id] = tpl
    if dup_ids:
        for dup_id, kinds in dup_ids.items():
            kinds_str = ",".join(sorted(set(kinds)))
            LOG.error("[VAL] E107 duplicate junction_template id: id=%s kinds=%s", dup_id, kinds_str)
        raise SemanticValidationError(f"duplicate junction_template id(s): {', '.join(sorted(dup_ids.keys()))}")
    LOG.info("junction_templates: %d", len(result))
    return result

def parse_signal_ref(obj: Optional[Dict]) -> Optional[SignalRef]:
    if not obj:
        return None
    return SignalRef(profile_id=str(obj["profile_id"]), offset_s=int(obj["offset_s"]))

def parse_signal_profiles(spec_json: Dict) -> Dict[str, Dict[str, SignalProfileDef]]:
    """
    signal_profiles のパースと基本検査：
      - 各 kind 配列から id->profile の辞書を構築
      - allow_movements の語彙検査
      - phases.duration_s の総和 == cycle_s 検査
    """
    profiles_by_kind: Dict[str, Dict[str, SignalProfileDef]] = {"tee": {}, "cross": {}, "xwalk_midblock": {}}
    sp_root = spec_json.get("signal_profiles", {})
    errors: List[str] = []

    def add_profile(kind: str, p: Dict, idx: int):
        pid = str(p["id"])
        cycle = int(p["cycle_s"])
        phases_data = p.get("phases", [])
        phases: List[SignalPhaseDef] = []
        sum_dur = 0
        for j, ph in enumerate(phases_data):
            name = str(ph.get("name", f"phase{j}"))
            dur = int(ph["duration_s"])
            amv_list = list(ph.get("allow_movements", []))
            # 語彙検査
            bad = [m for m in amv_list if not MOVEMENT_RE.match(str(m))]
            if bad:
                errors.append(f"[VAL] E301 invalid movement token(s) in profile={pid} kind={kind}: {bad}")
            phases.append(SignalPhaseDef(name=name, duration_s=dur, allow_movements=[str(m) for m in amv_list]))
            sum_dur += dur
        if sum_dur != cycle:
            errors.append(f"[VAL] E302 cycle mismatch in profile={pid} kind={kind}: sum(phases)={sum_dur} != cycle_s={cycle}")
        prof = SignalProfileDef(id=pid, cycle_s=cycle, phases=phases, kind=kind)
        if pid in profiles_by_kind[kind]:
            errors.append(f"[VAL] E303 duplicate signal_profile id within kind: id={pid} kind={kind}")
        else:
            profiles_by_kind[kind][pid] = prof

    for kind in ("tee", "cross", "xwalk_midblock"):
        arr = sp_root.get(kind, [])
        if not isinstance(arr, list):
            continue
        for i, p in enumerate(arr):
            add_profile(kind, p, i)

    if errors:
        for e in errors:
            LOG.error(e)
        raise SemanticValidationError(f"signal_profiles validation failed with {len(errors)} error(s)")
    LOG.info("signal_profiles: tee=%d cross=%d xwalk_midblock=%d",
             len(profiles_by_kind["tee"]), len(profiles_by_kind["cross"]), len(profiles_by_kind["xwalk_midblock"]))
    return profiles_by_kind

def parse_layout_events(spec_json: Dict, snap_rule: SnapRule, main_road: MainRoadConfig) -> List[LayoutEvent]:
    """
    layout[] を読んで丸め（snap）を適用する。
    位置が [0, main_road.length_m] の範囲外なら Warning を出してスキップする。
    丸め後は [0, grid_max] の範囲にあるもののみ採用する。
    """
    events: List[LayoutEvent] = []
    length = float(main_road.length_m)
    grid_max = grid_upper_bound(length, snap_rule.step_m)
    LOG.info("snap grid: step=%d, grid_max=%d (length=%.3f)", snap_rule.step_m, grid_max, length)

    for e in spec_json.get("layout", []):
        event_type = e["type"]
        if event_type not in ("tee", "cross", "xwalk_midblock"):
            LOG.warning("unknown event type: %s (skip)", event_type)
            continue
        pos_raw = float(e["pos_m"])
        if not (0.0 <= pos_raw <= length):
            LOG.warning("event out of range: type=%s pos_m=%.3f (skip)", event_type, pos_raw)
            continue
        pos_snapped = round_position(pos_raw, snap_rule.step_m, snap_rule.tie_break)
        # 離散端点 grid_max で再チェック
        if not (0 <= pos_snapped <= grid_max):
            LOG.warning(
                "snapped position out of grid range: type=%s raw=%.3f -> snap=%d valid=[0,%d] (skip)",
                event_type, pos_raw, pos_snapped, grid_max
            )
            continue

        if event_type in ("tee", "cross"):
            # template_id: None は None のまま保持（"None" 文字列化を避ける）
            tpl_raw = e.get("template")
            template_id = (str(tpl_raw) if tpl_raw is not None else None)
            layout_event = LayoutEvent(
                type=event_type,
                pos_m_raw=pos_raw,
                pos_m=pos_snapped,
                template_id=template_id,
                signalized=bool(e.get("signalized")),
                signal=parse_signal_ref(e.get("signal")),
                main_ped_crossing_placement=e.get("main_ped_crossing_placement"),
                branch=(e.get("branch") if event_type == "tee" else None),
            )
        else:  # xwalk_midblock
            layout_event = LayoutEvent(
                type="xwalk_midblock",
                pos_m_raw=pos_raw,
                pos_m=pos_snapped,
                signalized=bool(e.get("signalized")),
                signal=parse_signal_ref(e.get("signal")),
                split_ped_crossing_on_main=bool(e.get("split_ped_crossing_on_main")),
            )
        events.append(layout_event)
        LOG.info("layout: %s raw=%.3f -> snap=%d", event_type, pos_raw, pos_snapped)
    return events

def build_clusters(layout_events: List[LayoutEvent]) -> List[Cluster]:
    """丸め後 pos_m でグルーピングし、Cluster 単位（join の単位）を得る。"""
    clusters: List[Cluster] = []
    for pos, group in itertools.groupby(sorted(layout_events, key=lambda ev: ev.pos_m), key=lambda ev: ev.pos_m):
        clusters.append(Cluster(pos_m=pos, events=list(group)))
    LOG.info("clusters: %d", len(clusters))
    return clusters

# ==============================
# レーン上書き・ブレークポイント
# ==============================
def compute_lane_overrides(
    main_road: MainRoadConfig,
    clusters: List[Cluster],
    junction_template_by_id: Dict[str, JunctionTemplate],
    snap_rule: SnapRule,
) -> Dict[str, List[LaneOverride]]:
    """
    junction_templates の main_approach_* に従い、EB/WB の上書き区間を作る。
      EB: [max(0, pos - d), pos)
      WB: [pos, min(grid_max, pos + d))
    main_approach_lanes==0 は上書きを行わない。
    d は snap_distance_to_step() で格子倍数に丸める。
    """
    grid_max = grid_upper_bound(main_road.length_m, snap_rule.step_m)
    eb_overrides: List[LaneOverride] = []
    wb_overrides: List[LaneOverride] = []

    for cluster in clusters:
        pos = cluster.pos_m
        jt_id = next((ev.template_id for ev in cluster.events
                      if ev.type in ("tee", "cross") and ev.template_id), None)
        if not jt_id:
            continue
        tpl = junction_template_by_id.get(jt_id)
        if not tpl or tpl.main_approach_lanes <= 0:
            continue

        d_raw = float(tpl.main_approach_begin_m)
        d = snap_distance_to_step(d_raw, snap_rule.step_m)
        LOG.info("lane-override: pos=%d, d_raw=%.3f -> d_snap=%d, lanes=%d", pos, d_raw, d, tpl.main_approach_lanes)
        if d <= 0:
            continue

        # EB: pos-d ～ pos
        start_eb = max(0, pos - d)
        end_eb = max(0, min(grid_max, pos))
        if start_eb < end_eb:
            eb_overrides.append(LaneOverride(start=start_eb, end=end_eb, lanes=tpl.main_approach_lanes))
        # WB: pos ～ pos+d
        start_wb = max(0, min(grid_max, pos))
        end_wb = min(grid_max, pos + d)
        if start_wb < end_wb:
            wb_overrides.append(LaneOverride(start=start_wb, end=end_wb, lanes=tpl.main_approach_lanes))

    eb_overrides.sort(key=lambda r: (r.start, r.end))
    wb_overrides.sort(key=lambda r: (r.start, r.end))
    LOG.info("lane overrides EB: %s", [(r.start, r.end, r.lanes) for r in eb_overrides])
    LOG.info("lane overrides WB: %s", [(r.start, r.end, r.lanes) for r in wb_overrides])
    return {"EB": eb_overrides, "WB": wb_overrides}

def collect_breakpoints_and_reasons(
    main_road: MainRoadConfig,
    clusters: List[Cluster],
    lane_overrides: Dict[str, List[LaneOverride]],
    snap_rule: SnapRule,
) -> Tuple[List[int], Dict[int, BreakpointInfo]]:
    """
    0 / grid_max / クラスタ位置（tee/cross/xwalk_midblock）/ 上書き境界(start/end) を統合して
    ブレークポイントを作成し、理由タグを付与する。
    """
    grid_max = grid_upper_bound(main_road.length_m, snap_rule.step_m)
    reason_by_pos: Dict[int, BreakpointInfo] = {}

    def add(pos: int, reason: str):
        if pos < 0 or pos > grid_max:
            return
        if pos not in reason_by_pos:
            reason_by_pos[pos] = BreakpointInfo(pos=pos, reasons=set())
        reason_by_pos[pos].reasons.add(reason)

    add(0, "endpoint")
    add(grid_max, "endpoint")

    for cluster in clusters:
        x = cluster.pos_m
        if any(ev.type in ("tee", "cross") for ev in cluster.events):
            add(x, "junction")
        if any(ev.type == "xwalk_midblock" for ev in cluster.events):
            add(x, "xwalk_midblock")

    for override in lane_overrides["EB"] + lane_overrides["WB"]:
        add(override.start, "lane_change")
        add(override.end, "lane_change")

    breakpoints = sorted(reason_by_pos.keys())
    LOG.info("breakpoints: %s", [(x, sorted(reason_by_pos[x].reasons)) for x in breakpoints])
    return breakpoints, reason_by_pos

def pick_lanes_for_segment(
    direction: str, west: int, east: int, base_lanes: int,
    lane_overrides: Dict[str, List[LaneOverride]]
) -> int:
    """
    区間 [west, east) と重なる上書きの lanes の最大値を採用する。
    上書きが無ければ base_lanes を返す。
    """
    max_override = 0
    for override in lane_overrides[direction]:
        if not (east <= override.start or override.end <= west):
            max_override = max(max_override, override.lanes)
    return max(base_lanes, max_override)

# =========================================
# ノード・エッジ識別子
# =========================================
def main_node_id(direction: str, pos: int) -> str:
    return f"Node.Main.{direction}.{pos}"

def main_edge_id(direction: str, west: int, east: int) -> str:
    return f"Edge.Main.{direction}.{west}-{east}"

def minor_end_node_id(pos: int, ns: str) -> str:
    return f"Node.Minor.{pos}.{ns}-End"

def minor_edge_id(pos: int, to_from: str, ns: str) -> str:
    return f"Edge.Minor.{pos}.{to_from}.{ns}"

def cluster_id(pos: int) -> str:
    return f"Cluster.Main.{pos}"

# =========================================
# 幾何補助（座標・近傍セグメント）
# =========================================
def build_main_carriageway_y(main_road: MainRoadConfig) -> Tuple[float, float]:
    """EB/WB の y 座標（南北方向）を返す。"""
    y_eb = + main_road.center_gap_m / 2.0
    y_wb = - main_road.center_gap_m / 2.0
    return y_eb, y_wb

def find_neighbor_segments(breakpoints: List[int], pos: int) -> Tuple[Optional[int], Optional[int]]:
    """
    breakpoints に含まれる pos の左右隣（west/east）を返す。
    pos が無い場合は (None, None)。
    """
    if pos not in breakpoints:
        return None, None
    idx = breakpoints.index(pos)
    west = breakpoints[idx - 1] if idx - 1 >= 0 else None
    east = breakpoints[idx + 1] if idx + 1 < len(breakpoints) else None
    return west, east

# =========================================
# nodes.xml 出力
# =========================================
def emit_nodes_xml(main_road: MainRoadConfig,
                   defaults: Defaults,
                   clusters: List[Cluster],
                   breakpoints: List[int],
                   reason_by_pos: Dict[int, BreakpointInfo],
                   nodes_path: Path) -> None:
    """
    nodes.xml を生成する。
    - 主道路：各ブレークポイントに EB/WB ノードを出力。
    - join：端点以外の全ブレークポイントで EB/WB を <join>（id=Cluster.Main.{pos}）。
    - 従道路端点：tee/cross の位置に north/south の dead_end ノードを出力。
    """
    y_eb, y_wb = build_main_carriageway_y(main_road)
    # 離散端点 grid_max は breakpoints[-1] と一致する前提（collect_* が追加済み）
    grid_max = breakpoints[-1] if breakpoints else 0

    lines: List[str] = []
    lines.append("<nodes>")

    # 主道路ノード
    for x in breakpoints:
        lines.append(f'  <node id="{main_node_id("EB", x)}" x="{x}" y="{y_eb}"/>')
        lines.append(f'  <node id="{main_node_id("WB", x)}" x="{x}" y="{y_wb}"/>')

    # join（端点除外）
    for pos in breakpoints:
        if pos in (0, grid_max):
            continue
        eb = main_node_id("EB", pos)
        wb = main_node_id("WB", pos)
        reasons_text = ",".join(sorted(reason_by_pos[pos].reasons))
        lines.append(f'  <join id="{cluster_id(pos)}" x="{pos}" y="0" nodes="{eb} {wb}"/>'
                     f'  <!-- reasons: {reasons_text} -->')

    # tee/cross の従道路端点
    for cluster in clusters:
        pos = cluster.pos_m
        for layout_event in cluster.events:
            if layout_event.type not in ("tee", "cross"):
                continue
            if layout_event.type == "tee":
                branches: List[str] = [layout_event.branch] if layout_event.branch in ("north", "south") else []
            else:  # cross
                branches = ["north", "south"]

            for b in branches:
                ns = "N" if b == "north" else "S"
                offset_m = defaults.minor_road_length_m
                y_end = +offset_m if ns == "N" else -offset_m
                end_id = minor_end_node_id(pos, ns)
                lines.append(
                    f'  <node id="{end_id}" x="{pos}" y="{y_end}"/>'
                    f'  <!-- minor dead_end ({ns}), offset={offset_m} from y=0 -->'
                )

    lines.append("</nodes>")
    nodes_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    LOG.info("wrote nodes: %s", nodes_path)

# =========================================
# edges.xml 出力
# =========================================
def attach_main_node_for_minor(ns: str, pos: int) -> str:
    """
    従道路の接続先（主道路側ノード）を返す。
      north(N) → EB 側ノード、south(S) → WB 側ノード。
    """
    if ns == "N":
        return main_node_id("EB", pos)
    if ns == "S":
        return main_node_id("WB", pos)
    raise InvalidConfigurationError(f"unknown ns={ns}")

def emit_edges_xml(main_road: MainRoadConfig,
                   defaults: Defaults,
                   clusters: List[Cluster],
                   breakpoints: List[int],
                   junction_template_by_id: Dict[str, JunctionTemplate],
                   lane_overrides: Dict[str, List[LaneOverride]],
                   edges_path: Path) -> None:
    """
    edges.xml を生成する。
    - 主道路：ブレークポイントで分割し、区間ごとに numLanes を決定（lane_overrides を反映）。
    - 従道路：tee/cross ごとに north/south の to/from エッジを生成。
    - 速度：主・従道路とも defaults.speed_kmh を適用（m/s）。
    注意:
      * lane_overrides は main() 側で compute_lane_overrides(..., snap_rule) した結果を受領する。
      * 本関数内では lane_overrides の再計算はしない（snap 規則の一貫性確保）。
    """
    speed_mps = kmh_to_mps(defaults.speed_kmh)

    lines: List[str] = []
    lines.append("<edges>")

    # 主道路 EB/WB
    for west, east in zip(breakpoints[:-1], breakpoints[1:]):
        lanes_eb = pick_lanes_for_segment("EB", west, east, main_road.lanes, lane_overrides)
        lines.append(
            f'  <edge id="{main_edge_id("EB", west, east)}" '
            f'from="{main_node_id("EB", west)}" to="{main_node_id("EB", east)}" '
            f'numLanes="{lanes_eb}" speed="{speed_mps:.3f}"/>'
        )
        lanes_wb = pick_lanes_for_segment("WB", west, east, main_road.lanes, lane_overrides)
        lines.append(
            f'  <edge id="{main_edge_id("WB", west, east)}" '
            f'from="{main_node_id("WB", east)}" to="{main_node_id("WB", west)}" '
            f'numLanes="{lanes_wb}" speed="{speed_mps:.3f}"/>'
        )

    # 従道路エッジ
    for cluster in clusters:
        pos = cluster.pos_m
        for layout_event in cluster.events:
            if layout_event.type not in ("tee", "cross"):
                continue
            if not layout_event.template_id:
                LOG.warning("junction at %s has no template_id (skip)", pos)
                continue
            tpl = junction_template_by_id.get(layout_event.template_id)
            if not tpl:
                LOG.warning("junction template not found: id=%s (pos=%s)", layout_event.template_id, pos)
                continue

            if layout_event.type == "tee":
                branches: List[str] = [layout_event.branch] if layout_event.branch in ("north", "south") else []
            else:
                branches = ["north", "south"]

            for b in branches:
                ns = "N" if b == "north" else "S"
                attach_node = attach_main_node_for_minor(ns, pos)
                # to: minor -> main
                lines.append(
                    f'  <edge id="{minor_edge_id(pos, "to", ns)}" '
                    f'from="{minor_end_node_id(pos, ns)}" to="{attach_node}" '
                    f'numLanes="{tpl.minor_lanes_to_main}" speed="{speed_mps:.3f}"/>'
                )
                # from: main -> minor
                lines.append(
                    f'  <edge id="{minor_edge_id(pos, "from", ns)}" '
                    f'from="{attach_node}" to="{minor_end_node_id(pos, ns)}" '
                    f'numLanes="{tpl.minor_lanes_from_main}" speed="{speed_mps:.3f}"/>'
                )

    lines.append("</edges>")
    edges_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    LOG.info("wrote edges: %s", edges_path)

# =========================================
# connections.xml（crossings のみ）出力
# =========================================
def crossing_id_minor(pos: int, ns: str) -> str:
    return f"Cross.Minor.{pos}.{ns}"

def crossing_id_main(pos: int, side: str) -> str:
    return f"Cross.Main.{pos}.{side}"

def crossing_id_main_split(pos: int, side: str, direction: str) -> str:
    return f"Cross.Main.{pos}.{side}.{direction}"

def crossing_id_midblock(pos: int) -> str:
    return f"Cross.Mid.{pos}"

def crossing_id_midblock_split(pos: int, direction: str) -> str:
    return f"Cross.Mid.{pos}.{direction}"

def get_main_edges_west_side(breakpoints: List[int], pos: int) -> Tuple[Optional[str], Optional[str]]:
    """
    pos の西側隣接区間に対応する EB/WB エッジ ID を返す。
    EB は west->pos、WB は pos->west のセグメント。
    """
    west, _ = find_neighbor_segments(breakpoints, pos)
    if west is None:
        return None, None
    return (main_edge_id("EB", west, pos), main_edge_id("WB", west, pos))

def get_main_edges_east_side(breakpoints: List[int], pos: int) -> Tuple[Optional[str], Optional[str]]:
    """
    pos の東側隣接区間に対応する EB/WB エッジ ID を返す。
    EB は pos->east、WB は east->pos のセグメント。
    """
    _, east = find_neighbor_segments(breakpoints, pos)
    if east is None:
        return None, None
    return (main_edge_id("EB", pos, east), main_edge_id("WB", pos, east))

# =========================================
# 接続（車両）の割付補助
# =========================================
def _alloc_excess_by_ratio(excess: int, wL: int, wT: int, wR: int) -> Tuple[int, int, int]:
    """
    余り本数 'excess' を (wL:wT:wR) 比で整数配分する。
    最大剰余法：基準配分=excess*比/和 の床、残りを剰余の大きい順で配る。
    """
    weights = [max(0, wL), max(0, wT), max(0, wR)]
    W = sum(weights)
    if excess <= 0 or W == 0:
        return (0, 0, 0)
    quotas = [excess * (w / W) for w in weights]
    floors = [int(q) for q in quotas]
    remain = excess - sum(floors)
    remainders = [(i, quotas[i] - floors[i]) for i in range(3)]
    remainders.sort(key=lambda x: (-x[1], x[0]))
    for k in range(remain):
        floors[remainders[k][0]] += 1
    return tuple(floors)  # (aL, aT, aR)

def _build_lane_permissions(s: int, l: int, t: int, r: int) -> List[Tuple[bool, bool, bool]]:
    """
    入力 (s,l,t,r) に対して、S₁..S_s（左→右順）各レーンの L/T/R 可否を返す。
    戻り値は [(L可, T可, R可)] × s。プロジェクト規則に従い決定的。
    """
    hasL, hasT, hasR = (l > 0), (t > 0), (r > 0)
    M = int(hasL) + int(hasT) + int(hasR)

    # s>0 で M=0 は上位で検査（E401）。
    if s == 0:
        return []

    # ケース 1A: s >= M（純化ブロック）
    if s >= M and M > 0:
        aL_min, aT_min, aR_min = int(hasL), int(hasT), int(hasR)
        base = aL_min + aT_min + aR_min
        excess = s - base
        aL_add, aT_add, aR_add = _alloc_excess_by_ratio(excess, l, t, r)
        aL = aL_min + aL_add
        aT = aT_min + aT_add
        aR = aR_min + aR_add
        perms: List[Tuple[bool, bool, bool]] = []
        perms += [(True,  False, False)] * aL  # L-only
        perms += [(False, True,  False)] * aT  # T-only
        perms += [(False, False, True )] * aR  # R-only
        return perms

    # ケース 1B: s = M-1 = 2 かつ M=3 → [LT, TR]
    if s == 2 and M == 3:
        return [(True, True, False), (False, True, True)]

    # ケース 1C: s = 1
    if s == 1:
        return [(hasL, hasT, hasR)]

    # その他の稀ケース（s < M-1 など）は、最小限を優先しつつ左→右で詰める
    # 例：M=3, s=1 は上で処理済み、M=2, s=1 → 先頭に LT / TR / LR のいずれか
    if M == 2:
        if hasL and hasT:
            return [(True, True, False)]
        if hasT and hasR:
            return [(False, True, True)]
        if hasL and hasR:
            return [(True, False, True)]
    # 到達不能想定だが、防御的に L/T/R のあるものを詰める
    return [(hasL, hasT, hasR)] * max(1, s)

def _emit_vehicle_connections_for_approach(
    lines: List[str],
    pos: int,
    in_edge_id: str,
    s_count: int,
    L_target: Optional[Tuple[str, int]],
    T_target: Optional[Tuple[str, int]],
    R_target: Optional[Tuple[str, int]],
) -> int:
    """
    単一アプローチに対して <connection> を生成し、lines に追記する。
    L/T/R 行先は (edge_id, lane_count) または None。
    返り値：生成した <connection> 件数。
    """
    l = (L_target[1] if L_target else 0)
    t = (T_target[1] if T_target else 0)
    r = (R_target[1] if R_target else 0)

    if s_count > 0 and (l + t + r) == 0:
        LOG.error("[VAL] E401 no available movements: pos=%s in_edge=%s s=%d l=%d t=%d r=%d",
                  pos, in_edge_id, s_count, l, t, r)
        raise SemanticValidationError("no available movements for approach")

    perms = _build_lane_permissions(s_count, l, t, r)
    # S の添字（1..s）で、各動作に許された起点レーンを列挙
    I_L = [i for i, (pL, pT, pR) in enumerate(perms, start=1) if pL]
    I_T = [i for i, (pL, pT, pR) in enumerate(perms, start=1) if pT]
    I_R = [i for i, (pL, pT, pR) in enumerate(perms, start=1) if pR]

    emitted = 0
    # L
    if l > 0 and len(I_L) > 0:
        for offset, from_lane in enumerate(I_L):
            to_lane = min(offset + 1, l)
            lines.append(
                f'  <connection from="{in_edge_id}" to="{L_target[0]}" fromLane="{from_lane}" toLane="{to_lane}"/>'
            )
            emitted += 1
    # T
    if t > 0 and len(I_T) > 0:
        for offset, from_lane in enumerate(I_T):
            to_lane = min(offset + 1, t)
            lines.append(
                f'  <connection from="{in_edge_id}" to="{T_target[0]}" fromLane="{from_lane}" toLane="{to_lane}"/>'
            )
            emitted += 1
        if len(I_T) < t:
            rightmost_source = I_T[-1]
            for to_lane in range(len(I_T) + 1, t + 1):
                lines.append(
                    f'  <connection from="{in_edge_id}" to="{T_target[0]}" fromLane="{rightmost_source}" toLane="{to_lane}"/>'
                )
                emitted += 1
    # R
    if r > 0 and len(I_R) > 0:
        to_lane_map: Dict[int, int] = {}
        for offset, from_lane in enumerate(reversed(I_R)):
            if offset < r:
                to_lane = r - offset
            else:
                to_lane = r
            to_lane_map[from_lane] = to_lane
        for from_lane in I_R:
            lines.append(
                f'  <connection from="{in_edge_id}" to="{R_target[0]}" fromLane="{from_lane}" toLane="{to_lane_map[from_lane]}"/>'
            )
            emitted += 1

    if (l + t + r) > 0 and emitted == 0 and s_count > 0:
        LOG.error("[VAL] E402 zero vehicle connections emitted: pos=%s in_edge=%s s=%d l=%d t=%d r=%d",
                  pos, in_edge_id, s_count, l, t, r)
        raise SemanticValidationError("no vehicle connections emitted")
    return emitted

def emit_connections_with_crossings(defaults: Defaults,
                                    clusters: List[Cluster],
                                    breakpoints: List[int],
                                    junction_template_by_id: Dict[str, JunctionTemplate],
                                    snap_rule: SnapRule,
                                    main_road: MainRoadConfig,
                                    lane_overrides: Dict[str, List[LaneOverride]],
                                    connections_path: Path) -> None:
    """
    connections.xml を生成する（<crossing> と 車両 <connection>）。
    - node は Cluster.Main.{pos}（2 段 netconvert 前提）。
    - 交差点:
        * 従道路（minor）横断は常設（tee: 片側 / cross: 両側）。
        * 主道路（main）横断は placement(west/east) に従う。
          - テンプレートの split_ped_crossing_on_main が真なら EB/WB を分割して出力。
    - 単路部（xwalk_midblock）:
        * イベントの split_ped_crossing_on_main に従う。
        * west/east どちら側に接続するかは snap.tie_break に従う。
          - 優先側に隣接セグメントが無ければ、反対側にフォールバックする。
    - junction と midblock が同一 pos の場合は midblock を交差点に吸収し、
      交差点の west/east 設置フラグに統合する（midblock 側の crossing は出力しない）。
    - 車両 <connection>:
        * 各 Cluster.Main.{pos} で流入アプローチを列挙し、(s,l,t,r) を決定。
        * レーン index は 1..本数（0 は歩道）を使用。
        * 左折: 左端から順に割当（余剰は最右レーン共有）。
        * 直進: 左詰めで割当し、最右直進レーンが余剰行先を扇出。
        * 右折: 右端から順に割当（余剰は最右レーン共有）。
        * 禁止規則（median_continuous / T 字路欠側 / 端点）を反映。
    """
    width = defaults.ped_crossing_width_m
    lines: List[str] = []
    lines.append("<connections>")

    # 吸収が発生した pos の集合（midblock crossing 出力抑止用）
    absorbed_pos: Set[int] = set()

    # 1) 交差点（tee/cross） … crossing 出力
    for cluster in clusters:
        pos = cluster.pos_m
        junction_events = [ev for ev in cluster.events if ev.type in ("tee", "cross")]
        if not junction_events:
            continue
        node = cluster_id(pos)

        # 主道路横断の設置可否と分割表現
        place_west = False
        place_east = False
        split_main = False
        for ev in junction_events:
            if ev.main_ped_crossing_placement:
                place_west = place_west or bool(ev.main_ped_crossing_placement.get("west", False))
                place_east = place_east or bool(ev.main_ped_crossing_placement.get("east", False))
            if ev.template_id and ev.template_id in junction_template_by_id:
                split_main = split_main or bool(junction_template_by_id[ev.template_id].split_ped_crossing_on_main)

        # 同位置 midblock の有無を確認し、側別に吸収
        mid_events = [ev for ev in cluster.events if ev.type == "xwalk_midblock"]
        if mid_events:
            absorbed_pos.add(pos)
            for mev in mid_events:
                side = decide_midblock_side_for_collision(mev.pos_m_raw, pos, snap_rule.tie_break)
                if side == "west":
                    place_west = True
                else:
                    place_east = True

        # 1-a) minor crossing（従道路）
        if any(ev.type == "cross" for ev in junction_events):
            branches: List[str] = ["north", "south"]
        else:
            b = junction_events[0].branch
            branches = [b] if b in ("north", "south") else []
        for b in branches:
            ns = "N" if b == "north" else "S"
            e_to = f"Edge.Minor.{pos}.to.{ns}"
            e_from = f"Edge.Minor.{pos}.from.{ns}"
            cid = crossing_id_minor(pos, ns)
            lines.append(f'  <crossing id="{cid}" node="{node}" edges="{e_to} {e_from}" width="{width:.3f}"/>')

        # 1-b) main crossing（West/East）
        if place_west:
            eb_edge, wb_edge = get_main_edges_west_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if split_main:
                    lines.append(f'  <crossing id="{crossing_id_main_split(pos, "West", "EB")}" '
                                 f'node="{node}" edges="{eb_edge}" width="{width:.3f}"/>')
                    lines.append(f'  <crossing id="{crossing_id_main_split(pos, "West", "WB")}" '
                                 f'node="{node}" edges="{wb_edge}" width="{width:.3f}"/>')
                else:
                    lines.append(f'  <crossing id="{crossing_id_main(pos, "West")}" '
                                 f'node="{node}" edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>')

        if place_east:
            eb_edge, wb_edge = get_main_edges_east_side(breakpoints, pos)
            if eb_edge and wb_edge:
                if split_main:
                    lines.append(f'  <crossing id="{crossing_id_main_split(pos, "East", "EB")}" '
                                 f'node="{node}" edges="{eb_edge}" width="{width:.3f}"/>')
                    lines.append(f'  <crossing id="{crossing_id_main_split(pos, "East", "WB")}" '
                                 f'node="{node}" edges="{wb_edge}" width="{width:.3f}"/>')
                else:
                    lines.append(f'  <crossing id="{crossing_id_main(pos, "East")}" '
                                 f'node="{node}" edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>')

    # 2) 単路部（midblock） … crossing 出力
    for cluster in clusters:
        pos = cluster.pos_m
        if pos in absorbed_pos:
            continue
        mid_events = [ev for ev in cluster.events if ev.type == "xwalk_midblock"]
        if not mid_events:
            continue
        node = cluster_id(pos)

        # tie_break に従い接続側を決定
        if snap_rule.tie_break == "toward_west":
            eb_edge, wb_edge = get_main_edges_west_side(breakpoints, pos)
            if not (eb_edge and wb_edge):
                eb_edge, wb_edge = get_main_edges_east_side(breakpoints, pos)
        else:
            eb_edge, wb_edge = get_main_edges_east_side(breakpoints, pos)
            if not (eb_edge and wb_edge):
                eb_edge, wb_edge = get_main_edges_west_side(breakpoints, pos)

        if not (eb_edge and wb_edge):
            LOG.warning("midblock at %s: adjacent main edges not found; crossing omitted", pos)
            continue

        split_midblock = any(bool(ev.split_ped_crossing_on_main) for ev in mid_events)
        width = defaults.ped_crossing_width_m
        if split_midblock:
            lines.append(f'  <crossing id="{crossing_id_midblock_split(pos, "EB")}" '
                         f'node="{node}" edges="{eb_edge}" width="{width:.3f}"/>')
            lines.append(f'  <crossing id="{crossing_id_midblock_split(pos, "WB")}" '
                         f'node="{node}" edges="{wb_edge}" width="{width:.3f}"/>')
        else:
            lines.append(f'  <crossing id="{crossing_id_midblock(pos)}" '
                         f'node="{node}" edges="{eb_edge} {wb_edge}" width="{width:.3f}"/>')

    # 3) 交差点（tee/cross） … 車両 <connection> 出力
    for cluster in clusters:
        pos = cluster.pos_m
        junction_events = [ev for ev in cluster.events if ev.type in ("tee", "cross")]
        if not junction_events:
            continue
        ev = junction_events[0]  # 同一 pos に複数 junction は検証で禁止（E106）
        tpl = junction_template_by_id.get(ev.template_id) if ev.template_id else None
        if not tpl:
            LOG.warning("junction template not found: id=%s (pos=%s)", getattr(ev, "template_id", None), pos)
            continue

        # 分岐の有無（T 字路か十字路か）
        if ev.type == "cross":
            exist_north = True
            exist_south = True
        else:
            exist_north = (ev.branch == "north")
            exist_south = (ev.branch == "south")

        west, east = find_neighbor_segments(breakpoints, pos)

        def pick_main_lanes(direction: str, a: Optional[int], b: Optional[int]) -> int:
            if a is None or b is None:
                return 0
            return pick_lanes_for_segment(direction, a, b, main_road.lanes, lane_overrides)

        # アプローチ列挙と (s,l,t,r) の決定
        # --- Main.EB-in（西→pos）: IDは {west}-{pos}
        if west is not None:
            in_edge = main_edge_id("EB", west, pos)  # OK: {西側}-{東側}
            s_count = pick_main_lanes("EB", west, pos)
            L_target = (minor_edge_id(pos, "from", "N"), tpl.minor_lanes_from_main) if exist_north else None
            T_target = (main_edge_id("EB", pos, east), pick_main_lanes("EB", pos, east)) if east is not None else None
            R_target = (minor_edge_id(pos, "from", "S"), tpl.minor_lanes_from_main) if exist_south else None
            if tpl.median_continuous:
                R_target = None  # 主道路流入は右折禁止
            _emit_vehicle_connections_for_approach(lines, pos, in_edge, s_count, L_target, T_target, R_target)

        # --- Main.WB-in（東→pos）: IDは {pos}-{east} ではなく {西側}-{東側} = {pos}-{east}
        if east is not None:
            in_edge = main_edge_id("WB", pos, east)  # 修正: 旧 (east, pos) → 正 (pos, east)
            s_count = pick_main_lanes("WB", pos, east)  # 修正: 旧 pick(..., east, pos) → pick(..., pos, east)
            L_target = (minor_edge_id(pos, "from", "S"), tpl.minor_lanes_from_main) if exist_south else None
            T_target = (main_edge_id("WB", west, pos), pick_main_lanes("WB", west, pos)) if west is not None else None  # 修正: 旧 (pos, west) → (west, pos)
            R_target = (minor_edge_id(pos, "from", "N"), tpl.minor_lanes_from_main) if exist_north else None
            if tpl.median_continuous:
                R_target = None  # 主道路流入は右折禁止
            _emit_vehicle_connections_for_approach(lines, pos, in_edge, s_count, L_target, T_target, R_target)

        # --- Minor.N.to-in（北端→pos）
        if exist_north:
            in_edge = minor_edge_id(pos, "to", "N")
            s_count = tpl.minor_lanes_to_main
            L_target = (main_edge_id("EB", pos, east), pick_main_lanes("EB", pos, east)) if east is not None else None
            T_target = (minor_edge_id(pos, "from", "S"), tpl.minor_lanes_from_main) if exist_south else None
            R_target = (main_edge_id("WB", west, pos), pick_main_lanes("WB", west, pos)) if west is not None else None
            # 従道路流入は直進・右折禁止
            T_target = None
            R_target = None
            _emit_vehicle_connections_for_approach(lines, pos, in_edge, s_count, L_target, T_target, R_target)

        # --- Minor.S.to-in（南端→pos）
        if exist_south:
            in_edge = minor_edge_id(pos, "to", "S")
            s_count = tpl.minor_lanes_to_main
            L_target = (main_edge_id("WB", west, pos), pick_main_lanes("WB", west, pos)) if west is not None else None  # 修正: 旧 (pos, west) → (west, pos)
            T_target = (minor_edge_id(pos, "from", "N"), tpl.minor_lanes_from_main) if exist_north else None
            R_target = (main_edge_id("EB", pos, east), pick_main_lanes("EB", pos, east)) if east is not None else None  # 統一: {西側}-{東側}（本ケースではRは後で禁止）
            # 従道路流入は直進・右折禁止
            T_target = None
            R_target = None
            _emit_vehicle_connections_for_approach(lines, pos, in_edge, s_count, L_target, T_target, R_target)


    # 重複の簡易排除（同一行の多重生成を防ぐ）
    unique_lines = []
    seen = set()
    for ln in lines:
        if ln.startswith("  <connection "):
            key = ln.strip()
            if key in seen:
                LOG.warning("[VAL] E405 duplicated <connection> suppressed: %s", key)
                continue
            seen.add(key)
        unique_lines.append(ln)

    unique_lines.append("</connections>")
    connections_path.write_text("\n".join(unique_lines) + "\n", encoding="utf-8")
    LOG.info("wrote connections(crossings+vehicles): %s", connections_path)

# ===========================
# Semantic Validation (v1.2)
# ===========================
def validate_semantics(
    spec_json: Dict,
    snap_rule: SnapRule,
    main_road: MainRoadConfig,
    junction_template_by_id: Dict[str, JunctionTemplate],
    signal_profiles_by_kind: Dict[str, Dict[str, SignalProfileDef]],
) -> None:
    """
    スキーマ検証では拾い切れない内容面の不正を検出する。
    仕様（本版）:
      - E101: layout[*].pos_m (raw) が [0, length] 外
      - E102: 丸め後 pos が [0, grid_max] 外
      - E103: tee/cross の template 未指定 or 未定義
      - E104: tee の branch 未指定 or 不正値
      - E105: signalized と signal の有無の不整合
      - E106: 同一カテゴリ同位置（丸め後 pos）での重複（tee/cross は "junction"）
      - E107: signal profile の存在・種別整合（既存）
      - E108: 丸め後に同一 pos に junction と midblock が同居し、横断歩道が重複
      - W201: 丸め後 pos が端点（0 or grid_max）
    エラーは全件をログ出力（[VAL]）し、1件以上で SemanticValidationError を送出。
    """
    def norm_type(etype: str) -> str:
        return "junction" if etype in ("tee", "cross") else etype

    length = float(main_road.length_m)
    grid_max = grid_upper_bound(length, snap_rule.step_m)
    errors: List[str] = []
    warnings: List[str] = []

    layout = spec_json.get("layout", []) or []
    seen: Dict[Tuple[str, int], int] = {}

    # pos ごとの分類マップ（E108 用）
    pos_to_junction: Dict[int, Dict] = {}
    pos_to_midblocks: Dict[int, List[Tuple[int, float, int]]] = {}  # (index, raw, snap)

    # すべての midblock を保存（近接検査用）
    all_midblocks: List[Tuple[int, float, int]] = []  # (index, raw, snap)

    for idx, e in enumerate(layout):
        try:
            etype = str(e["type"])
            pos_raw = float(e["pos_m"])
        except Exception:
            errors.append(f"[VAL] E000 unknown layout item at index={idx}: {e!r}")
            continue

        # E101: raw 範囲外（連続長に対して判定）
        if not (0.0 <= pos_raw <= length):
            errors.append(f"[VAL] E101 out-of-range (raw): index={idx} type={etype} pos_m={pos_raw} valid=[0,{length}]")
            continue

        # 丸め後（整数[m]）
        pos_snap = round_position(pos_raw, snap_rule.step_m, snap_rule.tie_break)

        # E102: snap 後の範囲外（離散端点 grid_max に対して判定）
        if not (0 <= pos_snap <= grid_max):
            errors.append(f"[VAL] E102 out-of-range (snapped): index={idx} type={etype} raw={pos_raw} snap={pos_snap} valid=[0,{grid_max}]")

        # W201: 端点一致（0 or grid_max）
        if pos_snap in (0, grid_max):
            warnings.append(f"[VAL] W201 snapped position at endpoint: index={idx} type={etype} snap={pos_snap}")

        # 個別検査
        if etype in ("tee", "cross"):
            tpl_id = e.get("template")
            if not tpl_id or tpl_id not in junction_template_by_id:
                errors.append(f"[VAL] E103 template missing/unknown: index={idx} type={etype} template={tpl_id}")
            if etype == "tee":
                branch = e.get("branch")
                if branch not in ("north", "south"):
                    errors.append(f"[VAL] E104 tee.branch invalid: index={idx} branch={branch}")

            # E105: signalized と signal の整合
            signalized = e.get("signalized")
            has_signal = bool(e.get("signal"))
            if signalized is True and not has_signal:
                errors.append(f"[VAL] E105 signal required but missing: index={idx} type={etype}")
            if signalized is False and has_signal:
                errors.append(f"[VAL] E105 signal must be absent when signalized=false: index={idx} type={etype}")

            # E107: プロファイル参照の存在・種別整合
            if signalized is True:
                sig = e.get("signal") or {}
                pid = sig.get("profile_id")
                kind = "tee" if etype == "tee" else "cross"
                exists = bool(pid) and (pid in signal_profiles_by_kind.get(kind, {}))
                if not exists:
                    errors.append(f"[VAL] E107 unknown signal profile or kind mismatch: index={idx} type={etype} profile_id={pid} expected_kind={kind}")

            # E108 用：junction 記録
            pos_to_junction[pos_snap] = {
                "index": idx,
                "placement": (e.get("main_ped_crossing_placement") or {"west": False, "east": False})
            }

        elif etype == "xwalk_midblock":
            signalized = e.get("signalized")
            has_signal = bool(e.get("signal"))
            if signalized is True and not has_signal:
                errors.append(f"[VAL] E105 signal required but missing: index={idx} type={etype}")
            if signalized is False and has_signal:
                errors.append(f"[VAL] E105 signal must be absent when signalized=false: index={idx} type={etype}")

            if signalized is True:
                sig = e.get("signal") or {}
                pid = sig.get("profile_id")
                kind = "xwalk_midblock"
                exists = bool(pid) and (pid in signal_profiles_by_kind.get(kind, {}))
                if not exists:
                    errors.append(f"[VAL] E107 unknown signal profile or kind mismatch: index={idx} type={etype} profile_id={pid} expected_kind={kind}")

            # E108 用：midblock 記録
            pos_to_midblocks.setdefault(pos_snap, []).append((idx, pos_raw, pos_snap))
            all_midblocks.append((idx, pos_raw, pos_snap))

        else:
            errors.append(f"[VAL] E000 unknown event type: index={idx} type={etype}")

        # E106 集計（midblock が junction と同居する pos は E106 から除外 → 後段 E108 で扱う）
        key = (norm_type(etype), pos_snap)
        seen[key] = seen.get(key, 0) + 1

    # E106: 同カテゴリ・同位置の重複
    for (ntype, pos_snap), cnt in seen.items():
        if cnt >= 2:
            if ntype == "xwalk_midblock" and pos_snap in pos_to_junction:
                # junction 同居の midblock は E106 ではなく E108 で扱う（両側追加の正当ケースを許容）
                continue
            errors.append(f"[VAL] E106 duplicated events at same snapped position: type={ntype} pos={pos_snap} count={cnt}")

    # E108: junction × midblock ルール違反
    #  (1) 同位置衝突：側別重複（同側に 2 本以上）⇒ エラー
    #  (2) 近接二重   ：非衝突でも ±step_m, ±2·step_m の snapped 位置に同側 2 本以上 ⇒ エラー
    step = snap_rule.step_m
    for jpos, jinfo in pos_to_junction.items():
        # (1) 同位置衝突
        colliders = pos_to_midblocks.get(jpos, [])  # [(idx, raw, snap)]
        if colliders:
            # 側別カウント
            side_to_indices: Dict[str, List[int]] = {"west": [], "east": []}
            for (idx, raw, _snap) in colliders:
                side = decide_midblock_side_for_collision(raw, jpos, snap_rule.tie_break)
                side_to_indices[side].append(idx)

            # 側別重複（同側 2 本以上）→ エラー
            for side, idxs in side_to_indices.items():
                if len(idxs) >= 2:
                    errors.append(
                        f"[VAL] E108 junction-midblock collision causes duplicate crossing: "
                        f"pos={jpos} side={side} midblock_indices={','.join(map(str, idxs))}"
                    )

            # (2) 近接二重（吸収側に近接 midblock が残っていないか）
            #     近接の定義：jpos ± step, jpos ± 2*step に snapped された midblock
            near_west = {jpos - step, jpos - 2*step}
            near_east = {jpos + step, jpos + 2*step}
            # 近接候補を全 midblock から抽出
            near_west_idxs = [idx for (idx, _raw, snap) in all_midblocks if snap in near_west and snap >= 0]
            near_east_idxs = [idx for (idx, _raw, snap) in all_midblocks if snap in near_east and snap <= grid_max]

            # 吸収が起きた側（= colliders が 1 本以上ある側）に、同側近接が 1 本以上重なっていればエラー
            if len(side_to_indices["west"]) >= 1 and len(near_west_idxs) >= 1:
                # もし近接が 2 本以上であれば明確に同側二重
                errors.append(
                    f"[VAL] E108 additional nearby midblock on absorbed side (west): "
                    f"pos={jpos} colliders={','.join(map(str, side_to_indices['west']))} "
                    f"near={','.join(map(str, near_west_idxs))}"
                )
            if len(side_to_indices["east"]) >= 1 and len(near_east_idxs) >= 1:
                errors.append(
                    f"[VAL] E108 additional nearby midblock on absorbed side (east): "
                    f"pos={jpos} colliders={','.join(map(str, side_to_indices['east']))} "
                    f"near={','.join(map(str, near_east_idxs))}"
                )

        else:
            # 同位置衝突が無い場合でも、近接二重（同側 2 本以上）はエラー
            # 例: junction=1000, midblock=1001,999,998 → west 側 2 本(999,998) でエラー
            near_west = {jpos - step, jpos - 2*step}
            near_east = {jpos + step, jpos + 2*step}
            west_idxs = [idx for (idx, _raw, snap) in all_midblocks if snap in near_west and snap >= 0]
            east_idxs = [idx for (idx, _raw, snap) in all_midblocks if snap in near_east and snap <= grid_max]

            if len(west_idxs) >= 2:
                errors.append(
                    f"[VAL] E108 multiple nearby midblocks on west side around junction: "
                    f"pos={jpos} indices={','.join(map(str, west_idxs))}"
                )
            if len(east_idxs) >= 2:
                errors.append(
                    f"[VAL] E108 multiple nearby midblocks on east side around junction: "
                    f"pos={jpos} indices={','.join(map(str, east_idxs))}"
                )

    # ログ出力
    if warnings:
        for w in warnings:
            LOG.warning(w)
    if errors:
        for er in errors:
            LOG.error(er)
        raise SemanticValidationError(f"semantic validation failed with {len(errors)} error(s)")

# ===========================
# メインフロー
# ===========================
def main():
    artifacts = ensure_output_directory()
    configure_logger(artifacts.log_path, console=True, level=logging.INFO)
    LOG.info("outdir: %s", artifacts.outdir.resolve())
    try:
        # 1) スキーマ読み込み・検証
        spec_json = load_json_file(INPUT_JSON_PATH)
        schema_json = load_schema_file(SCHEMA_JSON_PATH)
        validate_json_schema(spec_json, schema_json)

        # 2) バージョン強制（1.2.*）
        ensure_supported_version(spec_json)

        # 3) パース（順序重要：テンプレ重複→プロファイル検査→意味検証）
        snap_rule = parse_snap_rule(spec_json)
        defaults = parse_defaults(spec_json)
        main_road = parse_main_road(spec_json)
        junction_template_by_id = parse_junction_templates(spec_json)  # 重複検査を内包
        signal_profiles_by_kind = parse_signal_profiles(spec_json)      # cycle/語彙検査を内包

        # 4) 意味検証（参照整合・範囲・重複）
        validate_semantics(
            spec_json=spec_json,
            snap_rule=snap_rule,
            main_road=main_road,
            junction_template_by_id=junction_template_by_id,
            signal_profiles_by_kind=signal_profiles_by_kind,
        )

        # 5) layout パース
        layout_events = parse_layout_events(spec_json, snap_rule, main_road)

        # 6) クラスタ化 → レーン上書き → ブレークポイント収集
        clusters = build_clusters(layout_events)
        lane_overrides = compute_lane_overrides(main_road, clusters, junction_template_by_id, snap_rule)
        breakpoints, reason_by_pos = collect_breakpoints_and_reasons(main_road, clusters, lane_overrides, snap_rule)

        # 7) 出力（nodes / edges / connections）
        emit_nodes_xml(main_road, defaults, clusters, breakpoints, reason_by_pos, artifacts.nodes_path)
        emit_edges_xml(main_road, defaults, clusters, breakpoints, junction_template_by_id, lane_overrides, artifacts.edges_path)
        emit_connections_with_crossings(
            defaults, clusters, breakpoints, junction_template_by_id,
            snap_rule, main_road, lane_overrides, artifacts.connections_path
        )


        # 8) 2 段 netconvert（任意）
        run_two_step_netconvert(artifacts)

        LOG.info("build done.")
    except (SpecFileNotFound, SchemaFileNotFound) as exc:
        LOG.error("[IO] %s", exc)
    except UnsupportedVersionError as exc:
        LOG.error("[VER] %s", exc)
    except SchemaValidationError as exc:
        LOG.error("[SCH] %s", exc)
    except SemanticValidationError as exc:
        LOG.error("[VAL] %s", exc)
    except InvalidConfigurationError as exc:
        LOG.error("[CFG] %s", exc)
    except NetconvertExecutionError as exc:
        LOG.error("[NETCONVERT] %s", exc)
    except Exception as exc:
        LOG.exception("unexpected error: %s", exc)

if __name__ == "__main__":
    main()
