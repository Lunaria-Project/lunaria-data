#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, json, re, pathlib, pandas as pd
from datetime import datetime

# ===== 기본 경로/대상 확장자 =====
ROOT = pathlib.Path(".").resolve()
OUT_DIR = ROOT / "json"
OUT_DIR.mkdir(parents=True, exist_ok=True)

EXCEL_EXTS = {".xlsx", ".xlsm", ".xls"}
CSV_EXTS   = {".csv"}

# ===== 전역 매핑 설정 =====
ID_MAP_PATH = pathlib.Path(os.environ.get("ID_MAP_PATH", "id_map.json"))
ID_START    = int(os.environ.get("ID_START", "1000000"))
ID_TAG_RE   = re.compile(r"^\[(.+?)\]$")

# ===== 유틸 =====
def log(msg: str):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    print(f"[convert] {ts} {msg}")

def is_temp_excel(name: str) -> bool:
    return name.startswith("~$")

def safe_name(s: str) -> str:
    keep = []
    for ch in str(s):
        if ch.isalnum() or ch in ("_", "-", " "):
            keep.append(ch)
        else:
            keep.append("_")
    name = "".join(keep).strip()
    return name or "_"

def rel_to_out(path: pathlib.Path, sheet: str | None = None) -> pathlib.Path:
    stem = safe_name(path.stem)
    base_dir = OUT_DIR / stem
    if sheet:
        sheet = safe_name(sheet)
        file_name = f"{sheet}.json"
    else:
        file_name = f"{stem}.json"
    return base_dir / file_name

def write_json(obj, out_path: pathlib.Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def build_types(header_row, type_row):
    types = {}
    for col, typ in zip(header_row, type_row):
        col = "" if pd.isna(col) else str(col)
        types[col] = None if pd.isna(typ) else str(typ)
    return types

def base_type_of(t):
    if not isinstance(t, str):
        return ""
    return t.split(';', 1)[0].strip().lower()

def append_id_marker(t: str) -> str:
    if not isinstance(t, str) or t == "":
        return "int;id"
    parts = [p.strip() for p in t.split(';') if p.strip()]
    if "id" not in (p.lower() for p in parts):
        parts.append("id")
    return ";".join(parts)

# =========================================================
# id_map.json (tags=list, _next 제거, max+1 방식)
# =========================================================
# 파일 저장 구조:
# {
#   "tags": [
#     { "string": "...", "int": 1000000, "sheetName": "...", "columnName": "..." }
#   ]
# }
#
# 메모리 내부 구조:
#   idmap["tags"]     : { string -> int }
#   idmap["_origins"] : { string -> {sheetName, columnName} }
def load_id_map() -> dict:
    idmap = {"tags": {}, "_origins": {}}

    if not ID_MAP_PATH.exists():
        return idmap

    try:
        raw = json.loads(ID_MAP_PATH.read_text(encoding="utf-8"))
        tags = raw.get("tags")

        # v1 호환: {"tags": { "K": 1 }}
        if isinstance(tags, dict):
            for k, v in tags.items():
                key = str(k).strip()
                if not key:
                    continue
                try:
                    val = int(v)
                except Exception:
                    continue
                idmap["tags"][key] = val
                idmap["_origins"][key] = {"sheetName": "UNKNOWN", "columnName": "UNKNOWN"}

        # v2: {"tags": [ {...}, ... ]}
        elif isinstance(tags, list):
            for item in tags:
                if not isinstance(item, dict):
                    continue
                key = str(item.get("string", "")).strip()
                if not key:
                    continue
                try:
                    val = int(item.get("int"))
                except Exception:
                    continue

                sheet_name  = str(item.get("sheetName", "UNKNOWN")).strip() or "UNKNOWN"
                column_name = str(item.get("columnName", "UNKNOWN")).strip() or "UNKNOWN"

                idmap["tags"][key] = val
                idmap["_origins"][key] = {
                    "sheetName": sheet_name,
                    "columnName": column_name,
                }

    except Exception as e:
        log(f"[warn] failed to read {ID_MAP_PATH}: {e}; start fresh")

    return idmap

def save_id_map(idmap: dict):
    tags = idmap.get("tags", {})
    origins = idmap.get("_origins", {})

    out_tags = []
    items = sorted(tags.items(), key=lambda x: x[1])

    for key, val in items:
        origin = origins.get(key, {})
        out_tags.append(
            {
                "string": key,
                "int": int(val),
                "sheetName": str(origin.get("sheetName", "UNKNOWN")),
                "columnName": str(origin.get("columnName", "UNKNOWN")),
            }
        )

    payload = {"tags": out_tags}
    ID_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    ID_MAP_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[id-map] saved at {ID_MAP_PATH.resolve()} (tags={len(out_tags)})")

def _next_id(idmap: dict) -> int:
    tags = idmap.get("tags", {})
    if not tags:
        return ID_START
    return max(tags.values()) + 1

def _set_origin(idmap: dict, tag: str, sheet_name: str, column_name: str):
    origins = idmap.setdefault("_origins", {})
    if tag not in origins or origins[tag]["sheetName"] == "UNKNOWN":
        origins[tag] = {
            "sheetName": sheet_name or "UNKNOWN",
            "columnName": column_name or "UNKNOWN",
        }

def map_token_to_global_id(idmap: dict, token: str, sheet_name: str, column_name: str) -> (int, bool):
    m = ID_TAG_RE.match(token)
    if m:
        tag = m.group(1).strip()
        if tag in idmap["tags"]:
            _set_origin(idmap, tag, sheet_name, column_name)
            return idmap["tags"][tag], True

        new_id = _next_id(idmap)
        idmap["tags"][tag] = new_id
        _set_origin(idmap, tag, sheet_name, column_name)
        return new_id, True

    return int(float(token)), False

# ===== 숫자형 컬럼 전역 매핑 적용 + ;id 마킹 =====
def resolve_placeholders_for_numeric_columns(df: pd.DataFrame, types: dict, idmap: dict, sheet_name: str):
    numeric_bases = {"int", "long"}
    mapped_columns = set()

    for col in df.columns:
        if base_type_of(types.get(col, "")) not in numeric_bases:
            continue

        def map_cell(x):
            s = "" if pd.isna(x) else str(x).strip()
            if s == "":
                return 0
            val, was_tag = map_token_to_global_id(idmap, s, sheet_name, str(col))
            if was_tag:
                mapped_columns.add(col)
            return val

        df[col] = df[col].apply(map_cell).astype("int64")

    for col in mapped_columns:
        types[col] = append_id_marker(types.get(col, "int"))

# ===== 변환기 =====
def convert_excel(file_path: pathlib.Path, idmap: dict):
    log(f"[excel] {file_path}")
    xls = pd.ExcelFile(file_path)
    for s in xls.sheet_names:
        type_row   = xls.parse(s, header=None, nrows=1).iloc[0].tolist()
        header_row = xls.parse(s, header=None, skiprows=1, nrows=1).iloc[0].tolist()
        df         = xls.parse(s, header=1)

        if df.empty:
            continue

        types = build_types(header_row, type_row)
        resolve_placeholders_for_numeric_columns(df, types, idmap, s)

        rows = df.fillna("").values.tolist()
        out = rel_to_out(file_path, s)
        write_json({"types": types, "rows": rows}, out)
        log(f"  - wrote {out}")

def _read_csv(path, **kwargs):
    try:
        return pd.read_csv(path, **kwargs)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp949", **kwargs)

def convert_csv(file_path: pathlib.Path, idmap: dict):
    log(f"[csv] {file_path}")
    type_row   = _read_csv(file_path, header=None, nrows=1).iloc[0].tolist()
    header_row = _read_csv(file_path, header=None, skiprows=1, nrows=1).iloc[0].tolist()
    df         = _read_csv(file_path, header=1)

    if df.empty:
        return

    types = build_types(header_row, type_row)
    resolve_placeholders_for_numeric_columns(df, types, idmap, file_path.stem)

    rows = df.fillna("").values.tolist()
    out = rel_to_out(file_path, None)
    write_json({"types": types, "rows": rows}, out)
    log(f"  - wrote {out}")

# ===== 대상 수집 =====
def collect_from_diff(diff_env: str):
    tgt = []
    for line in diff_env.splitlines():
        p = pathlib.Path(line.strip())
        if p.exists() and p.suffix.lower() in (EXCEL_EXTS | CSV_EXTS):
            tgt.append(p.resolve())
    return tgt

def collect_full():
    return [
        p.resolve()
        for p in ROOT.rglob("*")
        if p.is_file() and p.suffix.lower() in (EXCEL_EXTS | CSV_EXTS) and not is_temp_excel(p.name)
    ]

# ===== 메인 =====
def main():
    log(f"id_map path = {ID_MAP_PATH.resolve()}")
    diff = os.environ.get("GIT_DIFF_FILES")
    targets = collect_from_diff(diff) if diff else collect_full()

    idmap = load_id_map()
    log(f"[id-map] loaded (tags={len(idmap['tags'])})")

    if not targets:
        save_id_map(idmap)
        return

    for p in targets:
        if p.suffix.lower() in EXCEL_EXTS:
            convert_excel(p, idmap)
        elif p.suffix.lower() in CSV_EXTS:
            convert_csv(p, idmap)

    save_id_map(idmap)

if __name__ == "__main__":
    main()
