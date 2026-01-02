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

ID_TAG_RE       = re.compile(r"^\[(.+?)\]$")          # 셀 전체가 [TAG]
ID_TAG_INNER_RE = re.compile(r"\[([^\[\]]+)\]")       # 문자열 내부의 [TAG]

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
# id_map.json 로딩/저장
# =========================================================
def load_id_map() -> dict:
    idmap = {"tags": {}, "_origins": {}}

    if not ID_MAP_PATH.exists():
        return idmap

    try:
        raw = json.loads(ID_MAP_PATH.read_text(encoding="utf-8"))
        tags = raw.get("tags")

        if isinstance(tags, dict):
            for k, v in tags.items():
                idmap["tags"][str(k)] = int(v)

        elif isinstance(tags, list):
            for item in tags:
                key = str(item.get("string", "")).strip()
                if not key:
                    continue
                idmap["tags"][key] = int(item.get("int"))

    except Exception as e:
        log(f"[warn] failed to read {ID_MAP_PATH}: {e}; start fresh")

    return idmap

def save_id_map(idmap: dict):
    out = [{"string": k, "int": v} for k, v in sorted(idmap["tags"].items(), key=lambda x: x[1])]
    ID_MAP_PATH.write_text(json.dumps({"tags": out}, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[id-map] saved ({len(out)})")

def _next_id(idmap: dict) -> int:
    return max(idmap["tags"].values(), default=ID_START - 1) + 1

def map_token(idmap: dict, tag: str) -> int:
    if tag in idmap["tags"]:
        return idmap["tags"][tag]
    new_id = _next_id(idmap)
    idmap["tags"][tag] = new_id
    return new_id

# =========================================================
# 문자열 내부 [TAG] 전역 치환
# =========================================================
def replace_inner_tags(text: str, idmap: dict) -> str:
    if not isinstance(text, str) or "[" not in text:
        return text

    def _repl(m: re.Match):
        tag = m.group(1).strip()
        return str(map_token(idmap, tag))

    return ID_TAG_INNER_RE.sub(_repl, text)

# =========================================================
# 숫자형 컬럼 처리
# =========================================================
def resolve_placeholders_for_numeric_columns(df: pd.DataFrame, types: dict, idmap: dict):
    numeric_bases = {"int", "long"}

    for col in df.columns:
        if base_type_of(types.get(col, "")) not in numeric_bases:
            continue

        def map_cell(x):
            s = "" if pd.isna(x) else str(x).strip()
            if not s:
                return 0

            m = ID_TAG_RE.match(s)
            if m:
                return map_token(idmap, m.group(1).strip())

            return int(float(s))

        df[col] = df[col].apply(map_cell).astype("int64")
        types[col] = append_id_marker(types.get(col, "int"))

# =========================================================
# 변환기
# =========================================================
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
        resolve_placeholders_for_numeric_columns(df, types, idmap)

        # ✅ 문자열 컬럼 전체에 [TAG] 치환 적용
        for col in df.columns:
            if base_type_of(types.get(col, "")) in ("string", "list<string>", "list<int>"):
                df[col] = df[col].apply(lambda x: replace_inner_tags("" if pd.isna(x) else str(x), idmap))

        rows = df.fillna("").values.tolist()
        out = rel_to_out(file_path, s)
        write_json({"types": types, "rows": rows}, out)
        log(f"  - wrote {out}")

def convert_csv(file_path: pathlib.Path, idmap: dict):
    log(f"[csv] {file_path}")
    df = pd.read_csv(file_path)

    if df.empty:
        return

    for col in df.columns:
        df[col] = df[col].apply(lambda x: replace_inner_tags("" if pd.isna(x) else str(x), idmap))

    rows = df.fillna("").values.tolist()
    out = rel_to_out(file_path, None)
    write_json({"types": {}, "rows": rows}, out)
    log(f"  - wrote {out}")

# ===== 메인 =====
def main():
    idmap = load_id_map()
    targets = [p for p in ROOT.rglob("*") if p.suffix.lower() in (EXCEL_EXTS | CSV_EXTS) and not is_temp_excel(p.name)]

    for p in targets:
        if p.suffix.lower() in EXCEL_EXTS:
            convert_excel(p, idmap)
        elif p.suffix.lower() in CSV_EXTS:
            convert_csv(p, idmap)

    save_id_map(idmap)

if __name__ == "__main__":
    main()
