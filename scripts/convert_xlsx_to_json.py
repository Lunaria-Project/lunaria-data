#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import re
import pathlib
from datetime import datetime

import pandas as pd

# ===== 기본 경로/대상 확장자 =====
ROOT = pathlib.Path(".").resolve()
OUT_DIR = ROOT / "json"
OUT_DIR.mkdir(parents=True, exist_ok=True)

EXCEL_EXTS = {".xlsx", ".xlsm", ".xls"}
CSV_EXTS = {".csv"}

# ===== 전역 매핑 설정 =====
ID_MAP_PATH = pathlib.Path(os.environ.get("ID_MAP_PATH", "id_map.json"))
ID_START = int(os.environ.get("ID_START", "1000000"))

# 셀 전체가 [TAG]
ID_TAG_RE = re.compile(r"^\[(.+?)\]$")
# 문자열 내부의 [TAG]들 전부
ID_TAG_INNER_RE = re.compile(r"\[([^\[\]]+)\]")


def log(message: str):
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    print(f"[convert] {timestamp} {message}")


def is_temp_excel(name: str) -> bool:
    return name.startswith("~$")


def safe_name(value: str) -> str:
    keep = []
    for ch in str(value):
        if ch.isalnum() or ch in ("_", "-", " "):
            keep.append(ch)
        else:
            keep.append("_")
    name = "".join(keep).strip()
    return name or "_"


def rel_to_out(path: pathlib.Path, sheet: str | None) -> pathlib.Path:
    stem = safe_name(path.stem)
    base_dir = OUT_DIR / stem
    if sheet:
        sheet_name = safe_name(sheet)
        file_name = f"{sheet_name}.json"
    else:
        file_name = f"{stem}.json"
    return base_dir / file_name


def write_json(obj: dict, out_path: pathlib.Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def build_types(header_row, type_row):
    types = {}
    for col, typ in zip(header_row, type_row):
        col_name = "" if pd.isna(col) else str(col)
        types[col_name] = None if pd.isna(typ) else str(typ)
    return types


def base_type_of(type_str) -> str:
    if not isinstance(type_str, str):
        return ""
    return type_str.split(";", 1)[0].strip().lower()


def append_id_marker(type_str: str) -> str:
    if not isinstance(type_str, str) or type_str == "":
        type_str = "int"
    parts = [p.strip() for p in type_str.split(";") if p.strip()]
    if not any(p.lower() == "id" for p in parts):
        parts.append("id")
    return ";".join(parts)


def remove_id_marker(type_str):
    if not isinstance(type_str, str):
        return type_str
    parts = [p.strip() for p in type_str.split(";") if p.strip()]
    parts = [p for p in parts if p.lower() != "id"]
    return ";".join(parts)


# =========================================================
# id_map.json 로딩/저장
# =========================================================
def load_id_map() -> dict:
    idmap = {"tags": {}}

    if not ID_MAP_PATH.exists():
        return idmap

    try:
        raw = json.loads(ID_MAP_PATH.read_text(encoding="utf-8"))
        tags = raw.get("tags")

        # 레거시 dict 포맷 지원: { "tags": { "TAG": 1000000, ... } }
        if isinstance(tags, dict):
            for key, value in tags.items():
                idmap["tags"][str(key)] = int(value)

        # 현재 list 포맷 지원: { "tags": [ {"string": "...", "int": ...}, ... ] }
        elif isinstance(tags, list):
            for item in tags:
                tag = str(item.get("string", "")).strip()
                if not tag:
                    continue
                idmap["tags"][tag] = int(item.get("int"))

    except Exception as e:
        log(f"[warn] failed to read {ID_MAP_PATH}: {e}; start fresh")

    return idmap


def save_id_map(idmap: dict):
    tags_list = [{"string": k, "int": v} for k, v in sorted(idmap["tags"].items(), key=lambda x: x[1])]
    ID_MAP_PATH.write_text(json.dumps({"tags": tags_list}, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[id-map] saved ({len(tags_list)})")


def next_id(idmap: dict) -> int:
    return max(idmap["tags"].values(), default=ID_START - 1) + 1


def map_token(idmap: dict, tag: str) -> int:
    if tag in idmap["tags"]:
        return idmap["tags"][tag]
    new_id = next_id(idmap)
    idmap["tags"][tag] = new_id
    return new_id


# =========================================================
# 문자열 내부 [TAG] 전역 치환
# =========================================================
def replace_inner_tags(text: str, idmap: dict) -> str:
    if not isinstance(text, str) or "[" not in text:
        return text

    def repl(match: re.Match) -> str:
        tag = match.group(1).strip()
        if not tag:
            return match.group(0)
        return str(map_token(idmap, tag))

    return ID_TAG_INNER_RE.sub(repl, text)


# =========================================================
# 숫자형 컬럼: 셀 전체가 [TAG] 인 케이스를 숫자로 치환
# + 이번 실행에서 변환이 실제로 발생한 컬럼에만 ;id 부착
# + 변환이 없었던 컬럼은 ;id 제거(이미 오염된 것 원복)
# =========================================================
def resolve_placeholders_for_numeric_columns(df: pd.DataFrame, types: dict, idmap: dict):
    numeric_bases = {"int", "long"}

    for col in df.columns:
        original_type = types.get(col, "")
        if base_type_of(original_type) not in numeric_bases:
            continue

        converted_placeholder = False  # ✅ 이번 실행에서 이 컬럼에 [TAG]가 실제로 있었는지

        def map_cell(x):
            nonlocal converted_placeholder

            s = "" if pd.isna(x) else str(x).strip()
            if not s:
                return 0

            m = ID_TAG_RE.match(s)
            if m:
                converted_placeholder = True
                return map_token(idmap, m.group(1).strip())

            # 일반 숫자는 그대로
            return int(float(s))

        df[col] = df[col].apply(map_cell).astype("int64")

        # ✅ 규칙: placeholder 변환이 "실제로 발생한 컬럼"에만 ;id
        if converted_placeholder:
            types[col] = append_id_marker(original_type)
        else:
            types[col] = remove_id_marker(original_type)


# =========================================================
# 변환기
# =========================================================
def convert_excel(file_path: pathlib.Path, idmap: dict):
    log(f"[excel] {file_path}")
    xls = pd.ExcelFile(file_path)

    for sheet_name in xls.sheet_names:
        # 0행: 타입, 1행: 헤더, 2행부터 데이터(= pandas header=1)
        type_row = xls.parse(sheet_name, header=None, nrows=1).iloc[0].tolist()
        header_row = xls.parse(sheet_name, header=None, skiprows=1, nrows=1).iloc[0].tolist()
        df = xls.parse(sheet_name, header=1)

        if df.empty:
            continue

        types = build_types(header_row, type_row)

        # 1) 숫자형 컬럼에서 셀 전체 [TAG] -> 숫자 치환 + ;id 규칙 적용
        resolve_placeholders_for_numeric_columns(df, types, idmap)

        # 2) 문자열/리스트 문자열에서 문자열 내부 [TAG] 전부 치환
        #    (요구사항: "[A],[B]" 같이 콤마로 이어진 것도 치환)
        for col in df.columns:
            base = base_type_of(types.get(col, ""))
            if base in ("string", "list<string>", "list<int>"):
                df[col] = df[col].apply(lambda x: replace_inner_tags("" if pd.isna(x) else str(x), idmap))

        rows = df.fillna("").values.tolist()
        out_path = rel_to_out(file_path, sheet_name)
        write_json({"types": types, "rows": rows}, out_path)
        log(f"  - wrote {out_path}")


def convert_csv(file_path: pathlib.Path, idmap: dict):
    log(f"[csv] {file_path}")
    df = pd.read_csv(file_path)

    if df.empty:
        return

    for col in df.columns:
        df[col] = df[col].apply(lambda x: replace_inner_tags("" if pd.isna(x) else str(x), idmap))

    rows = df.fillna("").values.tolist()
    out_path = rel_to_out(file_path, None)
    write_json({"types": {}, "rows": rows}, out_path)
    log(f"  - wrote {out_path}")


def main():
    idmap = load_id_map()

    targets = [
        p for p in ROOT.rglob("*")
        if p.suffix.lower() in (EXCEL_EXTS | CSV_EXTS)
        and not is_temp_excel(p.name)
    ]

    for path in targets:
        if path.suffix.lower() in EXCEL_EXTS:
            convert_excel(path, idmap)
        elif path.suffix.lower() in CSV_EXTS:
            convert_csv(path, idmap)

    save_id_map(idmap)


if __name__ == "__main__":
    main()
