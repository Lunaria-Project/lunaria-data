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
    """
    엑셀: json/<엑셀파일명>/<시트명>.json
    CSV  : json/<csv파일명>/<csv파일명>.json
    """
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
    """types 문자열에 ';id' 꼬리표가 없으면 붙여준다."""
    if not isinstance(t, str) or t == "":
        return "int;id"
    parts = [p.strip() for p in t.split(';') if p.strip() != ""]
    if "id" not in (p.lower() for p in parts):
        parts.append("id")
    return ";".join(parts)

# ===== id_map.json 로드/저장 =====
# (변경 후 저장 구조)
# {
#   "tags": [
#     { "string": "메인_코인", "int": 1000004, "sheetName": "Sheet1", "columnName": "Id" },
#     ...
#   ],
#   "_next": 1000005
# }
#
# 내부 처리 편의를 위해 메모리에서는 아래를 유지:
#   idmap["tags"]      : { string -> int }
#   idmap["_origins"]  : { string -> {sheetName, columnName} }
#   idmap["_next"]     : int
def load_id_map() -> dict:
    if ID_MAP_PATH.exists():
        try:
            raw = json.loads(ID_MAP_PATH.read_text(encoding="utf-8"))
            raw.pop("used", None)

            idmap = {"tags": {}, "_origins": {}, "_next": ID_START}

            if isinstance(raw, dict):
                # _next
                try:
                    idmap["_next"] = int(raw.get("_next", ID_START))
                except Exception:
                    idmap["_next"] = ID_START

                tags = raw.get("tags")

                # v1 호환: {"tags": { "K": 1 }}
                if isinstance(tags, dict):
                    for k, v in tags.items():
                        if k is None:
                            continue
                        key = str(k).strip()
                        if key == "":
                            continue
                        try:
                            val = int(v)
                        except Exception:
                            continue
                        idmap["tags"][key] = val
                        if key not in idmap["_origins"]:
                            idmap["_origins"][key] = {"sheetName": "UNKNOWN", "columnName": "UNKNOWN"}

                # v2: {"tags": [ {"string":..., "int":..., "sheetName":..., "columnName":...}, ... ]}
                elif isinstance(tags, list):
                    for item in tags:
                        if not isinstance(item, dict):
                            continue
                        key = str(item.get("string", "")).strip()
                        if key == "":
                            continue
                        try:
                            val = int(item.get("int"))
                        except Exception:
                            continue

                        sheet_name = str(item.get("sheetName", "UNKNOWN")).strip() or "UNKNOWN"
                        column_name = str(item.get("columnName", "UNKNOWN")).strip() or "UNKNOWN"

                        idmap["tags"][key] = val
                        existing = idmap["_origins"].get(key)
                        if existing is None:
                            idmap["_origins"][key] = {"sheetName": sheet_name, "columnName": column_name}
                        else:
                            # UNKNOWN보다 더 구체적인 정보가 들어오면 갱신
                            if existing.get("sheetName") == "UNKNOWN" and sheet_name != "UNKNOWN":
                                existing["sheetName"] = sheet_name
                            if existing.get("columnName") == "UNKNOWN" and column_name != "UNKNOWN":
                                existing["columnName"] = column_name

                # tags가 없거나 이상한 경우
                else:
                    pass

            return idmap
        except Exception as e:
            log(f"[warn] failed to read {ID_MAP_PATH}: {e}; start fresh")

    return {"tags": {}, "_origins": {}, "_next": ID_START}

def save_id_map(idmap: dict):
    # tags를 list로 저장
    tags_dict = idmap.get("tags", {})
    origins = idmap.get("_origins", {})
    next_val = idmap.get("_next", ID_START)

    out_tags = []
    if isinstance(tags_dict, dict):
        # id 기준 정렬로 안정적인 출력
        items = []
        for k, v in tags_dict.items():
            try:
                items.append((str(k), int(v)))
            except Exception:
                continue
        items.sort(key=lambda x: x[1])

        for key, val in items:
            origin = origins.get(key) if isinstance(origins, dict) else None
            sheet_name = "UNKNOWN"
            column_name = "UNKNOWN"
            if isinstance(origin, dict):
                sheet_name = str(origin.get("sheetName", "UNKNOWN")).strip() or "UNKNOWN"
                column_name = str(origin.get("columnName", "UNKNOWN")).strip() or "UNKNOWN"

            out_tags.append(
                {
                    "string": key,
                    "int": val,
                    "sheetName": sheet_name,
                    "columnName": column_name,
                }
            )

    clean = {"tags": out_tags, "_next": int(next_val)}
    ID_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    ID_MAP_PATH.write_text(json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[id-map] saved at {ID_MAP_PATH.resolve()} (tags={len(out_tags)}, next={clean['_next']})")

def _alloc_next_free(idmap: dict) -> int:
    # 새 번호는 '이미 태그로 배정된 값'만 피해서 배정
    taken = {int(v) for v in idmap.get("tags", {}).values()} if isinstance(idmap.get("tags"), dict) else set()
    nxt = int(idmap.get("_next", ID_START))
    while nxt in taken:
        nxt += 1
    idmap["_next"] = nxt + 1
    return nxt

def _mark_used(idmap: dict, val: int):
    # 숫자 전역 중복 허용 → 기록 불필요
    return

def _must_int(v) -> int:
    if isinstance(v, int):
        return v
    return int(float(str(v)))

def _set_origin_if_needed(idmap: dict, tag: str, sheet_name: str, column_name: str):
    origins = idmap.setdefault("_origins", {})
    if not isinstance(origins, dict):
        idmap["_origins"] = {}
        origins = idmap["_origins"]

    sheet_name = (sheet_name or "UNKNOWN").strip() or "UNKNOWN"
    column_name = (column_name or "UNKNOWN").strip() or "UNKNOWN"

    existing = origins.get(tag)
    if existing is None:
        origins[tag] = {"sheetName": sheet_name, "columnName": column_name}
        return

    if not isinstance(existing, dict):
        origins[tag] = {"sheetName": sheet_name, "columnName": column_name}
        return

    # UNKNOWN 보다 구체 정보 우선
    if existing.get("sheetName") == "UNKNOWN" and sheet_name != "UNKNOWN":
        existing["sheetName"] = sheet_name
    if existing.get("columnName") == "UNKNOWN" and column_name != "UNKNOWN":
        existing["columnName"] = column_name

def map_token_to_global_id(idmap: dict, token: str, sheet_name: str, column_name: str) -> (int, bool):
    """
    token 이 "[태그]" 면 전역 tags에서 번호 반환(없으면 신규 배정) → (id, True)
    token 이 숫자면 전역 중복이어도 그대로 사용 → (id, False)

    ※ 변경점:
      - 신규 태그를 배정하는 경우 sheet/column 출처를 _origins에 기록
      - 기존 태그라도 출처가 UNKNOWN이면 보강
    """
    m = ID_TAG_RE.match(token)
    if m:
        tag = m.group(1).strip()
        if tag in idmap.get("tags", {}):
            _set_origin_if_needed(idmap, tag, sheet_name, column_name)
            return int(idmap["tags"][tag]), True

        new_id = _alloc_next_free(idmap)
        idmap.setdefault("tags", {})[tag] = new_id
        _set_origin_if_needed(idmap, tag, sheet_name, column_name)
        _mark_used(idmap, new_id)  # no-op
        return new_id, True
    else:
        val = _must_int(token)
        _mark_used(idmap, val)  # no-op
        return val, False

# ===== 숫자형 컬럼 전역 매핑 적용 + ;id 마킹 =====
def resolve_placeholders_for_numeric_columns(df: pd.DataFrame, types: dict, idmap: dict, sheet_name: str):
    """
    모든 숫자형 컬럼(int/long):
      - "" → 0
      - "[태그]" → 전역 id_map으로 숫자 매핑(불변), 이 컬럼을 '매핑 컬럼'으로 기록
      - 숫자 → 그대로 사용(전역 중복 허용)
    매핑이 '실제로 발생한' 컬럼(types[col])에는 ';id' 꼬리표를 자동으로 붙인다.

    ※ 변경점:
      - map_token_to_global_id에 sheet_name/column_name 전달
    """
    numeric_bases = {"int", "long"}
    mapped_columns = set()

    for col in df.columns:
        base = base_type_of(types.get(col, ""))
        if base not in numeric_bases:
            continue

        def map_cell(x):
            s = "" if pd.isna(x) else str(x).strip()
            if s == "":
                val, was_tag = 0, False
            else:
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
    try:
        xls = pd.ExcelFile(file_path)  # .xls 는 xlrd 필요할 수 있음
        for s in xls.sheet_names:
            # 0행: 타입, 1행: 헤더, 2행~: 데이터
            type_row   = xls.parse(s, header=None, nrows=1).iloc[0].tolist()
            header_row = xls.parse(s, header=None, skiprows=1, nrows=1).iloc[0].tolist()
            df         = xls.parse(s, header=1)

            if df.empty or df.columns.size == 0:
                log(f"  - skip empty sheet: {s}")
                continue

            types = build_types(header_row, type_row)
            resolve_placeholders_for_numeric_columns(df, types, idmap, s)

            df = df.fillna("")  # 빈칸 → ""

            rows = df.values.tolist()
            out = rel_to_out(file_path, s)
            write_json({"types": types, "rows": rows}, out)
            log(f"  - wrote {out}")
    except Exception as e:
        log(f"  ! excel fail: {file_path}\n    {e}")
        raise

def _read_csv(path, **kwargs):
    try:
        return pd.read_csv(path, **kwargs)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp949", **kwargs)

def convert_csv(file_path: pathlib.Path, idmap: dict):
    log(f"[csv] {file_path}")
    try:
        # 0행: 타입, 1행: 헤더, 2행~: 데이터
        type_row_df   = _read_csv(file_path, header=None, nrows=1)
        header_row_df = _read_csv(file_path, header=None, skiprows=1, nrows=1)
        df            = _read_csv(file_path, header=1)

        if df.empty or df.columns.size == 0:
            log("  - skip empty csv")
            return

        type_row   = type_row_df.iloc[0].tolist()
        header_row = header_row_df.iloc[0].tolist()
        types = build_types(header_row, type_row)

        # CSV는 시트 개념이 없어서 파일 stem을 sheetName으로 사용
        resolve_placeholders_for_numeric_columns(df, types, idmap, file_path.stem)

        df = df.fillna("")

        rows = df.values.tolist()
        out = rel_to_out(file_path, None)
        write_json({"types": types, "rows": rows}, out)
        log(f"  - wrote {out}")
    except Exception as e:
        log(f"  ! csv fail: {file_path}\n    {e}")
        raise

# ===== 대상 수집 =====
def collect_from_diff(diff_env: str):
    tgt = []
    if not diff_env:
        return tgt
    for line in diff_env.splitlines():
        q = pathlib.Path(line.strip()).resolve()
        if not q.exists() or is_temp_excel(q.name):
            continue
        if q.suffix.lower() in (EXCEL_EXTS | CSV_EXTS):
            tgt.append(q)
    return tgt

def collect_full():
    tgt = []
    for p in ROOT.rglob("*"):
        if p.is_file() and not is_temp_excel(p.name):
            if p.suffix.lower() in (EXCEL_EXTS | CSV_EXTS):
                tgt.append(p.resolve())
    return tgt

# ===== 메인 =====
def main():
    log(f"cwd = {ROOT}")
    log(f"id_map path = {ID_MAP_PATH.resolve()}")

    diff = os.environ.get("GIT_DIFF_FILES")
    targets = collect_from_diff(diff) if diff else collect_full()

    idmap = load_id_map()
    log(f"[id-map] loaded (tags={len(idmap.get('tags', {}))}, next={idmap.get('_next')})")

    if not targets and diff:
        log("[info] no targets from diff -> full repository scan")
        targets = collect_full()

    if not targets:
        log("[info] no targets; ensuring id_map.json exists")
        save_id_map(idmap)
        return

    try:
        for p in targets:
            if p.suffix.lower() in EXCEL_EXTS:
                convert_excel(p, idmap)
            elif p.suffix.lower() in CSV_EXTS:
                convert_csv(p, idmap)
    finally:
        save_id_map(idmap)

if __name__ == "__main__":
    main()
