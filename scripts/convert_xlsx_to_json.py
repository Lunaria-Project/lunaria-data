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
ID_MAP_PATH = pathlib.Path(os.environ.get("ID_MAP_PATH", "id_map.json"))  # 전역 매핑 JSON (레포 루트)
ID_START    = int(os.environ.get("ID_START", "100000"))                   # 새 ID 시작 번호
ID_TAG_RE   = re.compile(r"^\[(.+?)\]$")                                  # [아이디_이렇게]

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

# ===== 전역 매핑(id_map.json) 로드/저장 =====
# 구조:
# {
#   "tags": { "테스트데이터_기사": 1001, ... },
#   "used": [1001, 1002, 12345],
#   "_next": 1003
# }
def load_id_map() -> dict:
    if ID_MAP_PATH.exists():
        try:
            m = json.loads(ID_MAP_PATH.read_text(encoding="utf-8"))
            m.setdefault("tags", {})
            m.setdefault("used", [])
            m.setdefault("_next", ID_START)
            return m
        except Exception as e:
            log(f"[warn] failed to read {ID_MAP_PATH}: {e}; start fresh")
    return {"tags": {}, "used": [], "_next": ID_START}

def save_id_map(idmap: dict):
    # 폴더 보장 후 저장
    ID_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    ID_MAP_PATH.write_text(json.dumps(idmap, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[id-map] saved at {ID_MAP_PATH.resolve()} (tags={len(idmap.get('tags', {}))}, used={len(idmap.get('used', []))})")

def _alloc_next_free(idmap: dict) -> int:
    used_tags_vals = {int(v) for v in idmap.get("tags", {}).values()}
    used_numbers   = set(int(x) for x in idmap.get("used", []))
    used_all       = used_tags_vals | used_numbers
    nxt = int(idmap.get("_next", ID_START))
    while nxt in used_all:
        nxt += 1
    idmap["_next"] = nxt + 1
    return nxt

def _mark_used(idmap: dict, val: int):
    val = int(val)
    tags_vals = {int(v) for v in idmap.get("tags", {}).values()}
    used_list = idmap.get("used", [])
    if val not in tags_vals and val not in used_list:
        used_list.append(val)
        idmap["used"] = used_list

def _must_int(v) -> int:
    if isinstance(v, int):
        return v
    return int(float(str(v)))

def map_token_to_global_id(idmap: dict, token: str) -> int:
    """
    token 이 "[태그]" 면 전역 tags에서 번호 반환(없으면 신규 배정).
    token 이 숫자면 전역 used/tags와 충돌 없는지 검사 후 사용.
    """
    m = ID_TAG_RE.match(token)
    if m:
        tag = m.group(1).strip()
        if tag in idmap["tags"]:
            return int(idmap["tags"][tag])
        new_id = _alloc_next_free(idmap)
        idmap["tags"][tag] = new_id
        _mark_used(idmap, new_id)
        return new_id
    else:
        val = _must_int(token)
        tags_vals = {int(v) for v in idmap["tags"].values()}
        used_vals = set(int(x) for x in idmap["used"])
        if val in tags_vals or val in used_vals:
            raise ValueError(f"[id-map] number {val} is already used globally")
        _mark_used(idmap, val)
        return val

# ===== 숫자형 컬럼 전역 매핑 적용 =====
def resolve_placeholders_for_numeric_columns(df: pd.DataFrame, types: dict, idmap: dict):
    """
    모든 숫자형 컬럼(int/long):
      - 값이 ""(빈칸) → 0 (정책상 에러로 바꾸려면 여기서 raise)
      - "[태그]" → 전역 id_map으로 숫자 매핑(불변)
      - 숫자 → 전역 충돌 검사 후 사용
    """
    numeric_bases = {"int", "long"}
    for col in df.columns:
        base = base_type_of(types.get(col, ""))
        if base not in numeric_bases:
            continue

        seen_in_df = set()
        def map_cell(x):
            s = "" if pd.isna(x) else str(x).strip()
            if s == "":
                val = 0
            else:
                val = map_token_to_global_id(idmap, s)
            if val in seen_in_df:
                raise ValueError(f"[id-map] duplicated value {val} within column '{col}' in this batch")
            seen_in_df.add(val)
            return val

        df[col] = df[col].apply(map_cell).astype("int64")

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

            resolve_placeholders_for_numeric_columns(df, types, idmap)

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

        resolve_placeholders_for_numeric_columns(df, types, idmap)

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
    for line in diff_env.splitlines():
        q = pathlib.Path(line.strip()).resolve()
        if not q.exists():
            continue
        if is_temp_excel(q.name):
            continue
        suf = q.suffix.lower()
        if suf in EXCEL_EXTS or suf in CSV_EXTS:
            tgt.append(q)
    return tgt

def collect_full():
    tgt = []
    for p in ROOT.rglob("*"):
        if p.is_file() and not is_temp_excel(p.name):
            suf = p.suffix.lower()
            if suf in EXCEL_EXTS or suf in CSV_EXTS:
                tgt.append(p.resolve())
    return tgt

# ===== 메인 =====
def main():
    log(f"cwd = {ROOT}")
    log(f"id_map path = {ID_MAP_PATH.resolve()}")

    diff = os.environ.get("GIT_DIFF_FILES")
    targets = collect_from_diff(diff) if diff else collect_full()

    # 전역 매핑 로드 (없으면 새로 만듦)
    idmap = load_id_map()
    log(f"[id-map] loaded (tags={len(idmap.get('tags', {}))}, used={len(idmap.get('used', []))}, next={idmap.get('_next')})")

    if not targets and diff:
        log("[info] no targets from diff -> full repository scan")
        targets = collect_full()

    if not targets:
        log("[info] no targets; still ensuring id_map.json exists")
        save_id_map(idmap)
        return

    try:
        for p in targets:
            if p.suffix.lower() in EXCEL_EXTS:
                convert_excel(p, idmap)
            elif p.suffix.lower() in CSV_EXTS:
                convert_csv(p, idmap)
    finally:
        # 변환 성공/실패와 무관하게 항상 저장
        save_id_map(idmap)

if __name__ == "__main__":
    main()
