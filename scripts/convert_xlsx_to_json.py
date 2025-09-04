#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, json, pathlib, pandas as pd

ROOT = pathlib.Path(".").resolve()
OUT_DIR = ROOT / "json"

EXCEL_EXTS = {".xlsx", ".xlsm", ".xls"}
CSV_EXTS   = {".csv"}

def is_temp_excel(name: str) -> bool:
    return name.startswith("~$")

def safe_rel(path: pathlib.Path, start: pathlib.Path) -> pathlib.Path:
    return pathlib.Path(os.path.relpath(path.resolve(), start.resolve()))

def safe_name(s: str) -> str:
    """파일/폴더 이름에 부적합한 문자를 안전하게 치환"""
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

def convert_excel(file_path: pathlib.Path):
    print(f"[excel] {file_path}")
    try:
        xls = pd.ExcelFile(file_path)  # .xls는 xlrd 필요할 수 있음
        for s in xls.sheet_names:
            # 0행: 타입, 1행: 헤더, 2행~: 데이터
            type_row   = xls.parse(s, header=None, nrows=1).iloc[0].tolist()
            header_row = xls.parse(s, header=None, skiprows=1, nrows=1).iloc[0].tolist()
            df         = xls.parse(s, header=1)

            if df.empty or df.columns.size == 0:
                print(f"  - skip empty sheet: {s}")
                continue

            types = build_types(header_row, type_row)
            df = df.fillna("")
            rows  = df.values.tolist()

            out = rel_to_out(file_path, s)
            write_json({"types": types, "rows": rows}, out)
            print(f"  - wrote {out}")
    except Exception as e:
        print(f"  ! excel fail: {file_path}\n    {e}")

def _read_csv(path, **kwargs):
    try:
        return pd.read_csv(path, **kwargs)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp949", **kwargs)

def convert_csv(file_path: pathlib.Path):
    print(f"[csv] {file_path}")
    try:
        # 0행: 타입, 1행: 헤더, 2행~: 데이터
        type_row_df   = _read_csv(file_path, header=None, nrows=1)
        header_row_df = _read_csv(file_path, header=None, skiprows=1, nrows=1)
        df            = _read_csv(file_path, header=1)
        df = df.fillna("")

        if df.empty or df.columns.size == 0:
            print("  - skip empty csv")
            return

        type_row   = type_row_df.iloc[0].tolist()
        header_row = header_row_df.iloc[0].tolist()
        types = build_types(header_row, type_row)
        rows  = df.values.tolist()

        out = rel_to_out(file_path, None)
        write_json({"types": types, "rows": rows}, out)
        print(f"  - wrote {out}")
    except Exception as e:
        print(f"  ! csv fail: {file_path}\n    {e}")

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

def main():
    diff = os.environ.get("GIT_DIFF_FILES")
    targets = collect_from_diff(diff) if diff else collect_full()
    # ✅ 안전망: diff가 있었는데도 타깃이 0개면 전체 스캔으로 폴백
    if not targets and diff:
        print("[info] no targets from diff -> full repository scan")
        targets = collect_full()
    if not targets:
        print("[info] no targets")
        return
    for p in targets:
        if p.suffix.lower() in EXCEL_EXTS:
            convert_excel(p)
        elif p.suffix.lower() in CSV_EXTS:
            convert_csv(p)

if __name__ == "__main__":
    main()
