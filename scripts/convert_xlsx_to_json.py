#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, json, pathlib, pandas as pd

ROOT = pathlib.Path(".").resolve()
OUT_DIR = ROOT / "json"

EXCEL_EXTS = {".xlsx", ".xlsm", ".xls"}
CSV_EXTS = {".csv"}

def is_temp_excel(name: str) -> bool:
    return name.startswith("~$")

def safe_rel(path: pathlib.Path, start: pathlib.Path) -> pathlib.Path:
    return pathlib.Path(os.path.relpath(path.resolve(), start.resolve()))

def rel_to_out(path: pathlib.Path, sheet: str | None = None) -> pathlib.Path:
    rel = safe_rel(path, ROOT)
    stem, parent = rel.stem, rel.parent
    if sheet:
        sheet = sheet.replace("/", "_").replace("\\", "_")
        name = f"{stem}__{sheet}.json"
    else:
        name = f"{stem}.json"
    return OUT_DIR / parent / name

def write_df(df: pd.DataFrame, out_path: pathlib.Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    data = json.loads(df.to_json(orient="records", force_ascii=False))
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def convert_excel(p: pathlib.Path):
    print(f"[excel] {p}")
    try:
        xls = pd.ExcelFile(p)  # .xls 필요 시 xlrd 설치 + engine="xlrd"
        for s in xls.sheet_names:
            df = xls.parse(s)
            if df.empty or df.columns.size == 0:
                print(f"  - skip empty sheet: {s}")
                continue
            out = rel_to_out(p, s)
            write_df(df, out)
            print(f"  - wrote {out}")
    except Exception as e:
        print(f"  ! excel fail: {p}\n    {e}")

def convert_csv(p: pathlib.Path):
    print(f"[csv] {p}")
    try:
        try:
            df = pd.read_csv(p)
        except UnicodeDecodeError:
            df = pd.read_csv(p, encoding="cp949")
        if df.empty or df.columns.size == 0:
            print("  - skip empty csv")
            return
        out = rel_to_out(p, None)
        write_df(df, out)
        print(f"  - wrote {out}")
    except Exception as e:
        print(f"  ! csv fail: {p}\n    {e}")

def collect_from_diff(diff_env: str):
    tgt = []
    for line in diff_env.splitlines():
        q = pathlib.Path(line.strip()).resolve()
        if not q.exists():  # 삭제/리네임 등
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
    if not targets:
        print("[info] no targets")
        return
    for p in targets:
        if p.suffix.lower() in EXCEL_EXTS: convert_excel(p)
        elif p.suffix.lower() in CSV_EXTS: convert_csv(p)

if __name__ == "__main__":
    main()
