#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

_LOCAL_STRING_BASE = "local_string"
_REF_RE = re.compile(r"\[([^\[\]]+)\]")


def _base_type(type_str: Any) -> str:
    if not isinstance(type_str, str):
        return ""
    return type_str.split(";", 1)[0].strip().lower()


def _ref_columns(type_str: Any) -> list[str]:
    if not isinstance(type_str, str):
        return []
    parts = type_str.split(";", 1)
    if len(parts) < 2:
        return []
    return [m.group(1).strip() for m in _REF_RE.finditer(parts[1]) if m.group(1).strip()]


def _iter_sheet_json_files(json_root: Path):
    if not json_root.exists():
        return
    for p in json_root.rglob("*.json"):
        rel = p.relative_to(json_root)
        if len(rel.parts) == 1:
            yield p.stem, p.stem, p
        else:
            yield rel.parts[0], p.stem, p


def _read_json(path: Path, warnings: list[str]) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        warnings.append(f"Failed to read json: {path} ({e})")
        return None


def _write_json(path: Path, data: dict, warnings: list[str]):
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        warnings.append(f"Failed to write json: {path} ({e})")


def build_localdata_and_rewrite(json_root: Path, rewrite_json: bool):
    warnings: list[str] = []
    localdata_rows: list[dict] = []
    localdata_json: dict[str, dict[str, str]] = {}
    seen_keys: set[str] = set()
    rewritten_files = 0

    for file_stem, sheet_name, json_path in sorted(_iter_sheet_json_files(json_root)):
        data = _read_json(json_path, warnings)
        if data is None:
            continue

        types = data.get("types")
        rows = data.get("rows")
        if not isinstance(types, dict) or not isinstance(rows, list):
            continue

        columns = list(types.keys())
        column_index = {name: i for i, name in enumerate(columns)}

        local_columns = [
            (col, _ref_columns(t))
            for col, t in types.items()
            if _base_type(t) == _LOCAL_STRING_BASE
        ]

        if not local_columns:
            continue

        modified = False

        for row_i, row in enumerate(rows):
            if not isinstance(row, list):
                continue

            for col, refs in local_columns:
                col_idx = column_index.get(col)
                if col_idx is None:
                    continue

                original = row[col_idx] if col_idx < len(row) else ""
                if original in (None, ""):
                    continue

                key_parts = [file_stem, sheet_name]

                if refs:
                    missing = False
                    for ref in refs:
                        ref_idx = column_index.get(ref)
                        if ref_idx is None:
                            warnings.append(f"[{file_stem}.{sheet_name}] unknown ref column '{ref}' in {json_path}")
                            missing = True
                            break
                        ref_val = row[ref_idx] if ref_idx < len(row) else ""
                        if ref_val in (None, ""):
                            warnings.append(f"[{file_stem}.{sheet_name}] empty ref '{ref}' row {row_i} in {json_path}")
                            missing = True
                            break
                        key_parts.append(str(ref_val).strip())
                    if missing:
                        continue
                else:
                    key_parts.append(str(row_i + 1))

                key = ".".join(key_parts)

                # 이미 key로 치환되어 있으면 skip
                if isinstance(original, str) and original.strip() == key:
                    continue

                # LocalData 기록 (첫 값 우선)
                if key not in seen_keys:
                    seen_keys.add(key)
                    ko_text = str(original)
                    localdata_rows.append({"key": key, "ko": ko_text, "en": "", "ja": ""})
                    localdata_json[key] = {"ko": ko_text, "en": "", "ja": ""}
                else:
                    warnings.append(f"Duplicate key skipped: {key} (from {json_path})")

                # json 치환
                if rewrite_json:
                    while len(row) <= col_idx:
                        row.append("")
                    row[col_idx] = key
                    modified = True

        if rewrite_json and modified:
            _write_json(json_path, data, warnings)
            rewritten_files += 1

    return localdata_rows, localdata_json, warnings, rewritten_files


def main():
    # allow_abbrev=False: --out 같은 약어로 --out_xlsx / --out_json을 추측하지 않게 막음
    parser = argparse.ArgumentParser(allow_abbrev=False)

    parser.add_argument("--json_dir", default="json")
    parser.add_argument("--out_xlsx", default="LocalData.xlsx")
    parser.add_argument("--out_json", default="LocalData.json")

    # 하위 호환: 예전 워크플로우가 --out LocalData.xlsx로 호출해도 동작하게
    parser.add_argument("--out", dest="out_xlsx", help="Alias of --out_xlsx (legacy)")

    parser.add_argument("--rewrite_json", default="1")
    args = parser.parse_args()

    rewrite = str(args.rewrite_json).strip().lower() not in ("0", "false", "no")

    json_root = Path(args.json_dir).resolve()
    out_xlsx = Path(args.out_xlsx).resolve()
    out_json = Path(args.out_json).resolve()

    rows, json_data, warnings, rewritten = build_localdata_and_rewrite(json_root, rewrite)

    df = pd.DataFrame(rows, columns=["key", "ko", "en", "ja"])
    if not df.empty:
        df.sort_values("key", inplace=True, kind="mergesort")

    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="LocalData")

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(json_data, ensure_ascii=False, indent=2), encoding="utf-8")

    if warnings:
        warn_path = out_xlsx.with_suffix(".warnings.txt")
        warn_path.write_text("\n".join(warnings), encoding="utf-8")
        print(f"Wrote warnings: {warn_path}")

    print(f"Done. keys={len(json_data)} rewritten_files={rewritten} rewrite_json={rewrite}")


if __name__ == "__main__":
    main()
