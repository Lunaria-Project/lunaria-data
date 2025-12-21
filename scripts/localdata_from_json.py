#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
localdata_from_json.py

- Scans converted sheet json files under --json_dir
- Extracts columns whose base type is "local_string"
- Builds LocalData.xlsx with columns: key, ko, en, ja
- (Optional, default ON) Rewrites the source json files so that each local_string cell value
  becomes the generated localization key.

Expected sheet json format:
{
  "types": { "ColA": "int", "ColB": "local_string;[Id1][Id2]", ... },
  "rows": [
    [ ... ],
    ...
  ]
}

Key rule:
- For each "local_string" column with reference suffix like ";[NpcId][Order]":
  key = "{FileStem}.{SheetName}.{valueOf(NpcId)}.{valueOf(Order)}"
- If the local_string type has no "[...]" references:
  key = "{FileStem}.{SheetName}.{rowIndex1Based}"

Outputs:
- LocalData.xlsx (sheet: LocalData)
- LocalData.warnings.txt (only if warnings exist)

Notes:
- Duplicate keys: keeps the first entry, warns for duplicates.
- If rewriting json and a cell already equals the computed key, it will not generate a new
  LocalData row for it (assumes already localized).
"""

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
    suffix = parts[1]
    refs: list[str] = []
    for m in _REF_RE.finditer(suffix):
        name = (m.group(1) or "").strip()
        if name:
            refs.append(name)
    return refs


def _iter_sheet_json_files(json_root: Path) -> list[tuple[str, str, Path]]:
    """
    Returns list of (file_stem, sheet_name, json_path)
    - Excel: json/<FileStem>/<Sheet>.json
    - CSV  : json/<FileStem>/<FileStem>.json (sheet_name == file_stem)
    """
    items: list[tuple[str, str, Path]] = []
    if not json_root.exists():
        return items

    for p in json_root.rglob("*.json"):
        rel = p.relative_to(json_root)

        # json/<FileStem>.json
        if len(rel.parts) == 1:
            file_stem = p.stem
            sheet_name = p.stem
            items.append((file_stem, sheet_name, p))
            continue

        # json/<FileStem>/<Sheet>.json
        file_stem = rel.parts[0]
        sheet_name = p.stem
        items.append((file_stem, sheet_name, p))

    return items


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


def build_localdata_and_rewrite(
    json_root: Path,
    rewrite_json: bool,
) -> tuple[list[dict], list[str], int]:
    """
    Returns:
      - localdata rows: list of {"key","ko","en","ja"}
      - warnings
      - rewritten_file_count
    """
    warnings: list[str] = []
    output_rows: list[dict] = []
    seen_keys: set[str] = set()
    rewritten_file_count = 0

    for file_stem, sheet_name, json_path in sorted(_iter_sheet_json_files(json_root)):
        data = _read_json(json_path, warnings)
        if data is None:
            continue

        types = data.get("types")
        rows = data.get("rows")

        if not isinstance(types, dict) or not isinstance(rows, list):
            warnings.append(f"Invalid format (missing types/rows): {json_path}")
            continue

        columns = list(types.keys())
        column_index = {name: idx for idx, name in enumerate(columns)}

        local_string_columns: list[tuple[str, list[str]]] = []
        for col_name, typ in types.items():
            if _base_type(typ) == _LOCAL_STRING_BASE:
                refs = _ref_columns(typ)
                local_string_columns.append((col_name, refs))

        if not local_string_columns:
            continue

        any_row_modified = False

        for row_i, row in enumerate(rows):
            if not isinstance(row, list):
                continue

            for local_col, ref_cols in local_string_columns:
                local_col_index = column_index.get(local_col)
                if local_col_index is None:
                    continue

                original_value = row[local_col_index] if local_col_index < len(row) else ""
                if original_value in (None, ""):
                    continue

                # key parts
                key_parts = [file_stem, sheet_name]

                if ref_cols:
                    missing_ref = False
                    for ref in ref_cols:
                        ref_idx = column_index.get(ref)
                        if ref_idx is None:
                            warnings.append(
                                f"[{file_stem}.{sheet_name}] type references unknown column '{ref}' "
                                f"(local col '{local_col}') in {json_path}"
                            )
                            missing_ref = True
                            break

                        ref_val = row[ref_idx] if ref_idx < len(row) else ""
                        if ref_val in (None, ""):
                            warnings.append(
                                f"[{file_stem}.{sheet_name}] empty ref value for '{ref}' at row {row_i} "
                                f"(local col '{local_col}') in {json_path}"
                            )
                            missing_ref = True
                            break

                        key_parts.append(str(ref_val).strip())

                    if missing_ref:
                        continue
                else:
                    # No explicit reference rule -> 1-based row index
                    key_parts.append(str(row_i + 1))

                key = ".".join(key_parts)

                # If it's already rewritten (cell equals key), skip producing new LocalData row
                if isinstance(original_value, str) and original_value.strip() == key:
                    continue

                # Record LocalData (first occurrence wins)
                if key in seen_keys:
                    warnings.append(f"Duplicate key skipped: {key} (from {json_path})")
                else:
                    seen_keys.add(key)
                    output_rows.append(
                        {
                            "key": key,
                            "ko": str(original_value),
                            "en": "",
                            "ja": "",
                        }
                    )

                # Rewrite json cell -> key
                if rewrite_json:
                    # Ensure row has enough columns (defensive)
                    while len(row) <= local_col_index:
                        row.append("")
                    row[local_col_index] = key
                    any_row_modified = True

        if rewrite_json and any_row_modified:
            _write_json(json_path, data, warnings)
            rewritten_file_count += 1

    return output_rows, warnings, rewritten_file_count


def main():
    parser = argparse.ArgumentParser(description="Build LocalData.xlsx from converted json directory and optionally rewrite json local_string cells to keys.")
    parser.add_argument("--json_dir", default="json", help="Input directory containing converted json output (default: ./json)")
    parser.add_argument("--out", default="LocalData.xlsx", help="Output xlsx path (default: LocalData.xlsx)")
    parser.add_argument("--rewrite_json", default="1", help="Rewrite json local_string cells to key (1/0, default: 1)")
    args = parser.parse_args()

    json_root = Path(args.json_dir).resolve()
    out_path = Path(args.out).resolve()
    rewrite_json = str(args.rewrite_json).strip() not in ("0", "false", "False", "no", "NO")

    rows, warnings, rewritten_file_count = build_localdata_and_rewrite(json_root, rewrite_json)

    df = pd.DataFrame(rows, columns=["key", "ko", "en", "ja"])
    if not df.empty:
        df.sort_values(["key"], inplace=True, kind="mergesort")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="LocalData")

    if warnings:
        warn_path = out_path.with_suffix(".warnings.txt")
        warn_path.write_text("\n".join(warnings), encoding="utf-8")
        print(f"Wrote warnings: {warn_path}")

    print(f"Done. Wrote: {out_path} (rows={len(df)}) rewrite_json={rewrite_json} rewritten_files={rewritten_file_count}")


if __name__ == "__main__":
    main()
