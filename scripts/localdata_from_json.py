#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
localdata_from_json.py

- Scan converted sheet json files under --json_dir
- Extract columns whose base type is "local_string"
- Build:
    1) LocalData.xlsx
    2) LocalData.json
- Rewrite source json files so that each local_string cell value
  becomes the generated localization key (default ON)
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
    return [m.group(1).strip() for m in _REF_RE.finditer(parts[1]) if m.group(1).strip()]


def _iter_sheet_json_files(json_root: Path):
    for p in json_root.rglob("*.json"):
        rel = p.relative_to(json_root)
        if len(rel.parts) == 1:
            yield p.stem, p.stem, p
        else:
            yield rel.parts[0], p.stem, p


def build_localdata(json_root: Path, rewrite_json: bool):
    warnings = []
    localdata_rows = []
    localdata_json = {}
    seen_keys = set()
    rewritten_files = 0

    for file_stem, sheet_name, json_path in sorted(_iter_sheet_json_files(json_root)):
        data = json.loads(json_path.read_text(encoding="utf-8"))
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
            for col, refs in local_columns:
                col_idx = column_index[col]
                original = row[col_idx]

                if not original:
                    continue

                key_parts = [file_stem, sheet_name]

                if refs:
                    for ref in refs:
                        key_parts.append(str(row[column_index[ref]]))
                else:
                    key_parts.append(str(row_i + 1))

                key = ".".join(key_parts)

                if original == key:
                    continue

                if key not in seen_keys:
                    seen_keys.add(key)
                    localdata_rows.append(
                        {"key": key, "ko": original, "en": "", "ja": ""}
                    )
                    localdata_json[key] = {
                        "ko": original,
                        "en": "",
                        "ja": "",
                    }

                if rewrite_json:
                    row[col_idx] = key
                    modified = True

        if modified:
            json_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            rewritten_files += 1

    return localdata_rows, localdata_json, warnings, rewritten_files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json_dir", default="json")
    parser.add_argument("--out_xlsx", default="LocalData.xlsx")
    parser.add_argument("--out_json", default="LocalData.json")
    parser.add_argument("--rewrite_json", default="1")
    args = parser.parse_args()

    rewrite = args.rewrite_json not in ("0", "false", "False")

    rows, json_data, warnings, rewritten = build_localdata(
        Path(args.json_dir), rewrite
    )

    # XLSX
    df = pd.DataFrame(rows, columns=["key", "ko", "en", "ja"])
    if not df.empty:
        df.sort_values("key", inplace=True)

    with pd.ExcelWriter(args.out_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="LocalData")

    # JSON
    Path(args.out_json).write_text(
        json.dumps(json_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        f"Done. keys={len(json_data)}, rewritten_files={rewritten}, rewrite_json={rewrite}"
    )


if __name__ == "__main__":
    main()
