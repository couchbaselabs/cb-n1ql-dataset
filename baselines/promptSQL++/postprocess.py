#!/usr/bin/env python3
"""
Step 4: Postprocess

Cleans raw LLM output files by stripping markdown fences, explanations,
and comments to produce clean .sqlpp files ready for submission.

Input:  raw_output/<instance_id>.sqlpp
Output: submission/<instance_id>.sqlpp
"""

import re
import os
import argparse
from pathlib import Path
from tqdm import tqdm


def extract_headers(raw_text: str) -> tuple[list[str], str]:
    """
    Extract leading -- bucket / -- scope header lines from the raw text.

    Scans only the lines before any code block or SQL body begins.
    Returns (header_lines, remaining_text).
    """
    header_lines = []
    remaining_lines = []
    found_non_header = False

    for line in raw_text.splitlines():
        stripped = line.strip()
        if not found_non_header:
            m = re.match(r"^--\s*(bucket|scope)\s*:\s*.+$", stripped, re.IGNORECASE)
            if m:
                header_lines.append(stripped)
                continue
        found_non_header = True
        remaining_lines.append(line)

    return header_lines, "\n".join(remaining_lines)


def extract_sql(raw_text: str) -> str:
    """
    Extract the SQL++ query from raw LLM output, preserving any leading
    -- bucket / -- scope header comments.

    Handles:
    - ```sql ... ``` or ```sqlpp ... ``` blocks
    - Plain SQL with surrounding explanation text
    - Multiple code blocks (takes the longest one)
    """
    # Pull out header lines first, then work on the rest
    header_lines, body = extract_headers(raw_text)

    # Try to find fenced code blocks
    patterns = [
        r"```(?:sql\+\+|sqlpp|sql|n1ql)\s*\n(.*?)```",  # language-tagged blocks
        r"```\s*\n(.*?)```",                                # untagged blocks
    ]

    sql = None
    for pattern in patterns:
        matches = re.findall(pattern, body, re.DOTALL | re.IGNORECASE)
        if matches:
            # Return the longest match (most likely the main query)
            sql = max(matches, key=len).strip()
            break

    if sql is None:
        # No code blocks found — assume the whole body is SQL
        text = body.strip()

        lines = text.split("\n")
        sql_start_keywords = (
            "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE",
            "DROP", "ALTER", "MERGE", "UPSERT", "EXPLAIN", "INFER",
            "--", "/*",
        )

        start_idx = 0
        for i, line in enumerate(lines):
            stripped = line.strip().upper()
            if any(stripped.startswith(kw) for kw in sql_start_keywords):
                start_idx = i
                break

        sql = "\n".join(lines[start_idx:]).strip()

        # Remove trailing explanation after the query (after a semicolon + newline + text)
        semicolon_match = re.search(r";\s*\n\s*\n", sql)
        if semicolon_match:
            sql = sql[:semicolon_match.end()].strip()

    if header_lines:
        return "\n".join(header_lines) + "\n" + sql
    return sql


def main():
    parser = argparse.ArgumentParser(description="Postprocess raw LLM output into clean .sqlpp files")
    parser.add_argument("--input_dir", type=str, default="raw_output",
                        help="Directory with raw .sqlpp files")
    parser.add_argument("--output_dir", type=str, default="submission",
                        help="Output directory for cleaned .sqlpp files")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    input_dir = (script_dir / args.input_dir).resolve()
    output_dir = (script_dir / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all .sqlpp files (skip error markers and logs)
    sqlpp_files = sorted(input_dir.glob("*.sqlpp"))
    sqlpp_files = [f for f in sqlpp_files if not f.name.startswith("_")]

    print(f"Processing {len(sqlpp_files)} files from: {input_dir}")

    processed = 0
    skipped = 0

    for sqlpp_file in tqdm(sqlpp_files, desc="Postprocessing"):
        raw_text = sqlpp_file.read_text(encoding="utf-8")

        # Skip error markers
        if raw_text.startswith("-- ERROR:"):
            skipped += 1
            continue

        clean_sql = extract_sql(raw_text)

        output_path = output_dir / sqlpp_file.name
        output_path.write_text(clean_sql, encoding="utf-8")
        processed += 1

    print(f"\nDone: {processed} processed, {skipped} skipped (errors)")
    print(f"Output: {output_dir}")


if __name__ == "__main__":
    main()
