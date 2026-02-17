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


def extract_sql(raw_text: str) -> str:
    """
    Extract the SQL++ query from raw LLM output.

    Handles:
    - ```sql ... ``` or ```sqlpp ... ``` blocks
    - Plain SQL with surrounding explanation text
    - Multiple code blocks (takes the longest one)
    """
    # Try to find fenced code blocks
    patterns = [
        r"```(?:sql\+\+|sqlpp|sql|n1ql)\s*\n(.*?)```",  # language-tagged blocks
        r"```\s*\n(.*?)```",                                # untagged blocks
    ]

    for pattern in patterns:
        matches = re.findall(pattern, raw_text, re.DOTALL | re.IGNORECASE)
        if matches:
            # Return the longest match (most likely the main query)
            return max(matches, key=len).strip()

    # No code blocks found — assume the whole text is SQL
    # Strip common non-SQL prefixes/suffixes
    text = raw_text.strip()

    # Remove leading explanation lines (lines not starting with SQL keywords)
    lines = text.split("\n")
    sql_start_keywords = (
        "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE",
        "DROP", "ALTER", "MERGE", "UPSERT", "EXPLAIN", "INFER",
        "--", "/*",
    )

    # Find the first line that looks like SQL
    start_idx = 0
    for i, line in enumerate(lines):
        stripped = line.strip().upper()
        if any(stripped.startswith(kw) for kw in sql_start_keywords):
            start_idx = i
            break

    result = "\n".join(lines[start_idx:]).strip()

    # Remove trailing explanation after the query (after a semicolon + newline + text)
    semicolon_match = re.search(r";\s*\n\s*\n", result)
    if semicolon_match:
        result = result[:semicolon_match.end()].strip()

    return result


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
