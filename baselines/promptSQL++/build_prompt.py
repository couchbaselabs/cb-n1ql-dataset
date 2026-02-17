#!/usr/bin/env python3
"""
Step 2: Build Prompts

Constructs LLM prompts for each instance using a Code Representation (CR)
format adapted for Couchbase SQL++.

Input:  preprocessed/instances.json
Output: prompts/questions.json
"""

import json
import os
import argparse
from pathlib import Path

# ---------------------------------------------------------------------------
# Few-shot example (SQLite question → SQL++ answer)
# ---------------------------------------------------------------------------
FEW_SHOT_EXAMPLE = """
/* Example question and corresponding Couchbase SQL++ query: */
/* Question: Please help me find the top 3 bowlers who conceded the maximum runs in a single over, along with the corresponding matches. */
/* Database: IPL — Keyspace: `ipl`.`ipl_scope`.`<collection>` */
/* SQL++ query: */
WITH combined_runs AS (
    SELECT cr.match_id, cr.over_id, cr.ball_id, cr.innings_no, cr.runs_scored AS runs
    FROM `ipl`.`ipl_scope`.`batsman_scored` AS cr
    UNION ALL
    SELECT er.match_id, er.over_id, er.ball_id, er.innings_no, er.extra_runs AS runs
    FROM `ipl`.`ipl_scope`.`extra_runs` AS er
),
over_runs AS (
    SELECT orr.match_id, orr.innings_no, orr.over_id, SUM(orr.runs) AS runs_scored
    FROM combined_runs AS orr
    GROUP BY orr.match_id, orr.innings_no, orr.over_id
),
max_over_runs AS (
    SELECT mor.match_id, MAX(mor.runs_scored) AS max_runs
    FROM over_runs AS mor
    GROUP BY mor.match_id
),
top_overs AS (
    SELECT o.match_id, o.innings_no, o.over_id, o.runs_scored
    FROM over_runs AS o
    JOIN max_over_runs AS m ON o.match_id = m.match_id AND o.runs_scored = m.max_runs
),
top_bowlers AS (
    SELECT
        bb.match_id,
        t.runs_scored AS maximum_runs,
        bb.bowler
    FROM `ipl`.`ipl_scope`.`ball_by_ball` AS bb
    JOIN top_overs AS t ON bb.match_id = t.match_id
        AND bb.innings_no = t.innings_no
        AND bb.over_id = t.over_id
    GROUP BY bb.match_id, t.runs_scored, bb.bowler
)
SELECT
    b.match_id,
    p.player_name
FROM (
    SELECT tb.*
    FROM top_bowlers AS tb
    ORDER BY tb.maximum_runs DESC
    LIMIT 3
) AS b
JOIN `ipl`.`ipl_scope`.`player` AS p ON p.player_id = b.bowler;
""".strip()


# ---------------------------------------------------------------------------
# System instruction
# ---------------------------------------------------------------------------
SYSTEM_INSTRUCTION = """You are a Couchbase SQL++ expert. Given a database schema and a natural language question, generate a valid Couchbase SQL++ query.

Key Couchbase SQL++ rules:
1. Use fully-qualified keyspace paths: `bucket`.`scope`.`collection`
2. Always use explicit table aliases (e.g., FROM `bucket`.`scope`.`orders` AS o)
3. Use ON (not USING) for JOIN conditions
4. Use proper SQL++ functions instead of SQLite-specific ones:
   - IFNULL → COALESCE
   - GROUP_CONCAT → ARRAY_TO_STRING(ARRAY_AGG(...))
   - STRFTIME → DATE_PART_STR / DATE_FORMAT_STR
5. String literals use single quotes, identifiers use backticks
6. Couchbase 7.6+ supports window functions (ROW_NUMBER, NTILE, RANK, etc.)
7. WITH RECURSIVE is NOT supported — rewrite recursive CTEs as iterative queries

Return ONLY the SQL++ query, no explanations."""


def build_schema_block(schema: list) -> str:
    """Format schema info into a readable text block for the prompt."""
    if not schema:
        return "/* No schema available */\n"

    lines = []
    for coll in schema:
        keyspace = coll["keyspace"]
        fields = coll.get("fields", [])

        # Collection header
        lines.append(f"/* Collection: {keyspace} */")

        # Field definitions
        if fields:
            field_strs = []
            for f in fields:
                samples_str = ""
                if f.get("samples"):
                    sample_vals = [repr(s) for s in f["samples"][:3]]
                    samples_str = f" -- e.g. {', '.join(sample_vals)}"
                field_strs.append(f"  {f['name']} ({f['type']}){samples_str}")
            lines.append("/* Fields:")
            lines.extend(field_strs)
            lines.append("*/")

        # Sample rows
        sample_rows = coll.get("sample_rows", [])
        if sample_rows:
            lines.append(f"/* Sample rows ({len(sample_rows)}):")
            for row in sample_rows[:2]:
                # Remove ~meta if present
                row_clean = {k: v for k, v in row.items() if k != "~meta"}
                lines.append(f"  {json.dumps(row_clean, default=str)}")
            lines.append("*/")

        lines.append("")

    return "\n".join(lines)


def build_prompt(instance: dict, use_few_shot: bool = True) -> dict:
    """
    Build a single prompt for an instance.

    Returns dict with: instance_id, prompt (str), system (str)
    """
    schema_block = build_schema_block(instance.get("schema", []))
    question = instance["question"]
    db_name = instance["db"]

    parts = []

    # Schema
    parts.append(f"/* Database: {db_name} */")
    parts.append(schema_block)

    # External knowledge (if any)
    ext_knowledge = instance.get("external_knowledge")
    if ext_knowledge:
        parts.append(f"/* External Knowledge Reference: {ext_knowledge} */")
        parts.append("")

    # Few-shot example
    if use_few_shot:
        parts.append(FEW_SHOT_EXAMPLE)
        parts.append("")

    # The actual question
    parts.append(f"/* Question: {question} */")
    parts.append("/* Generate the Couchbase SQL++ query: */")

    prompt_text = "\n".join(parts)

    return {
        "instance_id": instance["instance_id"],
        "db": db_name,
        "question": question,
        "system": SYSTEM_INSTRUCTION,
        "prompt": prompt_text,
    }


def main():
    parser = argparse.ArgumentParser(description="Build LLM prompts from preprocessed instances")
    parser.add_argument("--input", type=str, default="preprocessed/instances.json",
                        help="Path to preprocessed instances")
    parser.add_argument("--output_dir", type=str, default="prompts",
                        help="Output directory for questions.json")
    parser.add_argument("--no_few_shot", action="store_true",
                        help="Disable the 1-shot example in prompts")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    input_path = (script_dir / args.input).resolve()
    output_dir = (script_dir / args.output_dir).resolve()

    print(f"Loading instances from: {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        instances = json.load(f)

    print(f"Building prompts for {len(instances)} instances...")
    questions = []
    for inst in instances:
        q = build_prompt(inst, use_few_shot=not args.no_few_shot)
        questions.append(q)

    # Save
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "questions.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(questions, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(questions)} prompts to: {output_path}")

    # Print a sample
    if questions:
        sample = questions[0]
        print(f"\n--- Sample prompt for {sample['instance_id']} ---")
        print(sample["prompt"][:500])
        print("...")


if __name__ == "__main__":
    main()
