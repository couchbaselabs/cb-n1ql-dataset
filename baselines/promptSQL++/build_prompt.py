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
/* Question: Find the top 3 departments by average employee salary, showing the department name, average salary, and the number of employees in each. Only include departments with more than 5 employees. */
/* Database: hr_company — Keyspace: `hr_company`.`hr_scope`.`<collection>` */
/* SQL++ query: */
SELECT
    d.department_name,
    AVG(e.salary) AS avg_salary,
    COUNT(e.employee_id) AS employee_count
FROM `hr_company`.`hr_scope`.`employees` AS e
JOIN `hr_company`.`hr_scope`.`departments` AS d
    ON e.department_id = d.department_id
WHERE e.status = 'active'
GROUP BY d.department_name
HAVING COUNT(e.employee_id) > 5
ORDER BY avg_salary DESC
LIMIT 3;
""".strip()


# ---------------------------------------------------------------------------
# System instruction
# ---------------------------------------------------------------------------
SYSTEM_INSTRUCTION = """You are a Couchbase SQL++ expert. Given a database schema and a natural language question, generate a valid Couchbase SQL++ query.

Key Couchbase SQL++ rules:
1. Use the EXACT fully-qualified keyspace paths shown in the schema(e.g., FROM `<bucket_name>`.`<scope_name>`.`<collection_name>` AS o). Do NOT use generic names — always copy the keyspace paths from the provided schema.
2. Always use explicit table aliases after each keyspace reference.
3. Use ON (not USING) for JOIN conditions.
4. Use proper SQL++ functions instead of SQLite-specific ones:
   - IFNULL → COALESCE
   - GROUP_CONCAT → ARRAY_TO_STRING(ARRAY_AGG(...))
   - STRFTIME → DATE_PART_STR / DATE_FORMAT_STR
5. String literals use single quotes, identifiers use backticks.
6. Couchbase 7.6+ supports window functions (ROW_NUMBER, NTILE, RANK, etc.).
7. WITH RECURSIVE is NOT supported — rewrite recursive CTEs as iterative queries.

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
    ext_knowledge_content = instance.get("external_knowledge_content")
    ext_knowledge_file = instance.get("external_knowledge")
    if ext_knowledge_content:
        parts.append(f"/* External Knowledge ({ext_knowledge_file}): */")
        parts.append(f"/*")
        parts.append(ext_knowledge_content.strip())
        parts.append(f"*/")
        parts.append("")
    elif ext_knowledge_file:
        # Fallback: just reference the filename if content wasn't loaded
        parts.append(f"/* External Knowledge Reference: {ext_knowledge_file} */")
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
