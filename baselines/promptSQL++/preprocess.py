#!/usr/bin/env python3
"""
Step 1: Preprocess

Loads spider2-lite.jsonl (local* instances only) and merges each instance
with its Couchbase INFER schema + sample rows from resources/databases/couchbase_sqlite/.

Output: preprocessed/instances.json
"""

import json
import os
import argparse
from pathlib import Path
from tqdm import tqdm


def load_spider2_instances(jsonl_path: str, prefix: str = "local") -> list:
    """Load instances from spider2-lite.jsonl, filtered by instance_id prefix."""
    instances = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            item = json.loads(line.strip())
            if item["instance_id"].startswith(prefix):
                instances.append(item)
    return instances


def load_couchbase_schema(schema_dir: str, db_name: str) -> list:
    """
    Load all collection schemas for a database from the Couchbase INFER output.

    Returns a list of dicts, each with:
        - collection: collection/table name
        - keyspace: `bucket`.`scope`.`collection` string
        - fields: [{name, type, samples}]
        - sample_rows: [row dicts]
    """
    db_path = Path(schema_dir) / db_name
    if not db_path.exists():
        print(f"  Warning: schema dir not found for db '{db_name}': {db_path}")
        return []

    # Derive bucket/scope names (matching generate_couchbase_schema.py conventions)
    bucket_name = db_name.lower().replace(" ", "_").replace("-", "_")
    scope_name = f"{bucket_name}_scope"

    collections = []
    for json_file in sorted(db_path.glob("*.json")):
        collection_name = json_file.stem
        keyspace = f"`{bucket_name}`.`{scope_name}`.`{collection_name}`"

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"  Warning: failed to read {json_file}: {e}")
            continue

        # Parse INFER schema to extract fields
        fields = []
        infer_schema = data.get("infer_schema", [])
        if infer_schema and len(infer_schema) > 0:
            # infer_schema is [[{properties: {...}, ...}]]
            schema_obj = infer_schema[0]
            if isinstance(schema_obj, list) and len(schema_obj) > 0:
                schema_obj = schema_obj[0]
            properties = schema_obj.get("properties", {})
            for field_name, field_info in properties.items():
                if field_name == "~meta":
                    continue  # skip internal metadata
                fields.append({
                    "name": field_name,
                    "type": field_info.get("type", "unknown"),
                    "samples": field_info.get("samples", [])[:3],  # keep 3 samples max
                })

        sample_rows = data.get("sample_rows", [])[:3]  # keep 3 sample rows max

        collections.append({
            "collection": collection_name,
            "keyspace": keyspace,
            "fields": fields,
            "sample_rows": sample_rows,
        })

    return collections


def main():
    parser = argparse.ArgumentParser(description="Preprocess NL_questions instances with Couchbase schemas")
    parser.add_argument("--NL_questions_jsonl", type=str,
                        default="../../Spider2/evaluation_pipeline/NL_questions.jsonl",
                        help="Path to NL_questions.jsonl")
    parser.add_argument("--schema_dir", type=str,
                        default="../../resources/databases/couchbase_sqlite",
                        help="Path to Couchbase schema directory")
    parser.add_argument("--docs_dir", type=str,
                        default="../../Spider2/spider2-lite/resource/documents",
                        help="Path to external knowledge documents directory")
    parser.add_argument("--output_dir", type=str, default="preprocessed",
                        help="Output directory")
    parser.add_argument("--prefix", type=str, default="local",
                        help="Instance ID prefix to filter (default: local)")
    args = parser.parse_args()

    # Resolve paths relative to this script
    script_dir = Path(__file__).parent
    jsonl_path = (script_dir / args.spider2_jsonl).resolve()
    schema_dir = (script_dir / args.schema_dir).resolve()
    docs_dir = (script_dir / args.docs_dir).resolve()
    output_dir = (script_dir / args.output_dir).resolve()

    print(f"Loading instances from: {jsonl_path}")
    print(f"External knowledge dir: {docs_dir}")
    instances = load_spider2_instances(str(jsonl_path), prefix=args.prefix)
    print(f"Found {len(instances)} '{args.prefix}*' instances")

    # Enrich each instance with schema + external knowledge content
    enriched = []
    ext_knowledge_count = 0
    ext_knowledge_missing = []

    for item in tqdm(instances, desc="Loading schemas"):
        db_name = item["db"]
        schema = load_couchbase_schema(str(schema_dir), db_name)

        # Load external knowledge content if referenced
        ext_knowledge_file = item.get("external_knowledge")
        ext_knowledge_content = None
        if ext_knowledge_file:
            doc_path = docs_dir / ext_knowledge_file
            if doc_path.exists():
                try:
                    ext_knowledge_content = doc_path.read_text(encoding="utf-8")
                    ext_knowledge_count += 1
                except IOError as e:
                    ext_knowledge_missing.append(f"{item['instance_id']}: read error - {e}")
            else:
                ext_knowledge_missing.append(f"{item['instance_id']}: {ext_knowledge_file} not found")

        enriched.append({
            "instance_id": item["instance_id"],
            "db": db_name,
            "question": item["question"],
            "external_knowledge": ext_knowledge_file,
            "external_knowledge_content": ext_knowledge_content,
            "schema": schema,
        })

    # Save
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "instances.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(enriched)} instances to: {output_path}")
    print(f"External knowledge loaded: {ext_knowledge_count} instances")

    # Quick stats
    no_schema = sum(1 for item in enriched if len(item["schema"]) == 0)
    if no_schema > 0:
        print(f"Warning: {no_schema} instances have no schema (missing db in couchbase_sqlite)")
    if ext_knowledge_missing:
        print(f"Warning: {len(ext_knowledge_missing)} external knowledge files missing:")
        for msg in ext_knowledge_missing:
            print(f"  - {msg}")


if __name__ == "__main__":
    main()
