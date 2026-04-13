#!/usr/bin/env python3
"""
Step 1: Preprocess

Loads spider2-lite.jsonl instances and merges each with its Couchbase INFER
schema + sample rows.

Modes:
  sqlite    — local* instances, schemas from resources/databases/couchbase_sqlite/
  bigquery  — bq*/ga* instances, schemas from resources/databases/couchbase_bigquery/
  snowflake — sf* instances (sf001, sf_bq*), schemas from resources/databases/couchbase_sf/

Output: preprocessed/instances.json
"""

import json
import os
import argparse
from pathlib import Path
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Mode presets: default prefix(es) and schema directory per mode
# ---------------------------------------------------------------------------
MODE_PRESETS = {
    "sqlite": {
        "prefixes": ("local",),
        "schema_dir": "../../resources/databases/couchbase_sqlite",
        "db_name_transform": None,  # use db_name as-is
    },
    "bigquery": {
        "prefixes": ("bq", "ga"),
        "schema_dir": "../../resources/databases/couchbase_bigquery",
        "db_name_transform": None,
    },
    "snowflake": {
        # sf* covers sf001, sf002, sf_bq029, etc. — all Snowflake-sourced instances
        "prefixes": ("sf",),
        "schema_dir": "../../resources/databases/couchbase_sf",
        # Couchbase bucket names strip underscores and lowercase the DB name
        # e.g. PATENTS -> patents, GITHUB_REPOS -> githubrepos
        "db_name_transform": lambda name: name.lower().replace("_", "").replace(" ", ""),
    },
}


def load_spider2_instances(jsonl_path: str, prefixes: tuple = ("local",)) -> list:
    """Load instances from spider2-lite.jsonl, filtered by instance_id prefix(es)."""
    instances = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            item = json.loads(line.strip())
            if item["instance_id"].startswith(prefixes):
                instances.append(item)
    return instances


def load_couchbase_schema(schema_dir: str, db_name: str) -> list:
    """
    Load all collection schemas for a database from the Couchbase INFER output.

    Supports two directory layouts:
      - SQLite (flat):    <schema_dir>/<db>/<table>.json
      - BigQuery (nested): <schema_dir>/<db>/<scope>/<table>.json

    When the JSON contains an explicit 'keyspace' field, it is used directly.
    Otherwise the keyspace is derived using the SQLite convention.

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

    # Derive fallback bucket/scope names (SQLite convention)
    bucket_name = db_name.replace(" ", "_").replace("-", "_")
    scope_name = f"{bucket_name}_scope"

    # Collect all JSON files: flat or nested under scope subdirectories
    json_files = sorted(db_path.rglob("*.json"))

    collections = []
    for json_file in json_files:
        collection_name = json_file.stem

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"  Warning: failed to read {json_file}: {e}")
            continue

        # Use embedded keyspace if present (BigQuery), otherwise derive from dir layout
        if "keyspace" in data:
            keyspace = data["keyspace"]
        else:
            # Nested layout (Snowflake): <db>/<scope>/<collection>.json
            # Flat layout (SQLite):      <db>/<collection>.json
            relative = json_file.relative_to(db_path)
            if len(relative.parts) == 2:
                # scope is the parent directory name
                scope_in_path = relative.parts[0]
                keyspace = f"`{bucket_name}`.`{scope_in_path}`.`{collection_name}`"
            else:
                keyspace = f"`{bucket_name}`.`{scope_name}`.`{collection_name}`"

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
                field_entry = {
                    "name": field_name,
                    "type": field_info.get("type", "unknown"),
                    "samples": field_info.get("samples", [])[:3],  # keep 3 samples max
                }
                # Include original BigQuery type if available (bonus context)
                if "original_bq_type" in field_info:
                    field_entry["original_bq_type"] = field_info["original_bq_type"]
                fields.append(field_entry)

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
    parser.add_argument("--mode", type=str, choices=list(MODE_PRESETS.keys()),
                        default="sqlite",
                        help="Instance mode: 'sqlite' (local*), 'bigquery' (bq*/ga*), or 'snowflake' (sf*)")
    parser.add_argument("--NL_questions_jsonl", type=str,
                        default="../../evaluation_pipeline/NL_questions.jsonl",
                        help="Path to NL_questions.jsonl")
    parser.add_argument("--schema_dir", type=str, default=None,
                        help="Path to Couchbase schema directory (auto-set per mode if omitted)")
    parser.add_argument("--docs_dir", type=str,
                        default="../../resources/documents",
                        help="Path to external knowledge documents directory")
    parser.add_argument("--output_dir", type=str, default="preprocessed",
                        help="Output directory")
    parser.add_argument("--prefix", type=str, default=None,
                        help="Override instance ID prefix filter (default: auto from mode)")
    args = parser.parse_args()

    # Apply mode presets
    preset = MODE_PRESETS[args.mode]
    prefixes = (args.prefix,) if args.prefix else preset["prefixes"]
    schema_dir_default = args.schema_dir or preset["schema_dir"]
    db_name_transform = preset.get("db_name_transform")

    # Resolve paths relative to this script
    script_dir = Path(__file__).parent
    jsonl_path = (script_dir / args.NL_questions_jsonl).resolve()
    schema_dir = (script_dir / schema_dir_default).resolve()
    docs_dir = (script_dir / args.docs_dir).resolve()
    output_dir = (script_dir / args.output_dir).resolve()

    print(f"Mode: {args.mode}")
    print(f"Loading instances from: {jsonl_path}")
    print(f"Schema directory: {schema_dir}")
    print(f"External knowledge dir: {docs_dir}")
    prefix_str = ', '.join(f'{p}*' for p in prefixes)
    instances = load_spider2_instances(str(jsonl_path), prefixes=prefixes)
    print(f"Found {len(instances)} instances matching [{prefix_str}]")

    # Enrich each instance with schema + external knowledge content
    enriched = []
    skipped_no_schema = []
    ext_knowledge_count = 0
    ext_knowledge_missing = []

    for item in tqdm(instances, desc="Loading schemas"):
        db_name = item["db"]
        # Transform db_name to the schema directory name if needed (e.g. snowflake)
        schema_db_name = db_name_transform(db_name) if db_name_transform else db_name
        schema = load_couchbase_schema(str(schema_dir), schema_db_name)

        # Skip instance if no schema found
        if not schema:
            skipped_no_schema.append(f"{item['instance_id']} (db={db_name})")
            continue

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

    if skipped_no_schema:
        print(f"Skipped {len(skipped_no_schema)} instances (no schema found):")
        for msg in skipped_no_schema:
            print(f"  - {msg}")
    if ext_knowledge_missing:
        print(f"Warning: {len(ext_knowledge_missing)} external knowledge files missing:")
        for msg in ext_knowledge_missing:
            print(f"  - {msg}")


if __name__ == "__main__":
    main()
