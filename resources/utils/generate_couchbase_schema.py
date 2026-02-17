"""
Run INFER on every Couchbase collection and save the raw output as JSON.

Output: resources/databases/couchbase_sqlite/<db_name>/<collection>.json
Each file contains the raw INFER result for that collection.
"""

import json
import os
import sqlite3
from datetime import timedelta

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator

# ── Configuration ────────────────────────────────────────────────────────────

CB_HOST = "couchbase://127.0.0.1"
CB_USER = "Administrator"
CB_PASS = "password"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SQLITE_DB_DIR = os.path.join(
    BASE_DIR, "Spider2", "spider2-lite", "resource", "databases", "local_sqlite"
)

OUTPUT_DIR = os.path.join(
    BASE_DIR, "resources", "databases", "couchbase_sqlite"
)


# ── Discover databases (mirrors sample.py) ───────────────────────────────────

def discover_databases():
    structure = []
    if not os.path.exists(SQLITE_DB_DIR):
        print(f"Error: Directory {SQLITE_DB_DIR} does not exist")
        return structure

    for file_name in sorted(os.listdir(SQLITE_DB_DIR)):
        if not file_name.endswith(".sqlite"):
            continue
        db_path = os.path.join(SQLITE_DB_DIR, file_name)
        if not os.path.isfile(db_path):
            continue

        db_name = file_name[:-7]
        bucket_name = db_name.lower().replace(" ", "_").replace("-", "_")
        scope_name = f"{bucket_name}_scope"

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            if not tables:
                continue
        except Exception as e:
            print(f"Warning: cannot read {file_name}: {e}")
            continue

        structure.append({
            "db_name": db_name,
            "bucket_name": bucket_name,
            "scope_name": scope_name,
            "tables": tables,
        })
    return structure


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"Discovering databases from: {SQLITE_DB_DIR}\n")
    structure = discover_databases()
    if not structure:
        print("No databases found!")
        return

    print(f"Found {len(structure)} database(s).\n")

    print(f"Connecting to Couchbase at {CB_HOST} ...")
    auth = PasswordAuthenticator(CB_USER, CB_PASS)
    cluster = Cluster(CB_HOST, ClusterOptions(auth))
    cluster.wait_until_ready(timedelta(seconds=10))
    print("Connected.\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    total = 0
    errors = 0

    for entry in structure:
        db_name = entry["db_name"]
        bucket_name = entry["bucket_name"]
        scope_name = entry["scope_name"]
        tables = entry["tables"]

        print(f"{'=' * 60}")
        print(f"{db_name}  ({len(tables)} collections)")
        print(f"{'=' * 60}")

        db_out_dir = os.path.join(OUTPUT_DIR, db_name)
        os.makedirs(db_out_dir, exist_ok=True)

        for table_name in tables:
            keyspace = f"`{bucket_name}`.`{scope_name}`.`{table_name}`"
            infer_query = f"INFER {keyspace}"
            sample_query = f"SELECT d.* FROM {keyspace} AS d LIMIT 5"

            infer_result = None
            sample_rows = []

            try:
                infer_result = list(cluster.query(infer_query))
            except Exception as e:
                errors += 1
                print(f"   ⚠ {table_name} — INFER failed: {e}")

            try:
                sample_rows = list(cluster.query(sample_query))
            except Exception:
                pass

            output = {
                "infer_schema": infer_result,
                "sample_rows": sample_rows,
            }

            out_path = os.path.join(db_out_dir, f"{table_name}.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, default=str)

            if infer_result is not None:
                print(f"   ✓ {table_name}  ({len(sample_rows)} sample rows)")

            total += 1

    print(f"\nDone! {total} collections, {errors} errors.")
    print(f"Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
