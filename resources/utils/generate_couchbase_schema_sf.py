"""
Run INFER on every Couchbase collection for the specified Snowflake databases.

Bucket naming mirrors load_snowflake_to_couchbase.py:
    bucket_name = db_name.lower().replace("_", "")
    e.g. GITHUB_REPOS -> githubrepos

Scopes and collections are auto-discovered via the Couchbase management API.

Output: resources/databases/couchbase_sf/<db_name>/<scope>/<collection>.json
Each file contains the raw INFER result + 5 sample rows for that collection.
"""

import json
import os
from datetime import timedelta

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator

# ── Configuration ─────────────────────────────────────────────────────────────

CB_HOST = "couchbase://127.0.0.1"
CB_USER = "Administrator"
CB_PASS = "password"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "databases", "couchbase_sf")

# ── Databases to process ──────────────────────────────────────────────────────

DATABASES = [
    "CRYPTO",
    "THELOOK_ECOMMERCE",
    "GITHUB_REPOS",
    "IDC",
    "PATENTS",
    "_1000_GENOMES",
    "PATENTS_GOOGLE",
    "GOOG_BLOCKCHAIN",
    "ETHEREUM_BLOCKCHAIN",
    "WORD_VECTORS_US",
    "GENOMICS_CANNABIS",
    "GOOGLE_ADS",
    "HUMAN_GENOME_VARIANTS",
    "PATENTS_USPTO",
    "PANCANCER_ATLAS_1",
    "META_KAGGLE",
    "WIDE_WORLD_IMPORTERS",
    "TCGA_MITELMAN",
    "TCGA",
    "GITHUB_REPOS_DATE",
    "CENSUS_BUREAU_ACS_2",
]


def bucket_name_for(db_name: str) -> str:
    """Mirrors load_snowflake_to_couchbase.py: db_name.lower().replace('_', '')
    e.g. GITHUB_REPOS -> githubrepos, CENSUS_BUREAU_ACS_2 -> censusbureauacs2"""
    return db_name.lower().replace("_", "")


def main():
    print(f"Connecting to Couchbase at {CB_HOST} ...")
    auth = PasswordAuthenticator(CB_USER, CB_PASS)
    cluster = Cluster(CB_HOST, ClusterOptions(auth))
    cluster.wait_until_ready(timedelta(seconds=10))
    print("Connected.\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    total = 0
    errors = 0

    for db_name in DATABASES:
        bucket_name = bucket_name_for(db_name)

        print(f"{'=' * 60}")
        print(f"DB: {db_name}  →  bucket: {bucket_name}")
        print(f"{'=' * 60}")

        # Check bucket exists
        try:
            cluster.buckets().get_bucket(bucket_name)
        except Exception as e:
            print(f"  ⚠ Bucket '{bucket_name}' not found, skipping. ({e})\n")
            errors += 1
            continue

        bucket = cluster.bucket(bucket_name)

        # Auto-discover all scopes and collections
        try:
            scopes = bucket.collections().get_all_scopes()
        except Exception as e:
            print(f"  ⚠ Cannot list scopes for '{bucket_name}': {e}\n")
            errors += 1
            continue

        for scope in scopes:
            # Skip the default Couchbase internal scopes
            if scope.name in ("_default", "_system"):
                continue

            for collection in scope.collections:
                coll_name = collection.name
                scope_name = scope.name

                keyspace = f"`{bucket_name}`.`{scope_name}`.`{coll_name}`"
                infer_query = f"INFER {keyspace}"
                sample_query = f"SELECT d.* FROM {keyspace} AS d LIMIT 5"

                infer_result = None
                sample_rows = []

                try:
                    infer_result = list(cluster.query(infer_query))
                    status = "✓"
                except Exception as e:
                    errors += 1
                    status = "⚠"
                    print(f"  {status} {scope_name}.{coll_name} — INFER failed: {e}")

                try:
                    sample_rows = list(cluster.query(sample_query))
                except Exception:
                    pass

                out_dir = os.path.join(OUTPUT_DIR, bucket_name, scope_name)
                os.makedirs(out_dir, exist_ok=True)
                out_path = os.path.join(out_dir, f"{coll_name}.json")

                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(
                        {"infer_schema": infer_result, "sample_rows": sample_rows},
                        f,
                        indent=2,
                        default=str,
                    )

                if infer_result is not None:
                    print(f"  {status} {scope_name}.{coll_name}  ({len(sample_rows)} sample rows)")

                total += 1

        print()

    print(f"Done! {total} collections processed, {errors} errors.")
    print(f"Output: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()
