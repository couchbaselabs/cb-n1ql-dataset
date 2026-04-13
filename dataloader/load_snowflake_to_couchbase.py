import argparse
import ast
import datetime
import decimal
import json
import logging
import math
import os
import time

import snowflake.connector

# ── Logging setup ────────────────────────────────────────────────────────────
LOG_FILE = os.path.join(os.path.dirname(__file__), "migration.log")

logger = logging.getLogger("snowflake_to_cb")
logger.setLevel(logging.DEBUG)

# Console: INFO and above
_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)
_ch.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(_ch)

# File: DEBUG and above (includes per-doc errors)
_fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))
logger.addHandler(_fh)
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.management.buckets import CreateBucketSettings, BucketType

# ── Defaults ─────────────────────────────────────────────────────────────────
CB_HOST = os.environ.get("COUCHBASE_HOST", "couchbase://127.0.0.1")
CB_USER = os.environ.get("COUCHBASE_USER", "Administrator")
CB_PASS = os.environ.get("COUCHBASE_PASS", "password")

BUCKET_RAM_QUOTA_MB = 1000

GOLD_TABLES_PATH = os.path.join(
    os.path.dirname(__file__),
    "mine", "Spider2", "methods", "gold-tables", "spider2-lite-gold-tables.jsonl",
)

# Some user-facing DB names differ from the DB names inside gold-tables.jsonl.
# Map: user DB name -> gold-tables DB name
DB_NAME_TO_GOLD = {
    "HUMAN_GENOME_VAR": "HUMAN_GENOME_VARIANTS",
    "GEO_OSM_BOUNDARIES": "GEO_OPENSTREETMAP_BOUNDARIES",
    "GEO_OSM_WORLDPOP": "GEO_OPENSTREETMAP_WORLDPOP",
    "GEO_OSM_CENSUS": "GEO_OPENSTREETMAP_CENSUS_PLACES",
}

NON_GOLD_LIMIT = 5  # rows to load for tables NOT in the gold set


def load_gold_table_set(path: str) -> set:
    """Return a set of (DATABASE, SCHEMA, TABLE) tuples from the gold JSONL."""
    gold = set()
    with open(path) as f:
        for line in f:
            entry = json.loads(line)
            for t in entry.get("gold_tables", []):
                parts = t.split(".")
                if len(parts) == 3:
                    gold.add((parts[0].upper(), parts[1].upper(), parts[2].upper()))
    return gold


def is_gold_table(gold_set: set, db_name: str, schema_name: str, table_name: str) -> bool:
    """Check whether a Snowflake table is in the gold set."""
    lookup_db = DB_NAME_TO_GOLD.get(db_name.upper(), db_name.upper())
    return (lookup_db, schema_name.upper(), table_name.upper()) in gold_set


# ── Couchbase helpers ────────────────────────────────────────────────────────

def sanitize_cb_name(name: str) -> str:
    """Couchbase scope/collection names must not start with _ or %.
    Prefix with 't_' if the name starts with a forbidden character."""
    name = name.lower()
    if name.startswith(("_", "%")):
        name = "t_" + name
    return name

def setup_bucket(cluster, bucket_name, ram_quota_mb=BUCKET_RAM_QUOTA_MB):
    bm = cluster.buckets()
    try:
        bm.get_bucket(bucket_name.lower())
        print(f"   -> Bucket '{bucket_name.lower()}' exists.")
    except Exception:
        print(f"   -> Creating Bucket '{bucket_name.lower()}'...")
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name.lower(),
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=ram_quota_mb,
            )
        )
        time.sleep(5)

def setup_scope(bucket, scope_name):
    cm = bucket.collections()
    scopes = [s.name.lower() for s in cm.get_all_scopes()]
    if scope_name.lower() not in scopes:
        cm.create_scope(scope_name.lower())
        time.sleep(2)

def setup_collection(bucket, scope_name, collection_name):
    cm = bucket.collections()
    scope_def = next((s for s in cm.get_all_scopes() if s.name.lower() == scope_name.lower()), None)
    collections = [c.name.lower() for c in scope_def.collections] if scope_def else []
    if collection_name.lower() not in collections:
        cm.create_collection(scope_name.lower(), collection_name.lower())
        time.sleep(2)

# ── Data cleaning ────────────────────────────────────────────────────────────
def clean_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        # Try to parse Snowflake VARIANT JSON strings
        if value.startswith(("{", "[")):
            try:
                return json.loads(value)
            except (ValueError, SyntaxError):
                pass
        return value
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, datetime.time):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            import base64
            return base64.b64encode(value).decode("utf-8")
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, list):
        return [clean_value(v) for v in value]
    if isinstance(value, dict):
        return {k: clean_value(v) for k, v in value.items()}
    return value

def clean_row_data(row_dict):
    return {key: clean_value(value) for key, value in row_dict.items()}

# ── Main Loader ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Load data from Snowflake to Couchbase")
    parser.add_argument("--dbs", type=str, required=True, help="Comma-separated list of database names to migrate (e.g., THELOOK_ECOMMERCE,GITHUB_REPOS)")
    parser.add_argument("--passcode", type=str, required=True, help="Snowflake MFA TOTP Passcode")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows per table. Default uses no limit.")
    parser.add_argument("--gold-tables", type=str, default=GOLD_TABLES_PATH,
                        help="Path to spider2-lite-gold-tables.jsonl. "
                             "Gold tables are loaded fully; others get --non-gold-limit rows.")
    parser.add_argument("--non-gold-limit", type=int, default=NON_GOLD_LIMIT,
                        help=f"Max rows for non-gold tables (default {NON_GOLD_LIMIT}).")
    parser.add_argument("--tables", type=str, default=None,
                        help="Optional comma-separated SCHEMA.TABLE filter (e.g. CRYPTO_ZILLIQA.DS_BLOCKS,CRYPTO_ZILLIQA.TRANSACTIONS). "
                             "When set, only these tables are migrated.")
    parser.add_argument("--gold-only", action="store_true", default=False,
                        help="Only migrate gold tables; skip all non-gold tables entirely "
                             "(no collection created, no rows fetched). "
                             "Useful for huge DBs like GITHUB_REPOS_DATE that would exceed collection limits.")
    args = parser.parse_args()

    databases_to_migrate = [db.strip().upper() for db in args.dbs.split(",")]

    # Parse optional table filter
    table_filter = None
    if args.tables:
        table_filter = set()
        for t in args.tables.split(","):
            parts = t.strip().upper().split(".")
            if len(parts) == 2:
                table_filter.add((parts[0], parts[1]))
            else:
                parser.error(f"Invalid --tables entry '{t}'. Use SCHEMA.TABLE format.")
        logger.info(f"Table filter active: {len(table_filter)} table(s)")

    # Load gold table set
    gold_set = load_gold_table_set(args.gold_tables)
    print(f"Loaded {len(gold_set)} gold table references from {args.gold_tables}")

    # Connect to Couchbase
    print(f"Connecting to Couchbase at {CB_HOST}...")
    auth = PasswordAuthenticator(CB_USER, CB_PASS)
    cluster = Cluster(CB_HOST, ClusterOptions(auth))
    cluster.wait_until_ready(datetime.timedelta(seconds=10))

    # Connect to Snowflake.Note: The credentials here needs to be provided by the spider authors as we brought some of the data that spider authors used.Contact them by refering to their githb page.
    
    print("Connecting to Snowflake...")
    sf_config = dict(
        user="", #enter your Snowflake username here or set via env vars 
        password="", #enter your Snowflake password here or set via env vars
        account="RSRSBDK-YDB67606",
        passcode=args.passcode,
    )

    def sf_connect(config):
        """Create a fresh Snowflake connection + cursor."""
        c = snowflake.connector.connect(**config)
        return c, c.cursor()

    def sf_reconnect(config):
        """Prompt for a new MFA passcode and reconnect."""
        new_passcode = input("\n🔑 Snowflake token expired. Enter new MFA passcode: ").strip()
        config["passcode"] = new_passcode
        logger.info("Reconnecting to Snowflake...")
        return sf_connect(config)

    conn, cur = sf_connect(sf_config)

    # ── Migration tracking ────────────────────────────────────────────────
    migration_ok = []      # (db, schema, table, rows_upserted)
    migration_fail = []    # (db, schema, table, error_message)
    upsert_errors_total = 0

    for db_name in databases_to_migrate:
        logger.info(f"\n=====================================")
        logger.info(f"💼 Starting Database: {db_name}")
        logger.info(f"=====================================")
        
        # Format database name to valid bucket name
        bucket_name = db_name.lower().replace("_", "")

        # Check if DB exists in Snowflake
        try:
            cur.execute(f"USE DATABASE \"{db_name}\"")
        except Exception as e:
            if "Authentication token has expired" in str(e):
                conn, cur = sf_reconnect(sf_config)
                try:
                    cur.execute(f"USE DATABASE \"{db_name}\"")
                except Exception as e2:
                    logger.error(f"❌ Failed to switch to DB {db_name} after reconnect: {e2}")
                    migration_fail.append((db_name, "*", "*", str(e2)))
                    continue
            else:
                logger.error(f"❌ Failed to switch to DB {db_name} in Snowflake: {e}")
                migration_fail.append((db_name, "*", "*", str(e)))
                continue

        setup_bucket(cluster, bucket_name)
        bucket = cluster.bucket(bucket_name)

        # Get all schemas and tables
        try:
            cur.execute(f"""
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM "{db_name}".INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
            """)
            tables = [(row[0], row[1]) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"❌ Error getting tables for {db_name}: {e}")
            migration_fail.append((db_name, "*", "*", str(e)))
            continue

        # Apply optional table filter
        if table_filter:
            tables = [(s, t) for s, t in tables if (s.upper(), t.upper()) in table_filter]
            if not tables:
                logger.info(f"   No matching tables in {db_name} for the given filter. Skipping.")
                continue

        # Apply --gold-only filter
        if args.gold_only:
            all_count = len(tables)
            tables = [(s, t) for s, t in tables if is_gold_table(gold_set, db_name, s, t)]
            logger.info(f"   --gold-only: keeping {len(tables)} gold tables, skipping {all_count - len(tables)} non-gold.")
            if not tables:
                logger.info(f"   No gold tables found in {db_name}. Skipping.")
                continue

        # Count gold vs non-gold for this DB
        gold_count = sum(1 for s, t in tables if is_gold_table(gold_set, db_name, s, t))
        logger.info(f"📝 Found {len(tables)} tables in {db_name} ({gold_count} gold, {len(tables) - gold_count} non-gold → {args.non_gold_limit} rows each).")

        for schema_name, table_name in tables:
            gold = is_gold_table(gold_set, db_name, schema_name, table_name)
            tag = "⭐ GOLD" if gold else f"📎 non-gold (limit {args.non_gold_limit})"
            logger.info(f"\n   -> Table {schema_name}.{table_name}  [{tag}]")
            
            # Match SQL++ baseline conventions which typically use `PUBLIC`.
            # We map Snowflake SCHEMA -> Couchbase Scope
            scope_name = sanitize_cb_name(schema_name)
            collection_name = sanitize_cb_name(table_name)

            setup_scope(bucket, scope_name)
            setup_collection(bucket, scope_name, collection_name)
            
            cb_coll = bucket.scope(scope_name).collection(collection_name)

            # Determine row limit: gold tables → full load, others → non_gold_limit
            if args.limit is not None:
                effective_limit = args.limit
            elif gold:
                effective_limit = None  # no limit — load all rows
            else:
                effective_limit = args.non_gold_limit

            # Query Snowflake
            limit_clause = f" LIMIT {effective_limit}" if effective_limit else ""
            query = f'SELECT * FROM "{db_name}"."{schema_name}"."{table_name}"{limit_clause}'
            logger.info(f"      Executing: {query}")
            
            try:
                cur.execute(query)
                # Fetch row headers
                col_names = [desc[0] for desc in cur.description]
                
                rows_upserted = 0
                rows_failed = 0
                while True:
                    # Fetch in batches
                    try:
                        rows = cur.fetchmany(10000)
                    except Exception as fetch_err:
                        if "Authentication token has expired" in str(fetch_err):
                            logger.warning(f"      ⚠️  Token expired mid-fetch ({rows_upserted} rows so far). Reconnecting...")
                            conn, cur = sf_reconnect(sf_config)
                            cur.execute(f"USE DATABASE \"{db_name}\"")
                            # Re-execute with OFFSET to skip already-loaded rows
                            offset_query = f'{query} OFFSET {rows_upserted + rows_failed}'
                            logger.info(f"      Resuming: {offset_query}")
                            cur.execute(offset_query)
                            col_names = [desc[0] for desc in cur.description]
                            continue
                        else:
                            raise
                    if not rows:
                        break
                    
                    for row in rows:
                        row_dict = dict(zip(col_names, row))
                        doc = clean_row_data(row_dict)
                        # Build document key e.g. "users::10"
                        doc_key = f"{collection_name}::{rows_upserted + rows_failed}"
                        try:
                            cb_coll.upsert(doc_key, doc)
                            rows_upserted += 1
                        except Exception as e:
                            rows_failed += 1
                            upsert_errors_total += 1
                            # Log every failure to file; first 3 per table to console
                            msg = f"      ⚠️  Upsert FAILED  key={doc_key}  error={e}"
                            logger.debug(msg)
                            if rows_failed <= 3:
                                logger.warning(msg)
                            elif rows_failed == 4:
                                logger.warning(f"      ⚠️  (further upsert errors for this table logged to {LOG_FILE})")
                            
                    logger.info(f"      ...upserted {rows_upserted} rows so far (failed {rows_failed})")
                
                if rows_failed > 0:
                    logger.warning(f"      ⚠️  {schema_name}.{table_name}: {rows_upserted} OK, {rows_failed} FAILED")
                    migration_fail.append((db_name, schema_name, table_name, f"{rows_failed} upsert failures"))
                else:
                    logger.info(f"      ✅ Finished migrating {schema_name}.{table_name}: {rows_upserted} rows total.")
                migration_ok.append((db_name, schema_name, table_name, rows_upserted))
            except Exception as e:
                if "Authentication token has expired" in str(e):
                    logger.warning(f"      ⚠️  Token expired on query start. Reconnecting...")
                    conn, cur = sf_reconnect(sf_config)
                    # Put this table back — we'll retry by not incrementing the loop
                    # For simplicity, log it as failed; the user can re-run with --tables
                    logger.error(f"      ❌ Re-run needed for {db_name}.{schema_name}.{table_name} (token expired before data fetch)")
                    migration_fail.append((db_name, schema_name, table_name, "Token expired — reconnected, needs re-run"))
                else:
                    logger.error(f"      ❌ Error querying/upserting {db_name}.{schema_name}.{table_name}: {e}")
                    migration_fail.append((db_name, schema_name, table_name, str(e)))

    cur.close()
    conn.close()

    # ── Summary Report ────────────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("📊 MIGRATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Tables succeeded : {len(migration_ok)}")
    logger.info(f"  Tables with issues: {len(migration_fail)}")
    logger.info(f"  Total upsert errors: {upsert_errors_total}")

    if migration_fail:
        logger.info("\n❌ FAILED / PARTIAL tables:")
        for db, schema, table, err in migration_fail:
            logger.info(f"   {db}.{schema}.{table}  —  {err}")

    logger.info(f"\n📄 Full debug log: {LOG_FILE}")
    logger.info("🎉 Migration Complete!")

if __name__ == "__main__":
    main()
