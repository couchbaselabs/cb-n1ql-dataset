#!/usr/bin/env python3
"""
Automated BigQuery to Couchbase Ingestion Script for Spider2 Dataset

This script:
1. Scans the Spider2 resource/databases/bigquery folder
2. Discovers all tables from the JSON metadata files
3. Creates Couchbase bucket/scopes/collections automatically
4. Fetches data from BigQuery and upserts into Couchbase

Structure in Couchbase:
- Bucket: spider2
- Scope: <bq_dataset_name> (e.g., "austin_bikeshare", "cms_medicare")
- Collection: <table_name> (e.g., "bikeshare_stations", "crime")
"""

import ast
import datetime
import time
import json
import os
import re
import math
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from google.cloud import bigquery
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.management.buckets import CreateBucketSettings, BucketType
from datetime import timedelta


# --- CONFIGURATION ---
CB_HOST = "couchbase://127.0.0.1"
CB_USER = "Administrator"
CB_PASS = "password"
CB_RAM_QUOTA_MB = 1024  # Increase if you have more RAM

# Path to Spider2 resources
SPIDER2_BASE_PATH = Path(__file__).parent / "Spider2/spider2-lite/resource/databases"
SPIDER2_BQ_PATH = SPIDER2_BASE_PATH / "bigquery"
GOLD_TABLES_PATH = Path(__file__).parent / "Spider2/methods/gold-tables/spider2-lite-gold-tables.jsonl"

# Limit rows per table (None for all, or set a number for testing)
LIMIT_ROWS: Optional[int] = 1

# Process ONLY BigQuery for now
DIALECTS_TO_PROCESS = ["bigquery"]

# Only ingest tables present in gold-tables.jsonl (Set to True to filter, False for all)
ONLY_GOLD_TABLES = False

# Databases to process (None for all, or list specific ones)
DATABASES_TO_PROCESS: Optional[list] = None

# Skip these databases (large or problematic)
SKIP_DATABASES = []

# Dry run mode (just print what would be done, don't actually migrate)
DRY_RUN = False


@dataclass
class TableInfo:
    """Represents a table discovered from Spider2 metadata"""
    dialect: str           # bigquery, snowflake, sqlite
    db_name: str           # Spider2 database name (e.g., "austin")
    scope_name: str        # Couchbase scope name
    table_name: str        # Table name (e.g., "bikeshare_stations")
    table_fullname: str    # Full source path (for BQ query or SQLite table name)
    column_names: list
    json_path: str         # Path to the JSON metadata file


def sanitize_name(name: str) -> str:
    """Sanitize name for Couchbase (alphanumeric, underscore, hyphen, max 251 chars)"""
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
    return sanitized[:251]


def discover_tables(base_path: Path, databases: Optional[list] = None, skip: list = None) -> list[TableInfo]:
    """
    Scan Spider2 resource folders and discover all tables for all dialects.
    """
    tables = []
    skip = skip or []
    
    # Load gold tables if filter enabled
    gold_tables = set()
    if ONLY_GOLD_TABLES and GOLD_TABLES_PATH.exists():
        with open(GOLD_TABLES_PATH) as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    gold_tables.update(obj.get("gold_tables", []))
                except:
                    continue
        print(f"  Loaded {len(gold_tables)} gold tables for filtering.")

    dialects = DIALECTS_TO_PROCESS
    
    for dialect in dialects:
        dialect_path = base_path / dialect
        if not dialect_path.exists():
            continue
            
        for db_dir in sorted(dialect_path.iterdir()):
            if not db_dir.is_dir():
                continue
                
            db_name = db_dir.name
            if databases and db_name not in databases:
                continue
            if db_name in skip:
                continue
            
            # Use rglob to find all .json files recursively
            for json_file in db_dir.rglob("*.json"):
                if json_file.name == "DDL.csv":
                    continue
                    
                try:
                    with open(json_file) as f:
                        metadata = json.load(f)
                    
                    table_fullname = metadata.get("table_fullname", json_file.stem)
                    
                    # Filter for gold tables
                    if ONLY_GOLD_TABLES and table_fullname not in gold_tables:
                        continue
                    
                    # Determine scope name
                    rel_path = json_file.relative_to(db_dir)
                    if dialect == "bigquery":
                        # e.g., bigquery-public-data.austin_bikeshare/stations.json
                        scope_name = rel_path.parts[0].split(".")[-1]
                    else:
                        scope_name = rel_path.parts[0] if len(rel_path.parts) > 1 else f"{db_name}_scope"
                    
                    tables.append(TableInfo(
                        dialect=dialect,
                        db_name=db_name,
                        scope_name=scope_name,
                        table_name=metadata.get("table_name", json_file.stem),
                        table_fullname=table_fullname,
                        column_names=metadata.get("column_names", []),
                        json_path=str(json_file)
                    ))
                except Exception as e:
                    print(f"  Error reading {json_file}: {e}")
                    
    return tables


def setup_bucket(cluster, bucket_name: str) -> None:
    """Create a bucket if it doesn't exist"""
    bm = cluster.buckets()
    try:
        bm.get_bucket(bucket_name)
    except Exception:
        print(f"  Creating Bucket '{bucket_name}'...")
        bm.create_bucket(CreateBucketSettings(
            name=bucket_name,
            bucket_type=BucketType.COUCHBASE,
            ram_quota_mb=CB_RAM_QUOTA_MB
        ))
        time.sleep(5)


def ensure_scope_and_collection(cluster, bucket_name: str, scope_name: str, collection_name: str) -> None:
    """Create scope and collection if they don't exist"""
    bucket = cluster.bucket(bucket_name)
    cm = bucket.collections()
    
    scope_name = sanitize_name(scope_name)
    collection_name = sanitize_name(collection_name)
    
    # Check/create scope
    try:
        scopes = [s.name for s in cm.get_all_scopes()]
        if scope_name not in scopes:
            print(f"    Creating scope '{scope_name}' in bucket '{bucket_name}'...")
            cm.create_scope(scope_name)
            time.sleep(1)
    except Exception as e:
        print(f"    Warning: Scope check failed: {e}")
    
    # Check/create collection
    try:
        scope_def = next((s for s in cm.get_all_scopes() if s.name == scope_name), None)
        collections = [c.name for c in scope_def.collections] if scope_def else []
        
        if collection_name not in collections:
            print(f"    Creating collection '{collection_name}' in '{bucket_name}.{scope_name}'...")
            cm.create_collection(scope_name, collection_name)
            time.sleep(1)
    except Exception as e:
        print(f"    Warning: Collection check failed: {e}")


def clean_value(value):
    """Clean a single value for JSON serialization"""
    if value is None:
        return None
    elif isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    elif isinstance(value, bytes):
        return value.decode('utf-8', errors='replace')
    elif isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    elif isinstance(value, str):
        # Try to parse stringified lists/dicts
        if value.startswith('[') or value.startswith('{'):
            try:
                return ast.literal_eval(value)
            except:
                pass
        return value
    elif isinstance(value, (list, tuple)):
        return [clean_value(v) for v in value]
    elif isinstance(value, dict):
        return {k: clean_value(v) for k, v in value.items()}
    else:
        return value


def clean_row(row_dict: dict) -> dict:
    """Clean an entire row for Couchbase ingestion"""
    return {k: clean_value(v) for k, v in row_dict.items()}


def generate_doc_id(row: dict, table_name: str, index: int) -> str:
    """Generate a document ID from the row data"""
    # Try common ID field patterns
    id_fields = ['id', 'ID', 'Id', '_id',
                 f'{table_name}_id', f'{table_name}Id',
                 'key', 'pk', 'primary_key',
                 'station_id', 'trip_id', 'order_id', 'trade_id',
                 'TradeReportID', 'OrderID']
    
    for field in id_fields:
        if field in row and row[field] is not None:
            return f"{table_name}::{row[field]}"
    
    # Fallback to index-based ID
    return f"{table_name}::{index}"


def migrate_table(bq_client, cluster, table: TableInfo, limit: Optional[int] = None) -> tuple[int, int]:
    """
    Migrate a single table from BigQuery to Couchbase.
    Returns (success_count, error_count)
    """
    bucket_name = sanitize_name(table.db_name)
    scope_name = sanitize_name(table.scope_name)
    collection_name = sanitize_name(table.table_name)
    
    print(f"\n  Migrating [{table.dialect}]: {table.table_fullname}")
    print(f"    -> Couchbase: {bucket_name}.{scope_name}.{collection_name}")
    
    if DRY_RUN:
        print("    [DRY RUN] Would migrate this table")
        return (0, 0)
    
    # Ensure bucket, scope and collection exist
    setup_bucket(cluster, bucket_name)
    ensure_scope_and_collection(cluster, bucket_name, scope_name, collection_name)
    
    # Get collection reference
    cb_coll = cluster.bucket(bucket_name).scope(scope_name).collection(collection_name)
    
    # For BigQuery, we fetch from live BQ
    if table.dialect == "bigquery":
        # Build query
        query = f"SELECT * FROM `{table.table_fullname}`"
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute query
        try:
            query_job = bq_client.query(query)
        except Exception as e:
            print(f"    Error querying BigQuery: {e}")
            return (0, 1)
        
        success = 0
        errors = 0
        
        for idx, row in enumerate(query_job):
            try:
                doc = clean_row(dict(row))
                doc_id = generate_doc_id(doc, table.table_name, idx)
                cb_coll.upsert(doc_id, doc)
                success += 1
                if success % 500 == 0:
                    print(f"    Migrated {success} documents...")
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"    Error on row {idx}: {e}")
        
        print(f"    Completed: {success} docs, {errors} errors")
        return (success, errors)
    
    # For Snowflake/SQLite in this benchmark, we can often ingest from the sample_rows in the JSON
    else:
        try:
            with open(table.json_path) as f:
                data = json.load(f)
            
            sample_rows = data.get("sample_rows", [])
            success = 0
            errors = 0
            
            # If we want to ingest more than sample rows for SQLite, we'd need to connect to .sqlite files
            # For now, let's ingest from the JSON sample_rows as a baseline
            rows_to_ingest = sample_rows[:limit] if limit else sample_rows
            
            for idx, row in enumerate(rows_to_ingest):
                try:
                    doc = clean_row(row)
                    doc_id = generate_doc_id(doc, table.table_name, idx)
                    cb_coll.upsert(doc_id, doc)
                    success += 1
                except Exception as e:
                    errors += 1
            
            print(f"    Completed (from JSON samples): {success} docs, {errors} errors")
            return (success, errors)
        except Exception as e:
            print(f"    Error reading JSON data: {e}")
            return (0, 1)


def print_summary(tables: list[TableInfo]) -> None:
    """Print summary of discovered tables"""
    print("\n" + "="*60)
    print("DISCOVERY SUMMARY")
    print("="*60)
    
    # Group by dialect and database
    by_dialect = {}
    for t in tables:
        by_dialect.setdefault(t.dialect, {}).setdefault(t.db_name, []).append(t)
    
    print(f"Total tables: {len(tables)}")
    
    for dialect in sorted(by_dialect.keys()):
        d_dbs = by_dialect[dialect]
        print(f"\nDialect: {dialect}")
        print(f"  Databases: {len(d_dbs)}")
        print(f"  Total tables: {sum(len(v) for v in d_dbs.values())}")


def main():
    print("="*60)
    print("Spider2 Data -> Couchbase Migration")
    print("="*60)
    
    # 1. Discover all tables
    print("\n[1/4] Discovering tables from Spider2 metadata...")
    tables = discover_tables(
        SPIDER2_BASE_PATH,
        databases=DATABASES_TO_PROCESS,
        skip=SKIP_DATABASES
    )
    
    if not tables:
        print("No tables found!")
        return
    
    print_summary(tables)
    
    if DRY_RUN:
        print("\n[DRY RUN MODE] - No data will be migrated")
    
    # 2. Connect to BigQuery
    print("\n[2/4] Connecting to BigQuery...")
    try:
        bq_client = bigquery.Client()
        print("  Connected.")
    except Exception as e:
        print(f"  BigQuery Connection Failed (skipping live BQ): {e}")
        bq_client = None
    
    # 3. Connect to Couchbase
    print("\n[3/4] Connecting to Couchbase...")
    try:
        auth = PasswordAuthenticator(CB_USER, CB_PASS)
        cluster = Cluster(CB_HOST, ClusterOptions(auth))
        cluster.wait_until_ready(timedelta(seconds=10))
        print(f"  Connected to {CB_HOST}")
    except Exception as e:
        print(f"  Couchbase Connection Failed: {e}")
        return
    
    # 4. Migrate tables
    print("\n[4/4] Starting migration...")
    total_success = 0
    total_errors = 0
    table_errors = 0
    
    for i, table in enumerate(tables, 1):
        print(f"\n[{i}/{len(tables)}] {table.dialect} | {table.db_name}/{table.scope_name}/{table.table_name}")
        try:
            success, errors = migrate_table(bq_client, cluster, table, LIMIT_ROWS)
            total_success += success
            total_errors += errors
        except Exception as e:
            print(f"  Table migration failed: {e}")
            table_errors += 1
    
    # Final summary
    print("\n" + "="*60)
    print("MIGRATION COMPLETE")
    print("="*60)
    print(f"Tables processed: {len(tables)}")
    print(f"Tables failed: {table_errors}")
    print(f"Documents migrated: {total_success}")
    print(f"Document errors: {total_errors}")


if __name__ == "__main__":
    main()
