"""
Evaluate Spider 2.0-lite SQL++ (N1QL) submissions against Couchbase — BigQuery variant.

For each BigQuery database:
  1. Creates a Couchbase bucket/scopes/collections matching the BigQuery structure.
  2. Loads gold tables fully from BigQuery; non-gold tables with LIMIT 10.
  3. Runs all SQL++ predictions for that database.
  4. Tears down (drops) the bucket.
  5. Moves on to the next database.

All result CSVs are accumulated into a single output folder across all databases,
and the final evaluation score covers every evaluated instance.
"""
import argparse
import json
import math
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from threading import Lock

import pandas as pd
from tqdm import tqdm

# Google BigQuery SDK
from google.cloud import bigquery as bq

# Couchbase SDK
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.management.buckets import CreateBucketSettings, BucketType

# Flatten JSON -> flat rows (and CSV) for comparison
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from flatten.flatten import process_json as flatten_process_json


# --- Couchbase config (override via env or CLI) ---
CB_HOST = os.environ.get("COUCHBASE_HOST", "couchbase://127.0.0.1")
CB_USER = os.environ.get("COUCHBASE_USER", "Administrator")
CB_PASS = os.environ.get("COUCHBASE_PASS", "password")

# Bucket RAM quota in MB
BUCKET_RAM_QUOTA_MB = 10000


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

class TeeOutput:
    """Mirror stdout/stderr to both console and a logfile with thread safety."""

    def __init__(self, filename: str):
        self.console = sys.stdout
        self.file = open(filename, "w")
        self.lock = Lock()

    def write(self, message: str) -> None:
        with self.lock:
            self.console.write(message)
            self.file.write(message)

    def flush(self) -> None:
        with self.lock:
            self.console.flush()
            self.file.flush()

    def close(self) -> None:
        self.file.close()


sys.stdout = TeeOutput("log_sqlpp_bigquery.txt")
sys.stderr = sys.stdout


# ---------------------------------------------------------------------------
# CSV comparison (identical to SQLite version)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=None)
def load_gold_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)


def compare_multi_pandas_table(pred, multi_gold, multi_condition_cols=None, multi_ignore_order=False):
    if not multi_gold:
        return 0
    if multi_condition_cols in (None, [], [[]], [None]):
        multi_condition_cols = [[] for _ in range(len(multi_gold))]
    elif len(multi_gold) > 1 and not all(isinstance(s, list) for s in multi_condition_cols):
        multi_condition_cols = [multi_condition_cols for _ in range(len(multi_gold))]
    multi_ignore_order = [multi_ignore_order for _ in range(len(multi_gold))]
    for i, gold in enumerate(multi_gold):
        if compare_pandas_table(pred, gold, multi_condition_cols[i], multi_ignore_order[i]):
            return 1
    return 0


def compare_pandas_table(pred, gold, condition_cols=None, ignore_order=False):
    tolerance = 1e-2

    def normalize(value):
        if pd.isna(value):
            return 0
        return value

    def vectors_match(v1, v2, tol=tolerance, ignore_order_=False):
        v1 = [normalize(x) for x in v1]
        v2 = [normalize(x) for x in v2]
        if ignore_order_:
            v1 = sorted(v1, key=lambda x: (x is None, str(x), isinstance(x, (int, float))))
            v2 = sorted(v2, key=lambda x: (x is None, str(x), isinstance(x, (int, float))))
        if len(v1) != len(v2):
            return False
        for a, b in zip(v1, v2):
            if pd.isna(a) and pd.isna(b):
                continue
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                if not math.isclose(float(a), float(b), abs_tol=tol):
                    return False
            elif a != b:
                return False
        return True

    if condition_cols:
        if not isinstance(condition_cols, (list, tuple)):
            condition_cols = [condition_cols]
        gold_cols = gold.iloc[:, condition_cols]
    else:
        gold_cols = gold
    pred_cols = pred
    t_gold_list = gold_cols.transpose().values.tolist()
    t_pred_list = pred_cols.transpose().values.tolist()
    score = 1
    for gold_vector in t_gold_list:
        if not any(vectors_match(gold_vector, pv, ignore_order_=ignore_order) for pv in t_pred_list):
            score = 0
            break
    return score


# ---------------------------------------------------------------------------
# Couchbase setup / teardown helpers
# ---------------------------------------------------------------------------

def setup_bucket(cluster, bucket_name):
    """Create the bucket if it does not already exist."""
    bm = cluster.buckets()
    try:
        bm.get_bucket(bucket_name)
        print(f"   -> Bucket '{bucket_name}' exists.")
    except Exception:
        print(f"   -> Creating Bucket '{bucket_name}' with {BUCKET_RAM_QUOTA_MB}MB RAM quota...")
        bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=BUCKET_RAM_QUOTA_MB,
            )
        )
        time.sleep(3)


def teardown_bucket(cluster, bucket_name):
    """Drop the entire bucket to free resources."""
    bm = cluster.buckets()
    try:
        bm.drop_bucket(bucket_name)
        print(f"   -> Dropped bucket '{bucket_name}'.")
        time.sleep(2)
    except Exception as e:
        print(f"   -> Warning: could not drop bucket '{bucket_name}': {e}")


def setup_scope(bucket, scope_name):
    cm = bucket.collections()
    scopes = [s.name for s in cm.get_all_scopes()]
    if scope_name not in scopes:
        print(f"   -> Creating Scope '{scope_name}'...")
        cm.create_scope(scope_name)
        time.sleep(2)
    else:
        print(f"   -> Scope '{scope_name}' exists.")


def setup_collection(bucket, scope_name, collection_name):
    cm = bucket.collections()
    scope_def = next((s for s in cm.get_all_scopes() if s.name == scope_name), None)
    collections = [c.name for c in scope_def.collections] if scope_def else []
    if collection_name not in collections:
        print(f"   -> Creating Collection '{collection_name}' in scope '{scope_name}'...")
        cm.create_collection(scope_name, collection_name)
        time.sleep(2)
    else:
        print(f"   -> Collection '{collection_name}' exists.")


# ---------------------------------------------------------------------------
# BigQuery -> Couchbase data loading
# ---------------------------------------------------------------------------

def clean_value(value):
    """Convert a BigQuery value to something JSON-safe for Couchbase."""
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            import base64
            return base64.b64encode(value).decode("utf-8")
    if isinstance(value, (int, float, str, bool)):
        return value
    # datetime, date, Decimal, etc.
    return str(value)


def clean_row(row_dict):
    return {k: clean_value(v) for k, v in row_dict.items()}


def load_bigquery_table_to_couchbase(
    bq_client,
    cb_collection,
    scope_path: str,
    table_name: str,
    is_gold: bool,
):
    """
    Fetch rows from a BigQuery table and upsert into a Couchbase collection.
    Gold tables are fetched fully; non-gold tables are limited to 10 rows.
    """
    fq_table = f"{scope_path}.{table_name}"
    limit_clause = "" if is_gold else " LIMIT 10"
    query = f"SELECT * FROM `{scope_path}`.`{table_name}`{limit_clause}"

    try:
        query_job = bq_client.query(query)
        rows = list(query_job.result())
    except Exception as e:
        print(f"   [ERROR] BigQuery query for {fq_table}: {e}")
        return 0, 1

    count = 0
    errors = 0
    for row in rows:
        try:
            doc = clean_row(dict(row))
            doc_id = f"{table_name}::{count}"
            cb_collection.upsert(str(doc_id), doc)
            count += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"   [!] Upsert error for {fq_table} row {count}: {e}")

    label = "FULL" if is_gold else "LIMIT 10"
    if errors:
        print(f"   [!] {fq_table} ({label}): {count} docs loaded, {errors} FAILED")
    else:
        print(f"   [ok] {fq_table} ({label}): {count} docs loaded")

    return count, errors


def setup_db_in_couchbase(cluster, bq_client, db_entry, gold_tables_set):
    """
    Create bucket/scopes/collections for one db and load data from BigQuery.
    db_entry comes from bigquery_load_paths.json.
    gold_tables_set is the set of fully-qualified gold table names for this db.
    """
    db_name = db_entry["db"]
    bucket_name = db_name  # keep exact name from bigquery_load_paths.json

    print(f"\n{'=' * 60}")
    print(f"Setting up DB: {db_name}  (bucket: {bucket_name})")
    print(f"{'=' * 60}")

    setup_bucket(cluster, bucket_name)
    bucket = cluster.bucket(bucket_name)

    total_docs = 0
    total_errors = 0

    for scope_info in db_entry["scopes"]:
        scope_name = scope_info["scope"]
        scope_path = scope_info["scope_path"]
        tables = scope_info["tables"]

        setup_scope(bucket, scope_name)

        for table_name in tables:
            setup_collection(bucket, scope_name, table_name)
            cb_coll = bucket.scope(scope_name).collection(table_name)

            fq_table = f"{scope_path}.{table_name}"
            is_gold = fq_table in gold_tables_set

            docs, errs = load_bigquery_table_to_couchbase(
                bq_client, cb_coll, scope_path, table_name, is_gold
            )
            total_docs += docs
            total_errors += errs

    print(f"   DB '{db_name}' loaded: {total_docs} docs, {total_errors} errors")
    return bucket_name


# ---------------------------------------------------------------------------
# SQL++ execution & evaluation (reused from SQLite version)
# ---------------------------------------------------------------------------

def get_couchbase_sqlpp_result(
    cluster,
    sqlpp_query: str,
    save_dir=None,
    file_name: str = "result.csv",
    instance_id: str = None,
    timeout_seconds: int = 60,
):
    """
    Run SQL++ (N1QL) on Couchbase WITHOUT query_context.
    Queries are expected to use fully qualified bucket.scope.collection references.
    """
    prefix = f"[{instance_id}] " if instance_id else ""
    try:
        opts = QueryOptions(
            timeout=timeout_seconds,
        )
        result = cluster.query(sqlpp_query, opts)
        rows = list(result.rows())
    except Exception as e:
        error_message = str(e)
        print(f"{prefix}Couchbase N1QL error: {error_message}")
        return False, error_message

    if not rows:
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            pd.DataFrame().to_csv(os.path.join(save_dir, file_name), index=False)
        return True, None

    flat_rows = flatten_process_json(rows)
    if not flat_rows:
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            pd.DataFrame().to_csv(os.path.join(save_dir, file_name), index=False)
        return True, None

    if save_dir:
        os.makedirs(save_dir, exist_ok=True)
        df = pd.DataFrame(flat_rows)
        df.to_csv(os.path.join(save_dir, file_name), index=False)

    return True, None


def extract_sql_query(pred_sql_query: str) -> str:
    for pattern in (r"```sql\n(.*?)\n```", r"```sqlpp\n(.*?)\n```", r"```n1ql\n(.*?)\n```"):
        match = re.search(pattern, pred_sql_query, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return pred_sql_query


def resolve_gold_paths(instance_id: str, gold_result_dir: str):
    base_path = Path(gold_result_dir) / f"{instance_id}.csv"
    if base_path.exists():
        return [base_path], True
    pattern = re.compile(rf"^{re.escape(instance_id)}(_[a-z])?\.csv$")
    csv_files = sorted(
        file for file in os.listdir(gold_result_dir)
        if pattern.match(file)
    )
    return [Path(gold_result_dir) / file for file in csv_files], False


def evaluate_single_sql_instance(
    instance_id: str,
    eval_standard_dict: dict,
    pred_result_dir: str,
    gold_result_dir: str,
    temp_dir: Path,
    result_csv_dir: str = None,
    timeout: int = 60,
    cluster=None,
):
    error_info = None
    score = 0
    pred_sql_query = ""

    try:
        pred_sql_path = Path(pred_result_dir) / f"{instance_id}.sqlpp"
        pred_sql_query = pred_sql_path.read_text()
        pred_sql_query = extract_sql_query(pred_sql_query)

        thread_temp_dir = Path(temp_dir) / f"thread_{threading.get_ident()}_{instance_id}"
        thread_temp_dir.mkdir(parents=True, exist_ok=True)
        result_file = f"{instance_id}.csv"

        exe_flag, dbms_error_info = get_couchbase_sqlpp_result(
            cluster,
            pred_sql_query,
            save_dir=str(thread_temp_dir),
            file_name=result_file,
            instance_id=instance_id,
            timeout_seconds=timeout,
        )

        if not exe_flag:
            score = 0
            error_info = dbms_error_info
        else:
            pred_csv_path = thread_temp_dir / result_file
            print(f"[{instance_id}] Successfully ran query and saved CSV to {pred_csv_path}")
            pred_pd = pd.read_csv(pred_csv_path)

            if result_csv_dir:
                os.makedirs(result_csv_dir, exist_ok=True)
                shutil.copy2(pred_csv_path, Path(result_csv_dir) / result_file)

            gold_paths, is_single = resolve_gold_paths(instance_id, gold_result_dir)
            standard = eval_standard_dict.get(instance_id, {})
            condition_cols = standard.get("condition_cols")
            ignore_order = standard.get("ignore_order", False)

            if not gold_paths:
                score = 0
                error_info = error_info or "No matching gold file found"
            elif is_single:
                try:
                    gold_pd = load_gold_csv(str(gold_paths[0]))
                    score = compare_pandas_table(pred_pd, gold_pd, condition_cols, ignore_order)
                except Exception as e:
                    print(f"{instance_id}: compare against {gold_paths[0]} failed: {e}")
                    score = 0
                    error_info = f"Python Script Error:{str(e)}"
                if score == 0 and error_info is None:
                    error_info = "Result Error"
            else:
                try:
                    gold_pds = [load_gold_csv(str(p)) for p in gold_paths]
                    score = compare_multi_pandas_table(pred_pd, gold_pds, condition_cols, ignore_order)
                except Exception as e:
                    print(f"{instance_id}: multi-compare against {gold_paths} failed: {e}")
                    score = 0
                    error_info = f"Python Script Error:{str(e)}"
                if score == 0 and error_info is None:
                    error_info = "Result Error"

    except Exception as e:
        print(f"Error evaluating {instance_id}: {e}")
        score = 0
        error_info = f"Evaluation Error: {str(e)}"
        pred_sql_query = ""

    if score == 1:
        print(f" Comparison matched successfully.")
    else:
        print(f"Comparison did not match. Error: {error_info}")

    return {
        "instance_id": instance_id,
        "score": score,
        "pred_sql": pred_sql_query,
        "error_info": error_info,
    }


def evaluate_single_exec_result_instance(
    instance_id: str,
    eval_standard_dict: dict,
    pred_result_dir: str,
    gold_result_dir: str,
):
    error_info = None
    score = 0
    try:
        pred_pd = pd.read_csv(Path(pred_result_dir) / f"{instance_id}.csv")
        gold_paths, is_single = resolve_gold_paths(instance_id, gold_result_dir)
        standard = eval_standard_dict.get(instance_id, {})
        condition_cols = standard.get("condition_cols")
        ignore_order = standard.get("ignore_order", False)

        if not gold_paths:
            score = 0
            error_info = "No matching gold file found"
        elif is_single:
            try:
                gold_pd = load_gold_csv(str(gold_paths[0]))
                score = compare_pandas_table(pred_pd, gold_pd, condition_cols, ignore_order)
            except Exception as e:
                print(f"{instance_id}: compare against {gold_paths[0]} failed: {e}")
                score = 0
                error_info = f"Python Script Error:{str(e)}"
            if score == 0 and error_info is None:
                error_info = "Result Error"
        else:
            try:
                gold_pds = [load_gold_csv(str(p)) for p in gold_paths]
                score = compare_multi_pandas_table(pred_pd, gold_pds, condition_cols, ignore_order)
            except Exception as e:
                print(f"{instance_id}: multi-compare against {gold_paths} failed: {e}")
                score = 0
                error_info = f"Python Script Error:{str(e)}"
            if score == 0 and error_info is None:
                error_info = "Result Error"
    except Exception as e:
        print(f"{instance_id} ERROR: {e}")
        score = 0
        error_info = f"Evaluation Error: {str(e)}"

    if score == 1:
        print(f" Comparison matched successfully.")
    else:
        print(f" Comparison did not match. Error: {error_info}")

    return {
        "instance_id": instance_id,
        "score": score,
        "pred_sql": None,
        "error_info": error_info,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_jsonl_to_dict(jsonl_file: str) -> dict:
    data_dict = {}
    with open(jsonl_file, "r") as f:
        for line in f:
            item = json.loads(line.strip())
            data_dict[item["instance_id"]] = item
    return data_dict


def save_correct_ids_to_csv(output_results, result_dir: str):
    correct_ids = [item["instance_id"] for item in output_results if item["score"] == 1]
    transformed_ids = []
    for item in correct_ids:
        if item.startswith(("bq", "ga", "local")):
            transformed_ids.append(f"sf_{item}")
        else:
            transformed_ids.append(item)
    csv_file = f"{result_dir}-ids.csv"
    pd.DataFrame({"instance_id": transformed_ids}).to_csv(csv_file, index=False)
    print(f"Correct IDs saved to: {csv_file}")
    return csv_file


# ---------------------------------------------------------------------------
# Main evaluation loop
# ---------------------------------------------------------------------------

def evaluate_spider2sql_sqlpp_bigquery(args, temp_dir: Path, cluster):
    mode = args.mode
    gold_result_dir = os.path.join(args.gold_dir, "exec_result")
    pred_result_dir = args.result_dir

    eval_standard_dict = load_jsonl_to_dict(os.path.join(args.gold_dir, "spider2lite_eval.jsonl"))

    # Load combined config: db -> scopes, gold_tables, instances
    with open(args.db_config, "r", encoding="utf-8") as f:
        db_config = json.load(f)

    # Determine which prediction files we have
    if mode == "sql":
        pred_ids = {Path(f).stem for f in os.listdir(pred_result_dir) if f.endswith(".sqlpp")}
    elif mode == "exec_result":
        pred_ids = {Path(f).stem for f in os.listdir(pred_result_dir) if f.endswith(".csv")}
    else:
        pred_ids = set()

    gold_ids = set(eval_standard_dict.keys())

    # Setup shared result CSV dir (persists across all dbs)
    result_csv_dir = None
    if mode == "sql":
        result_csv_path = Path(f"{pred_result_dir}_csv")
        if result_csv_path.exists():
            shutil.rmtree(result_csv_path)
        result_csv_path.mkdir(parents=True, exist_ok=True)
        result_csv_dir = str(result_csv_path)

    # BigQuery client
    bq_client = bq.Client()

    max_workers = getattr(args, "max_workers", 8)
    all_output_results = []
    total_bigquery_questions = 0

    # Process DBs one at a time
    for db_entry in sorted(db_config, key=lambda e: e["db"]):
        db_name = db_entry["db"]
        gold_tables = set(db_entry.get("gold_tables", []))
        instances = db_entry.get("instances", [])
        instance_ids = [inst["instance_id"] for inst in instances]

        # Filter to instances that have predictions AND are in the gold set
        eval_ids = sorted(set(instance_ids) & pred_ids & gold_ids)
        total_bigquery_questions += len(set(instance_ids) & gold_ids)

        if not eval_ids:
            print(f"\n[SKIP] {db_name}: no predictions to evaluate.")
            continue

        # 1. Setup: load data from BigQuery -> Couchbase
        bucket_name = setup_db_in_couchbase(cluster, bq_client, db_entry, gold_tables)

        # 2. Evaluate all predictions for this db
        print(f"\n--- Evaluating {len(eval_ids)} prediction(s) for {db_name} ---")

        db_results = []
        if mode == "sql":
            workers = min(max_workers, len(eval_ids)) or 1
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_id = {
                    executor.submit(
                        evaluate_single_sql_instance,
                        iid,
                        eval_standard_dict,
                        pred_result_dir,
                        gold_result_dir,
                        temp_dir=temp_dir,
                        result_csv_dir=result_csv_dir,
                        timeout=getattr(args, "timeout", 60),
                        cluster=cluster,
                    ): iid
                    for iid in eval_ids
                }
                for future in tqdm(as_completed(future_to_id), total=len(eval_ids), desc=f"Eval {db_name}"):
                    db_results.append(future.result())

        elif mode == "exec_result":
            workers = min(max_workers, len(eval_ids)) or 1
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_id = {
                    executor.submit(
                        evaluate_single_exec_result_instance,
                        iid,
                        eval_standard_dict,
                        pred_result_dir,
                        gold_result_dir,
                    ): iid
                    for iid in eval_ids
                }
                for future in tqdm(as_completed(future_to_id), total=len(eval_ids), desc=f"Eval {db_name}"):
                    db_results.append(future.result())

        all_output_results.extend(db_results)

        # Per-db summary
        db_correct = sum(item["score"] for item in db_results)
        print(f"[{db_name}] Score: {db_correct}/{len(db_results)}")

        # 3. Teardown: drop bucket
        teardown_bucket(cluster, bucket_name)

    # --- Final aggregated results ---
    all_output_results.sort(key=lambda item: item["instance_id"])

    if not all_output_results:
        print("No predictions evaluated.")
        return []

    print(f"\n{'=' * 60}")
    print("FINAL RESULTS (all BigQuery databases)")
    print(f"{'=' * 60}")
    print({item["instance_id"]: item["score"] for item in all_output_results})

    correct = sum(item["score"] for item in all_output_results)
    total_eval = len(all_output_results)

    print(f"Final score: {correct / total_eval:.4f}, Correct: {correct}, Evaluated: {total_eval}")
    print(f"Real score (out of {total_bigquery_questions} BigQuery questions): "
          f"{correct / total_bigquery_questions:.4f}, Correct: {correct}, Total BigQuery questions: {total_bigquery_questions}")

    if mode == "sql" and result_csv_dir:
        print(f"Execution results (all dbs) saved to: {result_csv_dir}")

    save_correct_ids_to_csv(all_output_results, pred_result_dir)

    return all_output_results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Evaluate Spider 2.0-lite SQL++ (N1QL) on Couchbase — BigQuery variant."
    )
    parser.add_argument("--mode", type=str, choices=["sql", "exec_result"], default="sql",
                        help="Mode of submission results")
    parser.add_argument("--result_dir", type=str,
                        default=str(_REPO_ROOT / "spider2-lite/evaluation_suite/submission"),
                        help="Result directory (contains .sqlpp files)")
    parser.add_argument("--gold_dir", type=str, default="gold",
                        help="Gold directory (spider2lite_eval.jsonl + exec_result/*.csv)")
    parser.add_argument("--db_config", type=str,
                        default=str(_REPO_ROOT / "bigquery_load_paths_withgold.json"),
                        help="Path to combined bigquery_load_paths_withgold.json")
    parser.add_argument("--max_workers", type=int, default=20,
                        help="Maximum number of worker threads per db")
    parser.add_argument("--timeout", type=int, default=6000,
                        help="N1QL execution timeout in seconds")
    parser.add_argument("--temp_dir", type=str, default=None,
                        help="Optional working directory for temporary files.")
    parser.add_argument("--cb_host", type=str, default=None,
                        help="Couchbase connection string")
    parser.add_argument("--cb_user", type=str, default=None,
                        help="Couchbase user")
    parser.add_argument("--cb_pass", type=str, default=None,
                        help="Couchbase password")
    args = parser.parse_args()

    if args.cb_host is not None:
        CB_HOST = args.cb_host
    if args.cb_user is not None:
        CB_USER = args.cb_user
    if args.cb_pass is not None:
        CB_PASS = args.cb_pass

    print(f"Connecting to Couchbase at {CB_HOST}...")
    try:
        auth = PasswordAuthenticator(CB_USER, CB_PASS)
        cluster = Cluster(CB_HOST, ClusterOptions(auth))
        cluster.wait_until_ready(timedelta(seconds=10))
        print("Couchbase connected.")
    except Exception as e:
        print(f"Couchbase connection failed: {e}")
        sys.exit(1)

    auto_temp = False
    if args.temp_dir:
        temp_path = Path(args.temp_dir).expanduser().resolve()
        if temp_path.exists():
            shutil.rmtree(temp_path)
        temp_path.mkdir(parents=True, exist_ok=True)
    else:
        temp_path = Path(tempfile.mkdtemp(prefix="evaluate_sqlpp_bq_"))
        auto_temp = True

    try:
        evaluate_spider2sql_sqlpp_bigquery(args, temp_path, cluster)
    finally:
        if auto_temp:
            shutil.rmtree(temp_path, ignore_errors=True)
