"""
Evaluate Spider 2.0-lite SQL++ (N1QL) submissions against Couchbase.
Uses spider2-lite-local.jsonl for questions/db mapping, runs queries on Couchbase,
flattens JSON results to CSV via flatten/flatten.py, then reuses CSV-vs-CSV comparison.
"""
import argparse
import json
import logging
import math
import os
import re
import shutil
import sys
import tempfile
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
import pandas as pd
from tqdm import tqdm
# Couchbase SDK
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator

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


# ---------------------------------------------------------------------------
# Structured logging setup
# ---------------------------------------------------------------------------
_LOG_JSONL_PATH = _SCRIPT_DIR / "log_sqlpp.jsonl"

logger = logging.getLogger("sqlpp_eval")


class _JsonlFormatter(logging.Formatter):
    """Emit one JSON object per log line with structured extras."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "event": getattr(record, "event", None),
            "message": record.getMessage(),
        }
        # Merge any structured extras passed via `extra={"extra_data": {...}}`
        extra_data = getattr(record, "extra_data", None)
        if extra_data and isinstance(extra_data, dict):
            entry.update(extra_data)
        return json.dumps(entry, default=str)


def _setup_logging(level: int = logging.INFO) -> None:
    """Configure console + JSONL file handlers for the sqlpp_eval logger."""
    logger.setLevel(level)
    logger.handlers.clear()

    # Console: human-readable
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    console_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    console_handler.setFormatter(console_fmt)
    logger.addHandler(console_handler)

    # File: structured JSONL
    file_handler = logging.FileHandler(str(_LOG_JSONL_PATH), mode="w", encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(_JsonlFormatter())
    logger.addHandler(file_handler)


def _log(level: int, message: str, *, event: str = None, **extras) -> None:
    """Helper: log *message* at *level* with optional structured extras."""
    logger.log(level, message, extra={"event": event, "extra_data": extras if extras else None})


def _diff_summary(pred_pd: pd.DataFrame, gold_pd: pd.DataFrame, max_sample_rows: int = 5) -> dict:
    """Return a structured diff dict between predicted and gold DataFrames."""
    diff = {}
    pred_cols = set(pred_pd.columns)
    gold_cols = set(gold_pd.columns)
    diff["pred_only_cols"] = sorted(pred_cols - gold_cols)
    diff["gold_only_cols"] = sorted(gold_cols - pred_cols)
    diff["common_cols"] = sorted(pred_cols & gold_cols)
    diff["pred_shape"] = list(pred_pd.shape)
    diff["gold_shape"] = list(gold_pd.shape)

    # Head rows for quick visual inspection (as list-of-dicts)
    diff["pred_head"] = pred_pd.head(max_sample_rows).fillna("NaN").astype(str).to_dict(orient="records")
    diff["gold_head"] = gold_pd.head(max_sample_rows).fillna("NaN").astype(str).to_dict(orient="records")

    # For common columns with same row count, show first few mismatched rows
    if diff["common_cols"] and pred_pd.shape[0] == gold_pd.shape[0] and pred_pd.shape[0] > 0:
        mismatches = []
        for col in diff["common_cols"]:
            p = pred_pd[col].reset_index(drop=True)
            g = gold_pd[col].reset_index(drop=True)
            for i in range(min(len(p), len(g))):
                pv, gv = p.iloc[i], g.iloc[i]
                if pd.isna(pv) and pd.isna(gv):
                    continue
                if pv != gv:
                    mismatches.append({"row": i, "col": col, "pred": str(pv), "gold": str(gv)})
                if len(mismatches) >= max_sample_rows:
                    break
            if len(mismatches) >= max_sample_rows:
                break
        diff["value_mismatches"] = mismatches

    return diff


# Initialise with defaults; reconfigured in __main__ after CLI parsing.
_setup_logging()


@lru_cache(maxsize=None)
def load_gold_csv(file_path: str) -> pd.DataFrame:
    """Cache gold CSV loads to avoid repeated disk reads during evaluation."""
    return pd.read_csv(file_path)


def load_jsonl_to_dict(jsonl_file: str) -> dict:
    data_dict = {}
    with open(jsonl_file, "r") as file:
        for line in file:
            item = json.loads(line.strip())
            instance_id = item["instance_id"]
            data_dict[instance_id] = item
    return data_dict


def compare_multi_pandas_table(pred: pd.DataFrame, multi_gold, multi_condition_cols=None, multi_ignore_order=False) -> int:
    if not multi_gold:
        return 0

    if multi_condition_cols in (None, [], [[]], [None]):
        multi_condition_cols = [[] for _ in range(len(multi_gold))]
    elif len(multi_gold) > 1 and not all(isinstance(sublist, list) for sublist in multi_condition_cols):
        multi_condition_cols = [multi_condition_cols for _ in range(len(multi_gold))]

    multi_ignore_order = [multi_ignore_order for _ in range(len(multi_gold))]

    for i, gold in enumerate(multi_gold):
        if compare_pandas_table(pred, gold, multi_condition_cols[i], multi_ignore_order[i]):
            return 1
    return 0


def compare_pandas_table(pred: pd.DataFrame, gold: pd.DataFrame, condition_cols=None, ignore_order: bool = False) -> int:
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
        if not any(vectors_match(gold_vector, pred_vector, ignore_order_=ignore_order) for pred_vector in t_pred_list):
            score = 0
            break

    return score


def db_to_bucket_scope(db_name: str):
    """Match sqlite_loader: bucket and scope from database name."""
    bucket_name = db_name.lower().replace(" ", "_").replace("-", "_")
    scope_name = f"{bucket_name}_scope"
    return bucket_name, scope_name


def db_to_bucket_scope_sf(db_name: str):
    """
    Match load_snowflake_to_couchbase.py bucket naming for Snowflake-sourced instances.
    Bucket = db_name lowercased with underscores removed.
    e.g. PATENTS -> patents, GITHUB_REPOS -> githubrepos
    Queries generated for sf* instances use fully-qualified keyspace paths so the
    query_context is a fallback only.
    """
    bucket_name = db_name.lower().replace("_", "").replace(" ", "")
    scope_name = "_default"
    return bucket_name, scope_name


def get_couchbase_sqlpp_result(
    cluster,
    sqlpp_query: str,
    bucket_name: str,
    scope_name: str,
    save_dir=None,
    file_name: str = "result.csv",
    instance_id: str = None,
    timeout_seconds: int = 60,
):
    """
    Run SQL++ (N1QL) on Couchbase with query_context = bucket.scope.
    Returns (success, error_message). On success, if save_dir is set, writes JSON then
    flattened CSV to save_dir/file_name.
    """
    t0 = time.monotonic()

    try:
        query_context = f"`{bucket_name}`.{scope_name}"
        opts = QueryOptions(
            query_context=query_context,
            timeout=timeout_seconds,
        )
        result = cluster.query(sqlpp_query, opts)
        rows = list(result.rows())
    except Exception as e:
        duration_s = round(time.monotonic() - t0, 3)
        error_message = str(e)
        _log(logging.ERROR, f"[{instance_id}] Couchbase N1QL error: {error_message}",
             event="query_error", instance_id=instance_id,
             error_message=error_message, bucket=bucket_name, scope=scope_name,
             query=sqlpp_query, duration_s=duration_s)
        return False, error_message, None

    duration_s = round(time.monotonic() - t0, 3)

    if not rows:
        _log(logging.INFO, f"[{instance_id}] Query returned 0 rows",
             event="query_empty", instance_id=instance_id,
             rows_returned=0, duration_s=duration_s, bucket=bucket_name)
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            pd.DataFrame().to_csv(os.path.join(save_dir, file_name), index=False)
        return True, None, []

    # Flatten JSON rows to flat list of dicts (same logic as flatten/flatten.py)
    flat_rows = flatten_process_json(rows)
    if not flat_rows:
        _log(logging.WARNING, f"[{instance_id}] Flatten returned 0 rows from {len(rows)} raw rows",
             event="flatten_empty", instance_id=instance_id,
             raw_rows=len(rows), duration_s=duration_s, bucket=bucket_name)
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            pd.DataFrame().to_csv(os.path.join(save_dir, file_name), index=False)
        return True, None, []

    _log(logging.INFO, f"[{instance_id}] Query OK — {len(flat_rows)} rows in {duration_s}s",
         event="query_success", instance_id=instance_id,
         rows_returned=len(flat_rows), duration_s=duration_s, bucket=bucket_name)

    if save_dir:
        os.makedirs(save_dir, exist_ok=True)
        df = pd.DataFrame(flat_rows)
        df.to_csv(os.path.join(save_dir, file_name), index=False)

    return True, None, flat_rows


def extract_sql_query(pred_sql_query: str) -> str:
    # Support ```sql and ```sqlpp / ```n1ql
    for pattern in (r"```sql\n(.*?)\n```", r"```sqlpp\n(.*?)\n```", r"```n1ql\n(.*?)\n```"):
        match = re.search(pattern, pred_sql_query, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return pred_sql_query


def resolve_gold_paths(instance_id: str, gold_result_dir: str):
    base_path = Path(gold_result_dir) / f"{instance_id}.csv"
    if base_path.exists():
        return [base_path], True

    if "_" in instance_id:
        pattern = re.compile(rf"^{re.escape(instance_id)}(_[a-z])?\.csv$")
    else:
        pattern = re.compile(rf"^{re.escape(instance_id)}(_[a-z])?\.csv$")

    csv_files = sorted(
        file for file in os.listdir(gold_result_dir)
        if pattern.match(file)
    )
    return [Path(gold_result_dir) / file for file in csv_files], False


def evaluate_single_sql_instance(
    instance_id: str,
    eval_standard_dict: dict,
    spider2_local_metadata: dict,
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

        metadata = spider2_local_metadata.get(instance_id, {})
        db_name = metadata.get("db")
        if not db_name:
            score = 0
            error_info = f"Missing database mapping for {instance_id} in spider2-lite-local.jsonl"
            _log(logging.WARNING, error_info, event="missing_db", instance_id=instance_id)
            return {
                "instance_id": instance_id,
                "score": score,
                "pred_sql": pred_sql_query,
                "error_info": error_info,
            }

        # Snowflake-sourced instances use a different bucket naming convention
        if instance_id.startswith("sf"):
            bucket_name, scope_name = db_to_bucket_scope_sf(db_name)
        else:
            bucket_name, scope_name = db_to_bucket_scope(db_name)

        result_file = f"{instance_id}.csv"

        exe_flag, dbms_error_info, flat_rows = get_couchbase_sqlpp_result(
            cluster,
            pred_sql_query,
            bucket_name,
            scope_name,
            save_dir=result_csv_dir,
            file_name=result_file,
            instance_id=instance_id,
            timeout_seconds=timeout,
        )

        if not exe_flag:
            score = 0
            error_info = dbms_error_info
        else:
            pred_pd = pd.DataFrame(flat_rows) if flat_rows else pd.DataFrame()

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
                    _log(logging.ERROR, f"{instance_id}: compare against {gold_paths[0]} failed: {e}",
                         event="compare_error", instance_id=instance_id, gold_path=str(gold_paths[0]))
                    score = 0
                    error_info = f"Python Script Error:{str(e)}"
                if score == 0 and error_info is None:
                    error_info = "Result Error"
                    diff = _diff_summary(pred_pd, gold_pd)
                    _log(logging.WARNING,
                         f"[{instance_id}] Result mismatch: pred({pred_pd.shape[0]}r x {pred_pd.shape[1]}c) vs gold({gold_pd.shape[0]}r x {gold_pd.shape[1]}c)",
                         event="result_mismatch", instance_id=instance_id,
                         pred_rows=pred_pd.shape[0], pred_cols=pred_pd.shape[1],
                         gold_rows=gold_pd.shape[0], gold_cols=gold_pd.shape[1],
                         pred_columns=list(pred_pd.columns),
                         gold_columns=list(gold_pd.columns),
                         diff=diff,
                         query=pred_sql_query,
                         db=db_name)
            else:
                try:
                    gold_pds = [load_gold_csv(str(path)) for path in gold_paths]
                    score = compare_multi_pandas_table(pred_pd, gold_pds, condition_cols, ignore_order)
                except Exception as e:
                    _log(logging.ERROR, f"{instance_id}: multi-compare against {gold_paths} failed: {e}",
                         event="compare_error", instance_id=instance_id,
                         gold_paths=[str(p) for p in gold_paths])
                    score = 0
                    error_info = f"Python Script Error:{str(e)}"
                if score == 0 and error_info is None:
                    error_info = "Result Error"
                    gold_shapes = [(gp.shape[0], gp.shape[1]) for gp in gold_pds]
                    # Diff against the first gold file for a representative comparison
                    diff = _diff_summary(pred_pd, gold_pds[0])
                    _log(logging.WARNING,
                         f"[{instance_id}] Result mismatch (multi-gold): pred({pred_pd.shape[0]}r x {pred_pd.shape[1]}c) vs {len(gold_pds)} gold files {gold_shapes}",
                         event="result_mismatch", instance_id=instance_id,
                         pred_rows=pred_pd.shape[0], pred_cols=pred_pd.shape[1],
                         gold_shapes=gold_shapes,
                         pred_columns=list(pred_pd.columns),
                         diff=diff,
                         query=pred_sql_query,
                         db=db_name)

    except Exception as e:
        _log(logging.ERROR, f"Error evaluating {instance_id}: {e}",
             event="evaluation_error", instance_id=instance_id)
        score = 0
        error_info = f"Evaluation Error: {str(e)}"
        pred_sql_query = ""

    # Log final result for this instance
    if score == 1:
        _log(logging.INFO, f"[{instance_id}] PASS",
             event="result", instance_id=instance_id, score=1)
    else:
        _log(logging.WARNING, f"[{instance_id}] FAIL — {error_info}",
             event="result", instance_id=instance_id, score=0, error_info=error_info)

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
                _log(logging.ERROR, f"{instance_id}: compare against {gold_paths[0]} failed: {e}",
                     event="compare_error", instance_id=instance_id, gold_path=str(gold_paths[0]))
                score = 0
                error_info = f"Python Script Error:{str(e)}"
            if score == 0 and error_info is None:
                error_info = "Result Error"
                diff = _diff_summary(pred_pd, gold_pd)
                _log(logging.WARNING,
                     f"[{instance_id}] Result mismatch: pred({pred_pd.shape[0]}r x {pred_pd.shape[1]}c) vs gold({gold_pd.shape[0]}r x {gold_pd.shape[1]}c)",
                     event="result_mismatch", instance_id=instance_id,
                     pred_rows=pred_pd.shape[0], pred_cols=pred_pd.shape[1],
                     gold_rows=gold_pd.shape[0], gold_cols=gold_pd.shape[1],
                     pred_columns=list(pred_pd.columns),
                     gold_columns=list(gold_pd.columns),
                     diff=diff)
        else:
            try:
                gold_pds = [load_gold_csv(str(path)) for path in gold_paths]
                score = compare_multi_pandas_table(pred_pd, gold_pds, condition_cols, ignore_order)
            except Exception as e:
                _log(logging.ERROR, f"{instance_id}: multi-compare against {gold_paths} failed: {e}",
                     event="compare_error", instance_id=instance_id,
                     gold_paths=[str(p) for p in gold_paths])
                score = 0
                error_info = f"Python Script Error:{str(e)}"
            if score == 0 and error_info is None:
                error_info = "Result Error"
                gold_shapes = [(gp.shape[0], gp.shape[1]) for gp in gold_pds]
                diff = _diff_summary(pred_pd, gold_pds[0])
                _log(logging.WARNING,
                     f"[{instance_id}] Result mismatch (multi-gold): pred({pred_pd.shape[0]}r x {pred_pd.shape[1]}c) vs {len(gold_pds)} gold files {gold_shapes}",
                     event="result_mismatch", instance_id=instance_id,
                     pred_rows=pred_pd.shape[0], pred_cols=pred_pd.shape[1],
                     gold_shapes=gold_shapes,
                     pred_columns=list(pred_pd.columns),
                     diff=diff)

    except Exception as e:
        _log(logging.ERROR, f"Error evaluating {instance_id}: {e}",
             event="evaluation_error", instance_id=instance_id)
        score = 0
        error_info = f"Evaluation Error: {str(e)}"

    if score == 1:
        _log(logging.INFO, f"[{instance_id}] PASS",
             event="result", instance_id=instance_id, score=1)
    else:
        _log(logging.WARNING, f"[{instance_id}] FAIL — {error_info}",
             event="result", instance_id=instance_id, score=0, error_info=error_info)

    return {
        "instance_id": instance_id,
        "score": score,
        "pred_sql": None,
        "error_info": error_info,
    }


def save_correct_ids_to_csv(output_results, result_dir: str):
    correct_ids = [item["instance_id"] for item in output_results if item["score"] == 1]

    transformed_ids = []
    for item in correct_ids:
        if item.startswith(("bq", "ga", "local")):
            transformed_ids.append(f"{item}")
        else:
            transformed_ids.append(item)

    csv_file = f"{result_dir}-ids.csv"
    pd.DataFrame({"instance_id": transformed_ids}).to_csv(csv_file, index=False)
    _log(logging.INFO, f"Correct IDs saved to: {csv_file}",
         event="ids_saved", csv_path=csv_file)
    return csv_file


def evaluate_spider2sql_sqlpp(args, temp_dir: Path, cluster):
    mode = args.mode
    gold_result_dir = os.path.join(args.gold_dir, "exec_result")
    pred_result_dir = args.result_dir

    eval_standard_dict = load_jsonl_to_dict(os.path.join(args.gold_dir, "spider2lite_eval.jsonl"))

    root_dir = Path(__file__).resolve().parent.parent  # spider2-lite/
    # Questions / db mapping from spider2-lite-local.jsonl (Couchbase-loaded SQLite datasets)
    spider2_local_jsonl = root_dir / "evaluation_pipeline/NL_questions.jsonl"
    if not spider2_local_jsonl.exists():
        _log(logging.CRITICAL, f"{spider2_local_jsonl} not found. Required for Couchbase evaluation.",
             event="missing_jsonl", path=str(spider2_local_jsonl))
        return []
    spider2_local_metadata = load_jsonl_to_dict(str(spider2_local_jsonl))

    result_csv_dir = None
    if mode == "sql":
        result_csv_path = Path(f"{pred_result_dir}_csv")
        if result_csv_path.exists():
            shutil.rmtree(result_csv_path)
        result_csv_path.mkdir(parents=True, exist_ok=True)
        result_csv_dir = str(result_csv_path)

    pred_ids = []
    if mode == "sql":
        pred_ids = [Path(file).stem for file in os.listdir(pred_result_dir) if file.endswith(".sqlpp")]
    elif mode == "exec_result":
        pred_ids = [Path(file).stem for file in os.listdir(pred_result_dir) if file.endswith(".csv")]

    gold_ids = list(eval_standard_dict.keys())
    # Only evaluate instance_ids that are in spider2-lite-local.jsonl (Couchbase local DBs)
    local_ids = set(spider2_local_metadata.keys())
    total_local_questions = len(set(gold_ids).intersection(local_ids))  # 135 local questions with gold
    eval_ids = sorted(set(gold_ids).intersection(pred_ids).intersection(local_ids))

    if not eval_ids:
        _log(logging.WARNING, "No overlapping prediction IDs with gold set and spider2-lite-local.jsonl. Nothing to evaluate.",
             event="no_eval_ids")
        return []

    _log(logging.INFO,
         f"Evaluating {len(eval_ids)} prediction(s) (local Couchbase subset; {total_local_questions} total local questions in gold).",
         event="eval_start", eval_count=len(eval_ids), total_local=total_local_questions)

    max_workers = getattr(args, "max_workers", 8)
    max_workers = min(max_workers, len(eval_ids)) or 1

    output_results = []

    if mode == "sql":
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id = {
                executor.submit(
                    evaluate_single_sql_instance,
                    instance_id,
                    eval_standard_dict,
                    spider2_local_metadata,
                    pred_result_dir,
                    gold_result_dir,
                    temp_dir=temp_dir,
                    result_csv_dir=result_csv_dir,
                    timeout=getattr(args, "timeout", 60),
                    cluster=cluster,
                ): instance_id
                for instance_id in eval_ids
            }

            for future in tqdm(as_completed(future_to_id), total=len(eval_ids), desc="Evaluating SQL++"):
                output_results.append(future.result())

    elif mode == "exec_result":
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id = {
                executor.submit(
                    evaluate_single_exec_result_instance,
                    instance_id,
                    eval_standard_dict,
                    pred_result_dir,
                    gold_result_dir,
                ): instance_id
                for instance_id in eval_ids
            }

            for future in tqdm(as_completed(future_to_id), total=len(eval_ids), desc="Evaluating Exec Results"):
                output_results.append(future.result())

    output_results.sort(key=lambda item: item["instance_id"])

    score_map = {item["instance_id"]: item["score"] for item in output_results}
    correct_examples = sum(item["score"] for item in output_results)
    final_score = correct_examples / len(output_results)
    real_score = correct_examples / total_local_questions

    _log(logging.INFO, f"Score map: {score_map}", event="score_map", scores=score_map)
    _log(logging.INFO,
         f"Final score: {final_score}, Correct: {correct_examples}, Evaluated: {len(output_results)}",
         event="final_score", score=final_score, correct=correct_examples,
         total_evaluated=len(output_results))
    _log(logging.INFO,
         f"Real score (local): {real_score}, Correct: {correct_examples}, Total local: {total_local_questions}",
         event="real_score", score=real_score, correct=correct_examples,
         total_local=total_local_questions)

    if mode == "sql" and result_csv_dir:
        _log(logging.INFO, f"Execution results saved to: {result_csv_dir}",
             event="csv_saved", result_csv_dir=result_csv_dir)

    save_correct_ids_to_csv(output_results, pred_result_dir)

    return output_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate Spider 2.0-lite SQL++ (N1QL) on Couchbase.")
    parser.add_argument("--mode", type=str, choices=["sql", "exec_result"], default="sql", help="Mode of submission results")
    parser.add_argument("--result_dir", type=str, default="baselines/promptSQL++/submission_test", help="Result directory (contains .sql files)")
    parser.add_argument("--gold_dir", type=str, default="evaluation_pipeline/gold", help="Gold directory (spider2lite_eval.jsonl + exec_result/*.csv)")
    parser.add_argument("--max_workers", type=int, default=20, help="Maximum number of worker threads")
    parser.add_argument("--timeout", type=int, default=1200, help="N1QL execution timeout in seconds")
    parser.add_argument("--temp_dir", type=str, default=None, help="Optional working directory for temporary files.")
    parser.add_argument("--cb_host", type=str, default="couchbase://127.0.0.1", help="Couchbase connection string (default: COUCHBASE_HOST or couchbase://127.0.0.1)")
    parser.add_argument("--cb_user", type=str, default="Administrator", help="Couchbase user (default: COUCHBASE_USER or Administrator)")
    parser.add_argument("--cb_pass", type=str, default="password", help="Couchbase password (default: COUCHBASE_PASS or password)")
    parser.add_argument("--log_level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging verbosity (default: INFO)")
    args = parser.parse_args()

    # Reconfigure logging with user-chosen level
    _setup_logging(getattr(logging, args.log_level))

    if args.cb_host is not None:
        CB_HOST = args.cb_host
    if args.cb_user is not None:
        CB_USER = args.cb_user
    if args.cb_pass is not None:
        CB_PASS = args.cb_pass

    _log(logging.INFO, f"Connecting to Couchbase at {CB_HOST}...", event="cb_connecting", host=CB_HOST)
    try:
        auth = PasswordAuthenticator(CB_USER, CB_PASS)
        cluster = Cluster(CB_HOST, ClusterOptions(auth))
        from datetime import timedelta
        cluster.wait_until_ready(timedelta(seconds=10))
        _log(logging.INFO, "Couchbase connected.", event="cb_connected", host=CB_HOST)
    except Exception as e:
        _log(logging.CRITICAL, f"Couchbase connection failed: {e}", event="cb_connection_failed", host=CB_HOST)
        sys.exit(1)

    auto_temp = False
    if args.temp_dir:
        temp_path = Path(args.temp_dir).expanduser().resolve()
        if temp_path.exists():
            shutil.rmtree(temp_path)
        temp_path.mkdir(parents=True, exist_ok=True)
    else:
        temp_path = Path(tempfile.mkdtemp(prefix="evaluate_sqlpp_"))
        auto_temp = True

    try:
        evaluate_spider2sql_sqlpp(args, temp_path, cluster)
    finally:
        if auto_temp:
            shutil.rmtree(temp_path, ignore_errors=True)
