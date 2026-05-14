"""Analyze log_sqlpp.jsonl and produce a structured error report.

Reads the structured JSONL log produced by evaluate_sqlpp_sqlite.py and prints
a comprehensive analysis covering error categories, root causes, per-database
breakdowns, slowest queries, and a per-instance error table.
"""

import builtins
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
LOG_FILE = _SCRIPT_DIR / "log_sqlpp.jsonl"
OUTPUT_FILE = _SCRIPT_DIR / "analysis_report.txt"

# Tee print to both stdout and output file
_output_fh = open(OUTPUT_FILE, "w", encoding="utf-8")
_original_print = builtins.print

def _tee_print(*args, **kwargs):
    _original_print(*args, **kwargs)
    kwargs["file"] = _output_fh
    _original_print(*args, **kwargs)

builtins.print = _tee_print

# ---------------------------------------------------------------------------
# 1. Load all log entries
# ---------------------------------------------------------------------------
entries: list[dict] = []
with open(LOG_FILE, "r") as f:
    for line in f:
        line = line.strip()
        if line:
            entries.append(json.loads(line))

# ---------------------------------------------------------------------------
# 2. Classify entries by event type
# ---------------------------------------------------------------------------
query_errors: list[dict] = []        # event == query_error
results: list[dict] = []             # event == result
query_successes: list[dict] = []     # event == query_success
eval_errors: list[dict] = []         # event == evaluation_error
compare_errors: list[dict] = []      # event == compare_error
result_mismatches: list[dict] = []   # event == result_mismatch
final_score_entry: dict | None = None
real_score_entry: dict | None = None

for entry in entries:
    event = entry.get("event")
    if event == "query_error":
        query_errors.append(entry)
    elif event == "result":
        results.append(entry)
    elif event == "query_success":
        query_successes.append(entry)
    elif event == "evaluation_error":
        eval_errors.append(entry)
    elif event == "compare_error":
        compare_errors.append(entry)
    elif event == "result_mismatch":
        result_mismatches.append(entry)
    elif event == "final_score":
        final_score_entry = entry
    elif event == "real_score":
        real_score_entry = entry

# ---------------------------------------------------------------------------
# 3. Parse N1QL error codes from error_message field
# ---------------------------------------------------------------------------

def parse_error_code(error_message: str) -> tuple[int | None, str]:
    """Extract first_error_code and first_error_message from the N1QL error string."""
    code_match = re.search(r"first_error_code':\s*(\d+)", error_message)
    msg_match = re.search(r"first_error_message':\s*'(.*?)'", error_message, re.DOTALL)
    code = int(code_match.group(1)) if code_match else None
    msg = msg_match.group(1) if msg_match else error_message
    return code, msg


errors_parsed: list[dict] = []
for qe in query_errors:
    error_message = qe.get("error_message") or qe.get("message") or ""
    code, msg = parse_error_code(error_message)
    errors_parsed.append({
        "instance_id": qe.get("instance_id", "unknown"),
        "error_code": code,
        "error_message": msg,
        "bucket": qe.get("bucket"),
        "duration_s": qe.get("duration_s"),
    })

# ---------------------------------------------------------------------------
# 4. Categorise errors (same logic as original analyze_log.py)
# ---------------------------------------------------------------------------
categories: dict[str, list[str]] = defaultdict(list)

for err in errors_parsed:
    iid = err["instance_id"]
    code = err["error_code"]
    msg = err["error_message"]

    if code == 3000:
        if "CROSS" in msg:
            categories["CROSS JOIN not supported"].append(iid)
        elif "Subquery in FROM clause must have an alias" in msg:
            categories["Missing subquery alias"].append(iid)
        elif "CAST (reserved word)" in msg:
            categories["CAST syntax error (reserved word)"].append(iid)
        elif "ORDER (reserved word)" in msg and "ARRAY_AGG" in msg:
            categories["ARRAY_AGG ORDER BY syntax"].append(iid)
        elif "ORDER (reserved word)" in msg:
            categories["ORDER in aggregate not supported"].append(iid)
        elif "Invalid function" in msg:
            func_name = re.search(r"Invalid function (\w+)", msg)
            fn = func_name.group(1) if func_name else "unknown"
            categories[f"Invalid function: {fn}"].append(iid)
        elif "UNION (reserved word)" in msg:
            categories["UNION after ORDER BY/LIMIT"].append(iid)
        elif "FOR (reserved word)" in msg:
            categories["FOR comprehension syntax error"].append(iid)
        elif "Groups (reserved word)" in msg or "GROUPS" in msg.upper():
            categories["Reserved word used as identifier"].append(iid)
        elif "RIGHT (reserved word)" in msg:
            categories["RIGHT() function not supported"].append(iid)
        elif "Number of arguments to function" in msg:
            categories["Wrong number of function arguments"].append(iid)
        elif "LET (reserved word)" in msg:
            categories["LET placement error"].append(iid)
        elif "IN ('" in msg or "at: ," in msg:
            categories["IN with parentheses instead of array"].append(iid)
        elif "at: ." in msg:
            categories["LET assignment syntax error"].append(iid)
        else:
            categories["Other syntax error (3000)"].append(iid)
    elif code == 3080:
        categories["Ambiguous field reference (alias needed)"].append(iid)
    elif code == 4210:
        categories["Non-aggregate in GROUP BY context"].append(iid)
    elif code == 4000:
        categories["Window function inside aggregate"].append(iid)
    elif code == 5600:
        categories["Out of memory"].append(iid)
    elif code == 5420:
        categories["Missing secondary index for join"].append(iid)
    elif code is None:
        categories["Unparseable error"].append(iid)
    else:
        categories[f"Other error ({code})"].append(iid)

for ee in eval_errors:
    categories["Evaluation / parse error"].append(ee.get("instance_id", "unknown"))
for ce in compare_errors:
    categories["Comparison script error"].append(ce.get("instance_id", "unknown"))

# ---------------------------------------------------------------------------
# 5. Print report
# ---------------------------------------------------------------------------

SEP = "=" * 70

print(SEP)
print("SQL++ EVALUATION LOG ANALYSIS REPORT")
print(SEP)

# -- Score summary --
if final_score_entry:
    score = final_score_entry.get("score", 0)
    correct = final_score_entry.get("correct", 0)
    total_eval = final_score_entry.get("total_evaluated", 0)
    print(f"\nFinal Score: {score:.4f} ({correct}/{total_eval} correct)")
    print(f"Error Rate: {total_eval - correct}/{total_eval} = {(total_eval - correct) / max(total_eval, 1) * 100:.1f}%")

if real_score_entry:
    total_local = real_score_entry.get("total_local", 0)
    print(f"Total local questions: {total_local}")

total_errors = len(query_errors) + len(eval_errors) + len(compare_errors) + len(result_mismatches)
print(f"\nTotal errors captured: {total_errors}")
print(f"  - N1QL query errors: {len(query_errors)}")
print(f"  - Wrong output (result mismatch): {len(result_mismatches)}")
print(f"  - Evaluation errors: {len(eval_errors)}")
print(f"  - Comparison errors: {len(compare_errors)}")

# -- Error categories --
print(f"\n{SEP}")
print("ERROR CATEGORIES (sorted by frequency)")
print(SEP)

sorted_cats = sorted(categories.items(), key=lambda x: -len(x[1]))
for cat, ids in sorted_cats:
    print(f"\n[{len(ids)}] {cat}")
    print(f"    Instances: {', '.join(sorted(set(ids)))}")

# -- Root cause analysis --
print(f"\n{SEP}")
print("ROOT CAUSE ANALYSIS")
print(SEP)

root_causes: dict[str, list[str]] = {
    "SQL dialect mismatch (SQL vs SQL++)": [],
    "Missing/unsupported functions": [],
    "Couchbase syntax restrictions": [],
    "Resource constraints": [],
    "Other": [],
}

for cat, ids in sorted_cats:
    if any(k in cat for k in ["CROSS JOIN", "CAST", "RIGHT()", "UNION after", "IN with parentheses", "Reserved word"]):
        root_causes["SQL dialect mismatch (SQL vs SQL++)"].extend(ids)
    elif "Invalid function" in cat or "Wrong number" in cat:
        root_causes["Missing/unsupported functions"].extend(ids)
    elif any(k in cat for k in ["Ambiguous", "Non-aggregate", "Window function", "Missing subquery", "LET", "ORDER in aggregate", "ARRAY_AGG"]):
        root_causes["Couchbase syntax restrictions"].extend(ids)
    elif any(k in cat for k in ["memory", "index", "Evaluation", "Comparison"]):
        root_causes["Resource constraints"].extend(ids)
    else:
        root_causes["Other"].extend(ids)

for cause, ids in root_causes.items():
    if ids:
        unique = sorted(set(ids))
        print(f"\n{cause}: {unique_count} instances" if (unique_count := len(unique)) else "")

# -- Errors by database --
print(f"\n{SEP}")
print("ERRORS BY DATABASE")
print(SEP)

db_errors: dict[str, list[str]] = defaultdict(list)
for qe in query_errors:
    bucket = qe.get("bucket", "unknown")
    db_errors[bucket].append(qe.get("instance_id", "unknown"))

for db in sorted(db_errors.keys()):
    unique_ids = sorted(set(db_errors[db]))
    print(f"  {db}: {len(unique_ids)} errors")

# -- Slowest queries (top 10) --
print(f"\n{SEP}")
print("TOP 10 SLOWEST QUERIES")
print(SEP)

timed_entries = [
    e for e in entries
    if e.get("duration_s") is not None and e.get("instance_id")
]
timed_entries.sort(key=lambda e: e.get("duration_s", 0), reverse=True)

print(f"\n{'Instance ID':<15} {'Duration (s)':<14} {'Rows':<8} {'Event'}")
print("-" * 55)
for e in timed_entries[:10]:
    iid = e.get("instance_id", "?")
    dur = e.get("duration_s", "?")
    rows = e.get("rows_returned", "?")
    event = e.get("event", "?")
    print(f"{iid:<15} {dur:<14} {rows:<8} {event}")

# -- Pass / fail by database --
print(f"\n{SEP}")
print("PASS RATE BY DATABASE")
print(SEP)

db_results: dict[str, dict[str, int]] = defaultdict(lambda: {"pass": 0, "fail": 0})
# Map instance_id → bucket from query events
iid_bucket: dict[str, str] = {}
for e in entries:
    if e.get("bucket") and e.get("instance_id"):
        iid_bucket[e["instance_id"]] = e["bucket"]

for r in results:
    iid = r.get("instance_id", "unknown")
    bucket = iid_bucket.get(iid, "unknown")
    if r.get("score", 0) == 1:
        db_results[bucket]["pass"] += 1
    else:
        db_results[bucket]["fail"] += 1

print(f"\n{'Database':<25} {'Pass':<8} {'Fail':<8} {'Rate'}")
print("-" * 50)
for db in sorted(db_results.keys()):
    p = db_results[db]["pass"]
    f = db_results[db]["fail"]
    total = p + f
    rate = f"{p / total * 100:.1f}%" if total else "N/A"
    print(f"{db:<25} {p:<8} {f:<8} {rate}")

# -- Wrong output analysis --
print(f"\n{SEP}")
print("WRONG OUTPUT ANALYSIS (queries that ran but produced incorrect results)")
print(SEP)

if result_mismatches:
    print(f"\nTotal result mismatches: {len(result_mismatches)}")

    # Shape mismatches vs value-only mismatches
    shape_mismatches = []
    value_mismatches = []
    for rm in result_mismatches:
        pr, pc = rm.get("pred_rows", "?"), rm.get("pred_cols", "?")
        gr, gc = rm.get("gold_rows", "?"), rm.get("gold_cols", "?")
        if pr != gr or pc != gc:
            shape_mismatches.append(rm)
        else:
            value_mismatches.append(rm)

    print(f"  - Shape mismatch (different rows/cols): {len(shape_mismatches)}")
    print(f"  - Value mismatch (same shape, wrong values): {len(value_mismatches)}")

    # Per-instance detail
    print(f"\n{'Instance ID':<15} {'DB':<20} {'Pred Shape':<14} {'Gold Shape':<14} {'Issue'}")
    print("-" * 75)
    for rm in sorted(result_mismatches, key=lambda x: x.get("instance_id", "")):
        iid = rm.get("instance_id", "?")
        db = rm.get("db", "?")
        pr, pc = rm.get("pred_rows", "?"), rm.get("pred_cols", "?")
        gr, gc = rm.get("gold_rows", "?"), rm.get("gold_cols", "?")
        gold_shapes = rm.get("gold_shapes")  # multi-gold

        pred_shape = f"{pr}x{pc}"
        if gold_shapes:
            gold_shape = " | ".join(f"{r}x{c}" for r, c in gold_shapes)
        else:
            gold_shape = f"{gr}x{gc}"

        if pr != gr or pc != gc:
            issue = "SHAPE"
        else:
            issue = "VALUES"

        # Check column name mismatch
        pred_cols_list = rm.get("pred_columns", [])
        gold_cols_list = rm.get("gold_columns", [])
        if pred_cols_list and gold_cols_list and set(pred_cols_list) != set(gold_cols_list):
            issue += " + COLUMNS"

        print(f"{iid:<15} {db:<20} {pred_shape:<14} {gold_shape:<14} {issue}")

    # Wrong output by database
    print(f"\n  Wrong output by database:")
    db_mismatch: dict[str, int] = defaultdict(int)
    for rm in result_mismatches:
        db_mismatch[rm.get("db") or iid_bucket.get(rm.get("instance_id", ""), "unknown")] += 1
    for db in sorted(db_mismatch.keys()):
        print(f"    {db}: {db_mismatch[db]}")

    # -----------------------------------------------------------------------
    # Detailed diff viewer for each mismatch
    # -----------------------------------------------------------------------
    print(f"\n{SEP}")
    print("DETAILED DIFF FOR EACH MISMATCH")
    print(SEP)

    def _fmt_table(rows: list[dict], max_col_width: int = 30) -> str:
        """Format a list-of-dicts as an ASCII table."""
        if not rows:
            return "  (empty)"
        cols = list(rows[0].keys())
        # Truncate wide values
        def trunc(v: str) -> str:
            return v if len(v) <= max_col_width else v[:max_col_width - 3] + "..."
        widths = {c: max(len(c), *(len(trunc(str(r.get(c, "")))) for r in rows)) for c in cols}
        header = "  " + " | ".join(f"{c:<{widths[c]}}" for c in cols)
        sep_line = "  " + "-+-".join("-" * widths[c] for c in cols)
        lines = [header, sep_line]
        for r in rows:
            lines.append("  " + " | ".join(f"{trunc(str(r.get(c, ''))):<{widths[c]}}" for c in cols))
        return "\n".join(lines)

    for rm in sorted(result_mismatches, key=lambda x: x.get("instance_id", "")):
        iid = rm.get("instance_id", "?")
        db = rm.get("db", "?")
        diff = rm.get("diff")
        query = rm.get("query")

        print(f"\n{'─' * 70}")
        print(f"  {iid}  (db: {db})")
        print(f"{'─' * 70}")

        if query:
            # Show query (truncate if very long)
            q = query.strip()
            if len(q) > 500:
                q = q[:500] + "\n    ... (truncated)"
            print(f"\n  SQL++ Query:")
            for qline in q.split("\n"):
                print(f"    {qline}")

        if not diff:
            print("  (no diff data available — run was before enhanced logging)")
            continue

        # Column differences
        pred_only = diff.get("pred_only_cols", [])
        gold_only = diff.get("gold_only_cols", [])
        if pred_only or gold_only:
            print(f"\n  Column differences:")
            if pred_only:
                print(f"    Pred-only columns: {pred_only}")
            if gold_only:
                print(f"    Gold-only columns: {gold_only}")
        common = diff.get("common_cols", [])
        if common:
            print(f"    Common columns:    {common}")

        # Pred head
        pred_head = diff.get("pred_head", [])
        if pred_head:
            print(f"\n  Pred head ({len(pred_head)} rows):")
            print(_fmt_table(pred_head))

        # Gold head
        gold_head = diff.get("gold_head", [])
        if gold_head:
            print(f"\n  Gold head ({len(gold_head)} rows):")
            print(_fmt_table(gold_head))

        # Value mismatches
        val_mm = diff.get("value_mismatches", [])
        if val_mm:
            print(f"\n  Value mismatches (first {len(val_mm)}):")
            print(f"    {'Row':<6} {'Column':<25} {'Pred':<25} {'Gold':<25}")
            print(f"    {'-'*6} {'-'*25} {'-'*25} {'-'*25}")
            for m in val_mm:
                row = str(m.get("row", "?"))
                col = m.get("col", "?")
                pv = str(m.get("pred", "?"))[:25]
                gv = str(m.get("gold", "?"))[:25]
                print(f"    {row:<6} {col:<25} {pv:<25} {gv:<25}")

else:
    print("\n  No result mismatches found — all executed queries matched gold!")

# -- Per-instance error table --
print(f"\n{'=' * 100}")
print(f"{'Instance ID':<15} {'Error Code':<12} {'Error Description'}")
print(f"{'=' * 100}")

all_table_errors: list[tuple[str, str, str]] = []
for err in errors_parsed:
    msg = err["error_message"]
    if len(msg) > 80:
        msg = msg[:77] + "..."
    all_table_errors.append((err["instance_id"], str(err["error_code"] or "N/A"), msg))

for ee in eval_errors:
    all_table_errors.append((ee.get("instance_id", "?"), "N/A", ee.get("message", "")[:80]))

all_table_errors.sort(key=lambda x: x[0])

for iid, code, desc in all_table_errors:
    print(f"{iid:<15} {code:<12} {desc}")

print(f"{'=' * 100}")
print(f"Total: {len(all_table_errors)} errors out of {len(results)} evaluated queries")

# Restore print and close output file
builtins.print = _original_print
_output_fh.close()
print(f"\nReport saved to: {OUTPUT_FILE}")
