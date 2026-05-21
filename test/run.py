from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from anyio import BrokenResourceError
from mcp import ClientSession, StdioServerParameters, stdio_client

logging.getLogger("mcp.os.posix.utilities").setLevel(logging.ERROR)

BASE_DIR = Path(__file__).resolve().parent
TEST_DIR = BASE_DIR.parent
OUTPUT_DIR = BASE_DIR / "output"
QUERIES_DIR = OUTPUT_DIR / "queries"
LOG_FILE = OUTPUT_DIR / "run_log.jsonl"
SERVER_LOG_FILE = Path(os.environ["MCP_SERVER_LOG_FILE"]) if "MCP_SERVER_LOG_FILE" in os.environ else OUTPUT_DIR / "server.log"


def _resolve_server_dir() -> Path:
    env_path = os.environ.get("MCP_SERVER_PATH", "").strip()
    if env_path:
        return Path(env_path)
    # fallback to old hardcoded location
    return Path.home() / "SEMANTIC-CATALOG" / "mcp-server-couchbase"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def load_env_file() -> None:
    env_file = BASE_DIR / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def load_questions(questions_file: Path) -> list[dict[str, Any]]:
    return json.loads(questions_file.read_text())


def build_output_paths(instance_id: str) -> dict[str, Path]:
    return {"query": QUERIES_DIR / f"{instance_id}.sqlpp"}


def build_server_env() -> dict[str, str]:
    server_dir = _resolve_server_dir()
    server_src = server_dir / "src"

    env = os.environ.copy()

    # Remap MCP_CB_* → CB_* so the MCP server sees its expected var names
    mcp_prefix = "MCP_CB_"
    for key, value in list(env.items()):
        if key.startswith(mcp_prefix):
            cb_key = "CB_" + key[len(mcp_prefix):]
            env.setdefault(cb_key, value)

    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        f"{server_src}{os.pathsep}{existing_pythonpath}"
        if existing_pythonpath
        else str(server_src)
    )
    env["CB_MCP_TRANSPORT"] = "stdio"
    return env


def _resolve_server_python() -> Path:
    venv_path = os.environ.get("MCP_SERVER_VENV_PATH", "").strip()
    if venv_path:
        return Path(venv_path) / "bin" / "python"
    # fallback: venv inside the server dir
    return _resolve_server_dir() / ".venv" / "bin" / "python"


def build_server_params() -> StdioServerParameters:
    return StdioServerParameters(
        command=str(_resolve_server_python()),
        args=["-m", "mcp_server"],
        env=build_server_env(),
    )


def append_log(entry: dict[str, Any]) -> None:
    with LOG_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(entry, ensure_ascii=False) + "\n")


async def call_mcp_tool(
    session: ClientSession,
    *,
    tool_name: str,
    tool_args: dict[str, Any],
) -> Any:
    result = await session.call_tool(tool_name, tool_args, read_timeout_seconds=timedelta(seconds=360))
    if getattr(result, "isError", False):
        error_text = " ".join(
            getattr(b, "text", str(b)) for b in (getattr(result, "content", None) or [])
        )
        raise RuntimeError(f"MCP tool error: {error_text or '(no message)'}")
    blocks = getattr(result, "content", None) or []
    if not blocks:
        return None
    if len(blocks) == 1:
        text = getattr(blocks[0], "text", None)
        return text if text is not None else str(blocks[0])
    return [getattr(b, "text", None) or str(b) for b in blocks]


async def generate_query_for_question(
    *,
    session: ClientSession,
    question_row: dict[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    started_at = utc_now_iso()
    started_perf = time.perf_counter()
    status = "success"
    error: str | None = None
    error_type: str | None = None
    query: str | None = None
    bucket_name = ""
    scope_name = ""

    message = f"Generate SQL++ query. question: {question_row['question']}"
    if question_row.get("external_knowledge"):
        message = f"Generate SQL++ query. question: {question_row['question']}.\nexternal_knowledge: {question_row['external_knowledge']}"

    try:
        tool_result = await call_mcp_tool(
            session,
            tool_name="generate_or_modify_sql_plus_plus_query",
            tool_args={"message": message, "bucket_name": question_row.get("db", "")},
        )
        backend_message: str | None = None
        if not tool_result:
            raise RuntimeError("Tool returned empty response.")
        if isinstance(tool_result, str):
            if not tool_result.strip():
                raise RuntimeError("Tool returned empty response.")
            parsed = json.loads(tool_result)
            query = str(parsed.get("query", "")).strip()
            bucket_name = str(parsed.get("bucket_name", "")).strip()
            scope_name = str(parsed.get("scope_name", "")).strip()
            backend_message = str(parsed.get("message", "")).strip() or None
        if not query:
            if backend_message:
                raise RuntimeError(f"Tool returned empty query. {backend_message}")
            raise RuntimeError("Tool returned empty query.")
    except Exception as exc:
        status = "error"
        error = str(exc)
        error_type = type(exc).__name__

    return query, {
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "latency_ms": round((time.perf_counter() - started_perf) * 1000, 2),
        "status": status,
        "error": error,
        "error_type": error_type,
        "bucket_name": bucket_name,
        "scope_name": scope_name,
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--questions_file",
        type=Path,
        default=BASE_DIR / "questions.json",
        help="Path to questions JSON file (default: test/questions.json)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max number of questions to process (0 = all)",
    )
    args = parser.parse_args()

    load_env_file()
    questions = load_questions(args.questions_file)
    if args.limit > 0:
        questions = questions[: args.limit]

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    QUERIES_DIR.mkdir(parents=True, exist_ok=True)

    params = build_server_params()
    _server_log = SERVER_LOG_FILE.open("a", encoding="utf-8")
    async with stdio_client(params, errlog=_server_log) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            for question_row in questions:
                output_paths = build_output_paths(question_row["instance_id"])
                query, generation = await generate_query_for_question(
                    session=session,
                    question_row=question_row,
                )

                bucket_name = generation["bucket_name"]
                scope_name = generation["scope_name"]

                metadata = {
                    "instance_id": question_row["instance_id"],
                    "question": question_row["question"],
                    "started_at": generation["started_at"],
                    "finished_at": generation["finished_at"],
                    "latency_ms": generation["latency_ms"],
                    "status": generation["status"],
                    "error_type": generation["error_type"],
                    "error": generation["error"],
                    "bucket_name": bucket_name,
                    "scope_name": scope_name,
                    "query_file": str(output_paths["query"].relative_to(OUTPUT_DIR)),
                }

                if query:
                    header_lines = []
                    if bucket_name:
                        header_lines.append(f"-- bucket: {bucket_name}")
                    if scope_name:
                        header_lines.append(f"-- scope: {scope_name}")
                    header = ("\n".join(header_lines) + "\n\n") if header_lines else ""
                    output_paths["query"].write_text(header + query + "\n", encoding="utf-8")

                append_log(metadata)
                print(f"{question_row['instance_id']}: {metadata['status']}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except BrokenPipeError:
        pass
    except BrokenResourceError:
        pass