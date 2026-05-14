#!/usr/bin/env python3
"""
LLM-based analysis of generated SQL++ queries vs ground truth.

For each generated query:
  - If ground truth exists: compare question intent, gold query, and generated query
  - If no ground truth: compare question intent vs generated query intent

Usage:
    python llm_analysis.py <generated_queries_dir> [options]

Examples:
    python llm_analysis.py ../runs/gpt-5.4/submission
    python llm_analysis.py ../runs/gpt-5.4/submission --output my_report.md --limit 10
    python llm_analysis.py ../runs/gpt-5.4/submission --provider openai --model gpt-4o
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*a, **kw):
        pass

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(it, **kw):
        return it


GOLD_DIR = Path(__file__).parent / "gold" / "sqlpp"
QUESTIONS_FILE = Path(__file__).parent / "NL_questions.jsonl"

SYSTEM_PROMPT = "You are an expert SQL++ query analyst. Be concise, precise, and technical."

PROMPT_WITH_GOLD = """Evaluate the generated SQL++ query against the ground truth for the given question.

**Question:**
{question}

**Ground Truth Query:**
```sql
{gold_query}
```

**Generated Query:**
```sql
{generated_query}
```

Provide your analysis in the following structure:

1. **Intent Match** (Yes / Partial / No): Does the generated query address the same question as the ground truth?
2. **Approach Comparison**: Key structural or logical differences from the ground truth.
3. **Correctness Assessment**: Is the generated query likely to produce correct results?
4. **Issues**: Specific problems found (wrong aggregation, missing filters, incorrect joins, wrong columns, etc.). Write "None" if no issues.
5. **Overall Score** (1–5):
   - 5 = functionally equivalent to ground truth
   - 4 = minor differences unlikely to affect results
   - 3 = partially correct / handles main intent but misses details
   - 2 = major issues but on the right track
   - 1 = incorrect or does not address the question"""

PROMPT_WITHOUT_GOLD = """Evaluate the generated SQL++ query for the given question. No ground truth is available.

**Question:**
{question}

**Generated Query:**
```sql
{generated_query}
```

Provide your analysis in the following structure:

1. **Intent Match** (Yes / Partial / No): Does the generated query appear to correctly address the question's intent?
2. **Query Logic**: What does the query actually compute and does it align with the question?
3. **Issues**: Specific problems found (wrong aggregation, missing filters, incorrect logic, unhandled edge cases, etc.). Write "None" if no issues.
4. **Overall Score** (1–5):
   - 5 = clearly correct
   - 4 = likely correct with minor concerns
   - 3 = partially addresses the question
   - 2 = significant issues
   - 1 = does not address the question"""


# ---------------------------------------------------------------------------
# LLM Client (mirrors baselines/promptSQL++/ask_llm.py)
# ---------------------------------------------------------------------------

class LLMClient:
    def __init__(self, provider: str, model: str, thinking: bool = False, reasoning_effort: str = "high"):
        self.provider = provider.lower()
        self.model = model
        self.thinking = thinking
        self.reasoning_effort = reasoning_effort
        self._client = None
        self._init_client()

    def _init_client(self):
        if self.provider == "gemini":
            from google import genai
            api_key = os.environ.get("GEMINI_API_KEY", "")
            if not api_key:
                raise ValueError("GEMINI_API_KEY not set")
            self._client = genai.Client(api_key=api_key)

        elif self.provider == "openai":
            from openai import OpenAI
            api_key = os.environ.get("OPENAI_API_KEY", "")
            if not api_key:
                raise ValueError("OPENAI_API_KEY not set")
            base_url = os.environ.get("OPENAI_BASE_URL")
            self._client = OpenAI(api_key=api_key, **({"base_url": base_url} if base_url else {}))

        elif self.provider == "factory":
            from openai import OpenAI
            api_key = os.environ.get("FACTORY_API_KEY", "")
            if not api_key:
                raise ValueError("FACTORY_API_KEY not set")
            self._client = OpenAI(api_key=api_key, base_url="https://api.factory.ai/api/v0/")

        else:
            raise ValueError(f"Unsupported provider: {self.provider}. Use 'gemini', 'openai', or 'factory'.")

    def generate(self, system: str, prompt: str) -> str:
        if self.provider == "gemini":
            return self._generate_gemini(system, prompt)
        return self._generate_openai(system, prompt)

    def _generate_gemini(self, system: str, prompt: str) -> str:
        from google.genai import types
        response = self._client.models.generate_content(
            model=self.model,
            contents=prompt,
            config=types.GenerateContentConfig(system_instruction=system),
        )
        return response.text.strip()

    def _generate_openai(self, system: str, prompt: str, _retries: int = 3) -> str:
        for attempt in range(1, _retries + 1):
            try:
                if self.thinking:
                    response = self._client.chat.completions.create(
                        model=self.model,
                        messages=[
                            {"role": "developer", "content": system},
                            {"role": "user", "content": prompt},
                        ],
                        reasoning_effort=self.reasoning_effort,
                    )
                else:
                    response = self._client.chat.completions.create(
                        model=self.model,
                        messages=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": prompt},
                        ],
                        temperature=0.0,
                    )
                return response.choices[0].message.content.strip()
            except Exception as e:
                err_str = str(e)
                is_transient = any(k in err_str for k in [
                    "502", "503", "429", "rate_limit", "timeout", "overloaded", "server_error",
                ])
                if is_transient and attempt < _retries:
                    wait = 2 ** attempt * 5
                    print(f"\n  Transient error (attempt {attempt}/{_retries}), retrying in {wait}s: {err_str[:120]}")
                    time.sleep(wait)
                else:
                    raise


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def load_questions(path: Path) -> dict:
    questions = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                item = json.loads(line)
                questions[item["instance_id"]] = item
    return questions


def extract_score(text: str) -> int | None:
    for line in text.splitlines():
        lower = line.lower()
        if "overall score" in lower:
            for ch in line:
                if ch in "12345":
                    return int(ch)
    return None


def analyze_instance(client: LLMClient, instance_id: str, question: str,
                     gold_query: str | None, generated_query: str) -> dict:
    if gold_query:
        prompt = PROMPT_WITH_GOLD.format(
            question=question,
            gold_query=gold_query,
            generated_query=generated_query,
        )
    else:
        prompt = PROMPT_WITHOUT_GOLD.format(
            question=question,
            generated_query=generated_query,
        )

    analysis = client.generate(system=SYSTEM_PROMPT, prompt=prompt)
    return {
        "instance_id": instance_id,
        "question": question,
        "has_gold": gold_query is not None,
        "score": extract_score(analysis),
        "analysis": analysis,
    }


def build_report(results: list, generated_dir: str) -> str:
    total = len(results)
    with_gold = [r for r in results if r["has_gold"]]
    without_gold = [r for r in results if not r["has_gold"]]
    scored = [r for r in results if r["score"] is not None]

    lines = [
        "# LLM Query Analysis Report",
        "",
        f"**Generated queries directory:** `{generated_dir}`",
        f"**Total instances analyzed:** {total}",
        f"**With ground truth:** {len(with_gold)}",
        f"**Without ground truth:** {len(without_gold)}",
        "",
    ]

    if scored:
        avg_score = sum(r["score"] for r in scored) / len(scored)
        dist = {i: sum(1 for r in scored if r["score"] == i) for i in range(1, 6)}
        lines += [
            "## Score Summary",
            "",
            f"**Average score:** {avg_score:.2f} / 5.0 (across {len(scored)} scored instances)",
            "",
            "| Score | Count |",
            "|-------|-------|",
        ]
        for s in range(5, 0, -1):
            lines.append(f"| {s}     | {dist[s]}     |")
        lines.append("")

    lines += ["---", "", "## Instance-by-Instance Analysis", ""]

    for r in sorted(results, key=lambda x: (x["score"] or 0)):
        score_str = f"{r['score']}/5" if r["score"] else "N/A"
        gold_tag = "with gold" if r["has_gold"] else "no gold"
        lines += [
            f"### `{r['instance_id']}` — Score: {score_str} ({gold_tag})",
            "",
            f"**Question:** {r['question']}",
            "",
            r["analysis"],
            "",
            "---",
            "",
        ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    script_dir = Path(__file__).parent
    load_dotenv(script_dir.parent / "baselines" / "promptSQL++" / ".env")

    parser = argparse.ArgumentParser(description="LLM-based SQL++ query analysis report")
    parser.add_argument("generated_dir", help="Directory containing generated .sqlpp files")
    parser.add_argument("--output", default="analysis_report.md", help="Output report file (default: analysis_report.md)")
    parser.add_argument("--provider", default=os.environ.get("LLM_PROVIDER", "openai"),
                        help="LLM provider: gemini, openai, factory")
    parser.add_argument("--model", default=os.environ.get("LLM_MODEL", "gpt-4o"),
                        help="Model name")
    parser.add_argument("--thinking", action="store_true", default=False)
    parser.add_argument("--thinking_effort", default="high", choices=["low", "medium", "high", "xhigh"])
    parser.add_argument("--delay", type=float, default=float(os.environ.get("REQUEST_DELAY", "0.5")),
                        help="Seconds between API calls")
    parser.add_argument("--limit", type=int, default=0, help="Process only N instances (0 = all)")
    parser.add_argument("--skip_existing", action="store_true",
                        help="Skip instances already present in a previous report JSON sidecar")
    args = parser.parse_args()

    generated_dir = Path(args.generated_dir)
    if not generated_dir.exists():
        print(f"Error: directory not found: {generated_dir}", file=sys.stderr)
        sys.exit(1)

    questions = load_questions(QUESTIONS_FILE)

    generated_files = sorted(generated_dir.glob("*.sqlpp"))
    if args.limit > 0:
        generated_files = generated_files[: args.limit]

    print(f"Provider : {args.provider}")
    print(f"Model    : {args.model}")
    print(f"Instances: {len(generated_files)}")
    print()

    client = LLMClient(provider=args.provider, model=args.model,
                       thinking=args.thinking, reasoning_effort=args.thinking_effort)

    results = []
    errors = []

    for i, gen_path in enumerate(tqdm(generated_files, desc="Analyzing"), 1):
        instance_id = gen_path.stem
        generated_query = gen_path.read_text().strip()
        if not generated_query or generated_query.startswith("-- ERROR"):
            print(f"  SKIP {instance_id} — empty or errored generated query")
            continue

        question_data = questions.get(instance_id)
        if not question_data:
            print(f"  SKIP {instance_id} — not found in questions file")
            continue

        gold_path = GOLD_DIR / f"{instance_id}.sqlpp"
        gold_query = gold_path.read_text().strip() if gold_path.exists() else None

        try:
            result = analyze_instance(
                client=client,
                instance_id=instance_id,
                question=question_data["question"],
                gold_query=gold_query,
                generated_query=generated_query,
            )
            results.append(result)
        except Exception as e:
            print(f"  ERROR {instance_id}: {e}")
            errors.append(f"{instance_id}: {e}")

        if i < len(generated_files):
            time.sleep(args.delay)

    report = build_report(results, str(generated_dir))
    output_path = Path(args.output)
    output_path.write_text(report)
    print(f"\nReport written to: {output_path}")

    scored = [r for r in results if r["score"] is not None]
    if scored:
        avg = sum(r["score"] for r in scored) / len(scored)
        print(f"Average score: {avg:.2f}/5.0 across {len(scored)} instances")

    if errors:
        print(f"\n{len(errors)} errors occurred:")
        for e in errors:
            print(f"  {e}")


if __name__ == "__main__":
    main()
