#!/usr/bin/env python3
"""
Step 3: Ask LLM

Sends prompts to the configured LLM (Gemini or OpenAI) and saves raw
SQL++ responses.

Input:  prompts/questions.json
Output: raw_output/<instance_id>.sqlpp

Configuration is read from .env file.
"""

import json
import os
import time
import argparse
from pathlib import Path
from tqdm import tqdm

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*a, **kw):
        pass  # gracefully degrade if python-dotenv not installed


# ---------------------------------------------------------------------------
# LLM Client abstraction
# ---------------------------------------------------------------------------

class LLMClient:
    """Unified interface for calling different LLM providers."""

    def __init__(self, provider: str, model: str):
        self.provider = provider.lower()
        self.model = model
        self._client = None
        self._init_client()

    def _init_client(self):
        if self.provider == "gemini":
            from google import genai
            api_key = os.environ.get("GEMINI_API_KEY", "")
            if not api_key:
                raise ValueError("GEMINI_API_KEY not set in .env or environment")
            self._client = genai.Client(api_key=api_key)

        elif self.provider == "openai":
            from openai import OpenAI
            api_key = os.environ.get("OPENAI_API_KEY", "")
            if not api_key:
                raise ValueError("OPENAI_API_KEY not set in .env or environment")
            self._client = OpenAI(api_key=api_key)

        else:
            raise ValueError(f"Unsupported LLM provider: {self.provider}. Use 'gemini' or 'openai'.")

    def generate(self, system: str, prompt: str) -> str:
        """Generate a response from the LLM. Returns the response text."""
        if self.provider == "gemini":
            return self._generate_gemini(system, prompt)
        elif self.provider == "openai":
            return self._generate_openai(system, prompt)

    def _generate_gemini(self, system: str, prompt: str) -> str:
        from google.genai import types
        response = self._client.models.generate_content(
            model=self.model,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=system,
            )
        )
        return response.text.strip()

    def _generate_openai(self, system: str, prompt: str) -> str:
        response = self._client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
        )
        return response.choices[0].message.content.strip()


def main():
    # Load .env file from the script directory
    script_dir = Path(__file__).parent
    load_dotenv(script_dir / ".env")

    parser = argparse.ArgumentParser(description="Send prompts to LLM and collect SQL++ responses")
    parser.add_argument("--input", type=str, default="prompts/questions.json",
                        help="Path to questions.json")
    parser.add_argument("--output_dir", type=str, default="raw_output",
                        help="Directory to save raw .sqlpp responses")
    parser.add_argument("--provider", type=str,
                        default=os.environ.get("LLM_PROVIDER", "gemini"),
                        help="LLM provider (gemini or openai)")
    parser.add_argument("--model", type=str,
                        default=os.environ.get("LLM_MODEL", "gemini-2.5-flash"),
                        help="Model name")
    parser.add_argument("--delay", type=float,
                        default=float(os.environ.get("REQUEST_DELAY", "1.0")),
                        help="Seconds to wait between API calls")
    parser.add_argument("--limit", type=int, default=0,
                        help="Process only N instances (0 = all)")
    parser.add_argument("--skip_existing", action="store_true",
                        help="Skip instances that already have output files")
    args = parser.parse_args()

    input_path = (script_dir / args.input).resolve()
    output_dir = (script_dir / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Provider: {args.provider}")
    print(f"Model:    {args.model}")
    print(f"Delay:    {args.delay}s between calls")
    print(f"Input:    {input_path}")
    print(f"Output:   {output_dir}")

    # Load questions
    with open(input_path, "r", encoding="utf-8") as f:
        questions = json.load(f)

    if args.limit > 0:
        questions = questions[:args.limit]
        print(f"Limiting to {args.limit} instances")

    # Filter out existing
    if args.skip_existing:
        before = len(questions)
        questions = [q for q in questions if not (output_dir / f"{q['instance_id']}.sqlpp").exists()]
        print(f"Skipping {before - len(questions)} existing, {len(questions)} remaining")

    if not questions:
        print("No questions to process.")
        return

    # Initialize LLM client
    client = LLMClient(provider=args.provider, model=args.model)
    print(f"\nProcessing {len(questions)} instances...\n")

    successes = 0
    failures = 0
    errors_log = []

    for i, q in enumerate(tqdm(questions, desc="Generating SQL++")):
        instance_id = q["instance_id"]
        output_path = output_dir / f"{instance_id}.sqlpp"

        try:
            response = client.generate(system=q["system"], prompt=q["prompt"])
            output_path.write_text(response, encoding="utf-8")
            successes += 1

        except Exception as e:
            error_msg = f"{instance_id}: {str(e)}"
            print(f"\n  Error: {error_msg}")
            errors_log.append(error_msg)
            failures += 1
            # Write error marker so we know it was attempted
            output_path.write_text(f"-- ERROR: {str(e)}", encoding="utf-8")

        # Rate limit (skip delay on last item)
        if i < len(questions) - 1:
            time.sleep(args.delay)

    # Summary
    print(f"\n{'='*50}")
    print(f"Results: {successes} succeeded, {failures} failed out of {len(questions)}")
    print(f"Output: {output_dir}")

    if errors_log:
        errors_path = output_dir / "_errors.log"
        errors_path.write_text("\n".join(errors_log), encoding="utf-8")
        print(f"Error log: {errors_path}")


if __name__ == "__main__":
    main()
