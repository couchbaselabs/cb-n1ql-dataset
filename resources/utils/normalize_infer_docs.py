"""
Recursively sets all "#docs" values to 1000 and "%docs" values to 100
in every .json file under resources/databases/couchbase_sf/.
"""

import json
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
TARGET_DIR = BASE_DIR / "databases" / "couchbase_sf"


def normalize_docs(obj):
    if isinstance(obj, dict):
        for key in list(obj.keys()):
            if key == "#docs":
                obj[key] = 1000
            elif key == "%docs":
                obj[key] = 100
            else:
                normalize_docs(obj[key])
    elif isinstance(obj, list):
        for item in obj:
            normalize_docs(item)


def main():
    count = 0
    for path in sorted(TARGET_DIR.rglob("*.json")):
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        normalize_docs(data)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        count += 1

    print(f"Done. Normalized {count} file(s) in {TARGET_DIR}")


if __name__ == "__main__":
    main()
