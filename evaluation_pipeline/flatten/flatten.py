import json
import sys
import pandas as pd
from copy import deepcopy


def flatten_object(obj, prefix=""):
    """
    Flattens a single JSON object into a list of flat dict rows.

    Rules:
    - Nested dict → flatten keys with dot notation.
    - Nested list → explode into multiple rows.
    - Multiple lists → Cartesian product.
    - Scalars → stay in same row.
    """

    # Start with one empty row
    rows = [{}]

    if isinstance(obj, dict):
        for key, value in obj.items():
            full_key = f"{prefix}.{key}" if prefix else key

            # Case 1: Nested dictionary
            if isinstance(value, dict):
                nested_rows = flatten_object(value, full_key)
                rows = merge_rows(rows, nested_rows)

            # Case 2: Nested list → explode
            elif isinstance(value, list):
                list_rows = []
                for item in value:
                    exploded = flatten_object(item, full_key)
                    list_rows.extend(exploded)

                rows = merge_rows(rows, list_rows)

            # Case 3: Scalar
            else:
                for row in rows:
                    row[full_key] = value

        return rows

    # If it's a scalar (leaf inside list)
    else:
        return [{prefix: obj}] if prefix else [{}]


def merge_rows(base_rows, new_rows):
    """
    Cartesian product merge of two row lists.
    """
    merged = []
    for base in base_rows:
        for new in new_rows:
            combined = deepcopy(base)
            combined.update(new)
            merged.append(combined)
    return merged


def process_json(data):
    """
    Preserves top-level list.
    """
    if isinstance(data, list):
        final_rows = []
        for item in data:
            final_rows.extend(flatten_object(item))
        return final_rows
    else:
        return flatten_object(data)


def main():
    if len(sys.argv) < 2:
        print("Usage: python flatten.py <input_json_file>")
        sys.exit(1)

    input_file = sys.argv[1]

    with open(input_file, "r") as f:
        data = json.load(f)

    rows = process_json(data)

    print("\n===== FLATTENED JSON OUTPUT =====\n")
    print(json.dumps(rows, indent=4))

    with open("flattened_output.json", "w") as f:
        json.dump(rows, f, indent=4)

    print("\nSaved JSON to: flattened_output.json")

    if rows:
        df=pd.DataFrame(rows)
        csv_output="flattened_output.csv"
        df.to_csv(csv_output, index=False)
        print(f"\nSaved CSV to: {csv_output}")
    else:
        print("No rows to save as CSV")

if __name__ == "__main__":
    main()
