# obliviator_formatting/format_operator2.py

import argparse
import csv
from pathlib import Path
from typing import List

def format_for_operator2(
    filepath: str,
    output_path: str,
    group_by_col: str,
    agg_col: str,
    payload_cols: List[str]
):
    """
    Reads a CSV and formats it for the Obliviator Aggregation operator.

    The output format is: <group_key> <agg_value> <payload_string>
    """
    print("--- Formatting CSV for Aggregation (Operator 2) ---")
    rows = []
    
    with open(filepath, mode='r', newline='', encoding='utf-8-sig') as infile:
        reader = csv.DictReader(infile)
        header = reader.fieldnames
        if not header:
            raise ValueError(f"CSV file is empty or has no header: {filepath}")
        
        required_cols = {group_by_col, agg_col, *payload_cols}
        if not required_cols.issubset(header):
            missing = sorted(list(required_cols - set(header)))
            raise ValueError(f"Missing columns in {filepath}.\n  Required: {sorted(list(required_cols))}\n  Found:    {header}\n  Missing:  {missing}")

        for row in reader:
            group_key = row[group_by_col]
            agg_value = row[agg_col]
            payload_string = ",".join(row[col] for col in payload_cols)
            
            rows.append(f"{group_key} {agg_value} {payload_string}\n")

    with open(output_path, "w", encoding='utf-8') as outfile:
        # Header for the C program is a single number: the row count
        outfile.write(f"{len(rows)}\n")
        outfile.writelines(rows)

    print(f"Formatting complete. {len(rows)} rows written to {output_path}.")


def main():
    parser = argparse.ArgumentParser(description="Formats a CSV for Obliviator Aggregation.")
    parser.add_argument("--filepath", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--group_by_col", required=True)
    parser.add_argument("--agg_col", required=True, help="The column with numeric values to be aggregated.")
    parser.add_argument("--payload_cols", nargs='+', required=True)
    args = parser.parse_args()
    
    format_for_operator2(
        args.filepath, args.output_path, args.group_by_col,
        args.agg_col, args.payload_cols
    )

if __name__ == "__main__":
    main()
