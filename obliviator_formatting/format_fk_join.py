# obliviator_formatting/format_fk_join.py

import argparse
import csv
from pathlib import Path
from typing import List

def format_for_fk_join(
    filepath1: str,
    key1: str,
    payload1_cols: List[str],
    filepath2: str,
    key2: str,
    payload2_cols: List[str],
    output_path: str
):
    """
    Reads two CSV files and formats them for the Obliviator FK Join operator,
    matching the C program's expected input format.

    - The output format is:
      <num_rows_table1> <num_rows_table2>
      <key> <payload>
      ...

    Args:
        filepath1 (str): Path to the primary table CSV.
        key1 (str): The join key column in the primary table.
        payload1_cols (list[str]): Payload columns from the primary table.
        filepath2 (str): Path to the foreign table CSV.
        key2 (str): The join key column in the foreign table.
        payload2_cols (list[str]): Payload columns from the foreign table.
        output_path (str): Path for the combined and formatted output file.
    """
    print("--- Formatting CSVs for FK Join ---")
    table1_rows = []
    table2_rows = []

    # Process the first (primary) table
    with open(filepath1, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        header = reader.fieldnames
        if not header: raise ValueError(f"CSV file is empty or has no header: {filepath1}")
        required_cols = {key1, *payload1_cols}
        if not required_cols.issubset(header): raise ValueError(f"Missing columns in {filepath1}")
        
        for row in reader:
            join_key = row[key1]
            payload_string = ",".join(row[col] for col in payload1_cols)
            table1_rows.append(f"{join_key} {payload_string}\n")

    # Process the second (foreign) table
    with open(filepath2, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        header = reader.fieldnames
        if not header: raise ValueError(f"CSV file is empty or has no header: {filepath2}")
        required_cols = {key2, *payload2_cols}
        if not required_cols.issubset(header): raise ValueError(f"Missing columns in {filepath2}")

        for row in reader:
            join_key = row[key2]
            payload_string = ",".join(row[col] for col in payload2_cols)
            table2_rows.append(f"{join_key} {payload_string}\n")

    # Write the combined output file with the correct header
    with open(output_path, "w", encoding='utf-8') as outfile:
        # CORRECT HEADER: num_rows_table1 num_rows_table2
        outfile.write(f"{len(table1_rows)} {len(table2_rows)}\n")
        outfile.writelines(table1_rows)
        outfile.writelines(table2_rows)

    print(f"Formatting complete. {len(table1_rows) + len(table2_rows)} total rows written to {output_path}.")


def main():
    parser = argparse.ArgumentParser(description="Formats two CSV files for Obliviator FK Join.")
    parser.add_argument("--filepath1", required=True)
    parser.add_argument("--key1", required=True)
    parser.add_argument("--payload1_cols", nargs='+', required=True)
    parser.add_argument("--filepath2", required=True)
    parser.add_argument("--key2", required=True)
    parser.add_argument("--payload2_cols", nargs='+', required=True)
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()
    
    format_for_fk_join(
        args.filepath1, args.key1, args.payload1_cols,
        args.filepath2, args.key2, args.payload2_cols,
        args.output_path
    )

if __name__ == "__main__":
    main()
