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
    Reads two CSV files and formats them for an Obliviator Join operator.
    """
    print("--- Formatting CSVs for Join ---")
    table1_rows = []
    table2_rows = []

    # --- Process the first table ---
    try:
        # FIX: Use 'utf-8-sig' to automatically handle Byte Order Marks (BOM)
        with open(filepath1, mode='r', newline='', encoding='utf-8-sig') as infile:
            # LDBC is pipe-separated
            reader = csv.DictReader(infile, delimiter='|')
            header1 = reader.fieldnames
            if not header1:
                raise ValueError(f"CSV file is empty or has no header: {filepath1}")
            
            required_cols1 = {key1, *payload1_cols}
            # FIX: Provide a much more detailed error message if columns are missing
            if not required_cols1.issubset(header1):
                missing = sorted(list(required_cols1 - set(header1)))
                raise ValueError(
                    f"Missing columns in {filepath1}.\n"
                    f"  Required: {sorted(list(required_cols1))}\n"
                    f"  Found:    {header1}\n"
                    f"  Missing:  {missing}"
                )
            
            for row in reader:
                join_key = row[key1]
                payload_string = "|".join(row[col] for col in payload1_cols)

                # If payload string is empty, use placeholder to prevent malformed lines
                if not payload_string:
                    payload_string = "_"

                table1_rows.append(f"{join_key} {payload_string}\n")
    except FileNotFoundError:
        raise FileNotFoundError(f"Input file not found: {filepath1}")


    # --- Process the second table ---
    try:
        with open(filepath2, mode='r', newline='', encoding='utf-8-sig') as infile:
            # Pipe-separated
            reader = csv.DictReader(infile, delimiter='|')
            header2 = reader.fieldnames
            if not header2:
                raise ValueError(f"CSV file is empty or has no header: {filepath2}")

            required_cols2 = {key2, *payload2_cols}
            if not required_cols2.issubset(header2):
                missing = sorted(list(required_cols2 - set(header2)))
                raise ValueError(
                    f"Missing columns in {filepath2}.\n"
                    f"  Required: {sorted(list(required_cols2))}\n"
                    f"  Found:    {header2}\n"
                    f"  Missing:  {missing}"
                )

            for row in reader:
                join_key = row[key2]
                payload_string = "|".join(row[col] for col in payload2_cols)

                # If payload string is empty, use placeholder to prevent malformed lines
                if not payload_string:
                    payload_string = "_"

                table2_rows.append(f"{join_key} {payload_string}\n")
    except FileNotFoundError:
        raise FileNotFoundError(f"Input file not found: {filepath2}")


    # --- Write the combined output file ---
    with open(output_path, "w", encoding='utf-8') as outfile:
        outfile.write(f"{len(table1_rows)} {len(table2_rows)}\n")
        outfile.writelines(table1_rows)
        outfile.writelines(table2_rows)

    print(f"Formatting complete. {len(table1_rows) + len(table2_rows)} total rows written to {output_path}.")


def main():
    parser = argparse.ArgumentParser(description="Formats two CSV files for an Obliviator Join.")
    parser.add_argument("--filepath1", required=True)
    parser.add_argument("--key1", required=True)
    parser.add_argument("--payload1_cols", nargs='*', required=False, default = [])
    parser.add_argument("--filepath2", required=True)
    parser.add_argument("--key2", required=True)
    parser.add_argument("--payload2_cols", nargs='*', required=False, default=[])
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()
    
    format_for_fk_join(
        args.filepath1, args.key1, args.payload1_cols,
        args.filepath2, args.key2, args.payload2_cols,
        args.output_path
    )

if __name__ == "__main__":
    main()
