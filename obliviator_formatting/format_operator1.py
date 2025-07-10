# obliviator_formatting/format_operator1.py

import argparse
import csv
from pathlib import Path
from typing import List

def format_for_operator1(
    filepath: str,
    output_path: str,
    filter_col: str,
    payload_cols: List[str]
):
    """
    Reads a CSV file and formats it for Obliviator Operator 1.

    - Extracts a single column to be used for filtering.
    - Extracts one or more payload columns and joins them into a single,
      comma-separated string. This becomes the 'value' for the operator.
    - Writes the output in the format: filter_col_value payload_string

    Args:
        filepath (str): Path to the input CSV file.
        output_path (str): Path for the formatted output file.
        filter_col (str): The name of the column to use for filtering.
        payload_cols (list[str]): A list of column names to be concatenated
                                  into the payload.
    """
    print("--- Formatting CSV for Operator 1 ---")
    print(f"Filter column: {filter_col}")
    print(f"Payload columns: {payload_cols}")

    rows = []
    try:
        with open(filepath, mode='r', newline='', encoding='utf-8') as infile:
            # LDBC is pipe-separated
            reader = csv.DictReader(infile, delimiter='|')
            
            # Verify that all specified columns exist in the CSV header
            header = reader.fieldnames
            print(header)
            if not header:
                raise ValueError("CSV file is empty or has no header.")
            
            required_cols = {filter_col, *payload_cols}
            missing_cols = required_cols - set(header)
            if missing_cols:
                raise ValueError(f"Missing required columns in CSV file: {', '.join(missing_cols)}")

            for row in reader:
                filter_value = row[filter_col]
                
                # Create the pipe-separated payload string
                payload_values = [row[col] for col in payload_cols]
                payload_string = "|".join(payload_values)
                
                rows.append(f"{filter_value} {payload_string}\n")

    except FileNotFoundError:
        print(f"Error: Input file not found at {filepath}")
        raise
    except Exception as e:
        print(f"An error occurred during CSV processing: {e}")
        raise

    # Write the formatted output file
    with open(output_path, "w") as outfile:
        # The C program expects a header line with the number of data rows
        # and a second number (which is 0 for this operator).
        outfile.write(f"{len(rows)} 0\n")
        outfile.writelines(rows)

    print(f"Formatting complete. {len(rows)} rows written to {output_path}.")


def main():
    parser = argparse.ArgumentParser(description="Formats a CSV file for Obliviator Operator 1.")
    parser.add_argument("--filepath", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--filter_col", required=True, help="The column to be used for filtering.")
    parser.add_argument("--payload_cols", nargs='+', required=True, help="One or more columns to include in the payload.")
    args = parser.parse_args()
    
    format_for_operator1(args.filepath, args.output_path, args.filter_col, args.payload_cols)


if __name__ == "__main__":
    main()
