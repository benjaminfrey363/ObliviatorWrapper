# obliviator_formatting/reconstruct_csv.py

import argparse
import csv
from pathlib import Path
from typing import List

def reconstruct_csv(
    intermediate_path: str,
    final_csv_path: str,
    filter_col: str,
    payload_cols: List[str]
):
    """
    Takes the intermediate output from the Obliviator pipeline and reconstructs
    it into a final CSV file with the original headers.

    Args:
        intermediate_path (str): The path to the file containing space-separated
                                 filter values and comma-separated payload strings.
        final_csv_path (str): The path for the final output CSV file.
        filter_col (str): The name of the original filter column.
        payload_cols (list[str]): The names of the original payload columns.
    """
    print("--- Reconstructing Final CSV ---")
    
    # The header for the new CSV file will be the filter column plus the payload columns
    final_header = [filter_col] + payload_cols
    
    try:
        with open(intermediate_path, 'r', encoding='utf-8') as infile, \
             open(final_csv_path, 'w', newline='', encoding='utf-8') as outfile:
            
            writer = csv.writer(outfile, delimiter='|')
            writer.writerow(final_header) # Write the header row

            for line in infile:
                parts = line.strip().split(maxsplit=1)
                if len(parts) != 2:
                    continue # Skip any malformed lines

                filter_val, payload_str = parts
                payload_vals = payload_str.split('|')

                # print(f"DEBUG: key='{filter_val}' payload='{payload_str}' split_vals={payload_vals}")
                
                # Write the reconstructed row
                writer.writerow([filter_val] + payload_vals)

    except FileNotFoundError:
        print(f"Error: Intermediate file not found at {intermediate_path}")
        raise
    except Exception as e:
        print(f"An error occurred during CSV reconstruction: {e}")
        raise
        
    print(f"CSV reconstruction complete. Final output at: {final_csv_path}")

def main():
    parser = argparse.ArgumentParser(description="Reconstructs a CSV from Obliviator's intermediate output.")
    parser.add_argument("--intermediate_path", required=True)
    parser.add_argument("--final_csv_path", required=True)
    parser.add_argument("--filter_col", required=True)
    parser.add_argument("--payload_cols", nargs='+', required=True)
    args = parser.parse_args()

    reconstruct_csv(args.intermediate_path, args.final_csv_path, args.filter_col, args.payload_cols)

if __name__ == "__main__":
    main()
