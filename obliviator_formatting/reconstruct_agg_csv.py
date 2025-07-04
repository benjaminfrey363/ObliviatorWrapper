# obliviator_formatting/reconstruct_agg_csv.py

import argparse
import csv
from typing import List

def reconstruct_agg_csv(
    intermediate_path: str,
    final_csv_path: str,
    group_by_header: str,
    payload_headers: List[str]
):
    """
    Reconstructs a final CSV from the intermediate output of the Aggregation operator.
    """
    print("--- Reconstructing Final Aggregation CSV ---")
    
    # --- FIX: Use more descriptive headers based on observed behavior ---
    final_header = [group_by_header, 'representative_value', 'global_aggregate'] + payload_headers
    
    with open(intermediate_path, 'r', encoding='utf-8') as infile, \
         open(final_csv_path, 'w', newline='', encoding='utf-8') as outfile:
        
        writer = csv.writer(outfile)
        writer.writerow(final_header)

        for line in infile:
            # Intermediate format is: group_key|agg_val_1|agg_val_2|payload_str
            parts = line.strip().split('|')
            if len(parts) != 4:
                continue

            group_key, agg_val1, agg_val2, payload_str = parts
            payload_vals = payload_str.split(',')
            
            writer.writerow([group_key, agg_val1, agg_val2] + payload_vals)

    print(f"CSV reconstruction complete. Final output at: {final_csv_path}")

def main():
    parser = argparse.ArgumentParser(description="Reconstructs a CSV from Aggregation intermediate output.")
    parser.add_argument("--intermediate_path", required=True)
    parser.add_argument("--final_csv_path", required=True)
    parser.add_argument("--group_by_header", required=True)
    parser.add_argument("--payload_headers", nargs='+', required=True)
    args = parser.parse_args()

    reconstruct_agg_csv(
        args.intermediate_path, args.final_csv_path,
        args.group_by_header, args.payload_headers
    )

if __name__ == "__main__":
    main()
