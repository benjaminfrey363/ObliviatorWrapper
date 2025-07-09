# obliviator_formatting/reconstruct_fk_join_csv.py

import argparse
import csv
from typing import List


'''
def reconstruct_fk_join_csv(
    intermediate_path: str,
    final_csv_path: str,
    key_header: str,
    payload1_headers: List[str],
    payload2_headers: List[str]
):
    """
    Reconstructs a final CSV from the intermediate output of the FK Join.
    """
    print("--- Reconstructing Final FK Join CSV ---")
    
    # The final header is key, then all headers from table 1, then all headers from table 2
    final_header = [key_header] + payload1_headers + payload2_headers
    
    with open(intermediate_path, 'r', encoding='utf-8') as infile, \
         open(final_csv_path, 'w', newline='', encoding='utf-8') as outfile:
        
        writer = csv.writer(outfile)
        writer.writerow(final_header)

        for line in infile:
            # Split on the robust pipe delimiter
            parts = line.strip().split('|')
            if len(parts) != 3:
                print(f"Warning: Skipping malformed intermediate line: '{line.strip()}'")
                continue

            key_val, payload1_str, payload2_str = parts
            
            # Payloads are comma-separated strings
            payload1_vals = payload1_str.split(',')
            payload2_vals = payload2_str.split(',')
            
            # Write the final, correctly ordered CSV row
            writer.writerow([key_val] + payload1_vals + payload2_vals)

    print(f"CSV reconstruction complete. Final output at: {final_csv_path}")
'''


# Updated function:

def reconstruct_fk_join_csv(
    intermediate_path: str,
    final_csv_path: str,
    key_header: str,
    payload1_headers: List[str],
    payload2_headers: List[str]
):
    """
    Reconstructs a final CSV from the C program's direct output.
    This version assumes the C output is in the format:
    key|payload1_field1|payload1_field2|payload2_field1|...
    """
    print("--- Reconstructing Final FK Join CSV ---")
    
    # The final header includes the key and all payload headers
    final_header = [key_header] + payload1_headers + payload2_headers
    
    with open(intermediate_path, 'r', encoding='utf-8') as infile, \
         open(final_csv_path, 'w', newline='', encoding='utf-8') as outfile:
        
        writer = csv.writer(outfile)
        writer.writerow(final_header)

        for line in infile:
            # The entire line from the C program is now pipe-delimited
            parts = line.strip().split('|')
            
            # The number of parts should match the number of columns
            if len(parts) != len(final_header):
                print(f"Warning: Skipping malformed intermediate line: '{line.strip()}'")
                continue
            
            # The parts are already correctly separated, just write them
            writer.writerow(parts)

    print(f"CSV reconstruction complete. Final output at: {final_csv_path}")



def main():
    parser = argparse.ArgumentParser(description="Reconstructs a CSV from FK Join intermediate output.")
    parser.add_argument("--intermediate_path", required=True)
    parser.add_argument("--final_csv_path", required=True)
    parser.add_argument("--key_header", required=True)
    parser.add_argument("--payload1_headers", nargs='+', required=True)
    parser.add_argument("--payload2_headers", nargs='+', required=True)
    args = parser.parse_args()

    reconstruct_fk_join_csv(
        args.intermediate_path, args.final_csv_path, args.key_header,
        args.payload1_headers, args.payload2_headers
    )

if __name__ == "__main__":
    main()
