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
    # Specify which table came frome
    final_header = ["t1." + key_header] + [ "t1." + header for header in payload1_headers ] + [ "t2." + header for header in payload2_headers ]
    
    with open(intermediate_path, 'r', encoding='utf-8') as infile, \
         open(final_csv_path, 'w', newline='', encoding='utf-8') as outfile:
        
        writer = csv.writer(outfile, delimiter='|')
        writer.writerow(final_header)

        for line in infile:
            # The entire line from the C program is now pipe-delimited
            # skip null entries if carrying no payload
            parts = [entry for entry in line.strip().split('|') if entry != '_']
            
            # The number of parts should match the number of columns
            if len(parts) != len(final_header):
                print(f"Warning: Skipping malformed intermediate line: '{line.strip()}'")
                continue
            
            # The parts are already correctly separated, just write them
            writer.writerow(parts)

    print(f"CSV reconstruction complete. Final output at: {final_csv_path}")


'''
def reconstruct_fk_join_csv(
    intermediate_path: str,
    final_csv_path: str,
    key_header: str,
    payload1_headers: List[str],
    payload2_headers: List[str]
):
    """
    Reconstructs a final CSV from the intermediate output of the join.
    This version correctly handles cases with empty payloads.
    """
    print("--- Reconstructing Final FK Join CSV ---")
    
    # Define the final header row, correctly handling empty payload lists
    final_header = [f"t1.{key_header}"]
    if payload1_headers:
        final_header.extend([f"t1.{h}" for h in payload1_headers])
    if payload2_headers:
        final_header.extend([f"t2.{h}" for h in payload2_headers])
    
    with open(intermediate_path, 'r', encoding='utf-8') as infile, \
         open(final_csv_path, 'w', newline='', encoding='utf-8') as outfile:
        
        writer = csv.writer(outfile, delimiter='|')
        writer.writerow(final_header)

        for line in infile:
            # The intermediate format is always key|payload1|payload2
            parts = line.strip().split('|')
            if len(parts) != 3:
                print(f"Warning: Skipping malformed intermediate line: '{line.strip()}'")
                continue

            key_val, payload1_str, payload2_str = parts
            
            # If the payload string is not empty, split it. Otherwise, use an empty list.
            payload1_vals = payload1_str.split('|') if payload1_str else []
            payload2_vals = payload2_str.split('|') if payload2_str else []
            
            # Construct the final row
            final_row = [key_val] + payload1_vals + payload2_vals
            writer.writerow(final_row)

    print(f"CSV reconstruction complete. Final output at: {final_csv_path}")
'''

    
def main():
    parser = argparse.ArgumentParser(description="Reconstructs a CSV from FK Join intermediate output.")
    parser.add_argument("--intermediate_path", required=True)
    parser.add_argument("--final_csv_path", required=True)
    parser.add_argument("--key_header", required=True)
    parser.add_argument("--payload1_headers", nargs='*', required=False, default=[])
    parser.add_argument("--payload2_headers", nargs='*', required=False, default=[])
    args = parser.parse_args()

    reconstruct_fk_join_csv(
        args.intermediate_path, args.final_csv_path, args.key_header,
        args.payload1_headers, args.payload2_headers
    )

if __name__ == "__main__":
    main()
