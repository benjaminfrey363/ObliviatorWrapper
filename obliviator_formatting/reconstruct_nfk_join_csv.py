import argparse
import csv
from typing import List

def reconstruct_nfk_join_csv(
    intermediate_path: str,
    final_csv_path: str,
    key_header: str,
    payload1_headers: List[str],
    payload2_headers: List[str]
):
    """
    Reconstructs the final CSV for an NFK join from the intermediate,
    human-readable file.
    """
    print("--- Reconstructing Final NFK Join CSV ---")
    
    # Define the final header row
    final_header = [f"t1.{key_header}"] + [f"t1.{h}" for h in payload1_headers] + [f"t2.{h}" for h in payload2_headers]
    
    with open(intermediate_path, 'r', encoding='utf-8') as infile, \
         open(final_csv_path, 'w', newline='', encoding='utf-8') as outfile:
        
        writer = csv.writer(outfile, delimiter='|')
        writer.writerow(final_header)

        for line in infile:
            parts = line.strip().split('|')
            # Expects key | payload1 | payload2
            if len(parts) != 3:
                print(f"Warning: Skipping malformed intermediate line: '{line.strip()}'")
                continue
            
            key_str, p1_str, p2_str = parts
            
            p1_vals = p1_str.split('|')
            p2_vals = p2_str.split('|')
            
            # Write the final reconstructed row
            writer.writerow([key_str] + p1_vals + p2_vals)

    print(f"CSV reconstruction complete. Final output at: {final_csv_path}")

def main():
    parser = argparse.ArgumentParser(description="Reconstructs a CSV from NFK join intermediate output.")
    parser.add_argument("--intermediate_path", required=True)
    parser.add_argument("--final_csv_path", required=True)
    parser.add_argument("--key_header", required=True)
    parser.add_argument("--payload1_headers", nargs='+', required=True)
    parser.add_argument("--payload2_headers", nargs='+', required=True)
    args = parser.parse_args()

    reconstruct_nfk_join_csv(
        args.intermediate_path,
        args.final_csv_path,
        args.key_header,
        args.payload1_headers,
        args.payload2_headers
    )

if __name__ == "__main__":
    main()
