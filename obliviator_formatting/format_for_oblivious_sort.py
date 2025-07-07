# obliviator_formatting/format_for_oblivious_sort.py

import argparse
import csv
from datetime import datetime

# A large, fixed timestamp in the future to subtract from, ensuring descending sort.
# Represents 2200-01-01
MAX_TIMESTAMP = 4102444800 
# A large number for message IDs to ensure descending sort on ties.
MAX_ID = 999999999999999999

def format_for_sort(input_path: str, output_path: str):
    """
    Prepares a CSV file for sorting via the aggregation operator.
    It creates a composite sort key that allows for descending chronological order.
    
    Input CSV must have 'messageCreationDate' and 'messageId' columns.
    
    Output format: <sort_key> <dummy_agg_value> <original_payload_as_string>
    """
    print("--- Preparing data for oblivious sort via aggregation ---")
    
    rows_to_write = []
    original_header = []
    try:
        with open(input_path, 'r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            original_header = reader.fieldnames
            if not original_header:
                return # Empty file

            for row in reader:
                # Convert date to a Unix timestamp (integer)
                creation_timestamp = int(datetime.fromisoformat(row['messageCreationDate'].rstrip('Z')).timestamp())
                
                # Create a key that sorts descending by date, then descending by ID
                sort_key_date = MAX_TIMESTAMP - creation_timestamp
                sort_key_id = MAX_ID - int(row['messageId'])
                
                # Combine into a single, padded string key for lexicographical sorting
                sort_key = f"{sort_key_date:012d}_{sort_key_id:020d}"
                
                # The payload is all original columns, joined into a single string
                payload_str = ",".join(row.values())
                
                # The dummy aggregate value can be anything, e.g., 1
                rows_to_write.append(f"{sort_key} 1 {payload_str}\n")

    except (FileNotFoundError, KeyError) as e:
        print(f"Error preparing for sort. Check input file and columns. Details: {e}")
        raise

    # Write the formatted output file
    with open(output_path, "w", encoding='utf-8') as outfile:
        # Header for the aggregation operator
        outfile.write(f"{len(rows_to_write)}\n")
        outfile.writelines(rows_to_write)
        
    print("Preparation for oblivious sort complete.")

def main():
    parser = argparse.ArgumentParser(description="Prepares data for sorting via aggregation.")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()
    format_for_sort(args.input_path, args.output_path)

if __name__ == "__main__":
    main()
