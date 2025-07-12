import argparse
import csv
from typing import List

def format_nfk_join(
    filepath1: str, key1: str, payload1_cols: List[str],
    filepath2: str, key2: str, payload2_cols: List[str],
    output_path: str
):
    """
    Formats two CSV files for a generic non-foreign key (NFK) join.
    Combines them into a single file with a table indicator (0 or 1).
    The output format is: `key|payload|table_id`
    """
    print("--- Formatting CSVs for NFK Join ---")
    all_rows = []
    
    # Process first table
    with open(filepath1, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter='|')
        for row in reader:
            join_key = row[key1].strip()
            payload = "|".join(row[col].strip() for col in payload1_cols)
            all_rows.append(f"{join_key}|{payload}|0\n")
    
    len_table1 = len(all_rows)

    # Process second table
    with open(filepath2, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter='|')
        for row in reader:
            join_key = row[key2].strip()
            payload = "|".join(row[col].strip() for col in payload2_cols)
            all_rows.append(f"{join_key}|{payload}|1\n")
            
    len_table2 = len(all_rows) - len_table1

    # Write the formatted output file
    with open(output_path, "w") as outfile:
        # The C program expects a header line with the counts of rows from each table
        outfile.write(f"{len_table1} {len_table2}\n")
        outfile.writelines(all_rows)

    print(f"Formatting complete. {len(all_rows)} total rows written to {output_path}.")

def main():
    parser = argparse.ArgumentParser(description="Formats two CSVs for an NFK join.")
    parser.add_argument("--filepath1", required=True)
    parser.add_argument("--key1", required=True)
    parser.add_argument("--payload1_cols", nargs='*', required=False, default=[])
    parser.add_argument("--filepath2", required=True)
    parser.add_argument("--key2", required=True)
    parser.add_argument("--payload2_cols", nargs='*', required=False, default=[])
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()
    
    format_nfk_join(
        args.filepath1, args.key1, args.payload1_cols,
        args.filepath2, args.key2, args.payload2_cols,
        args.output_path
    )

if __name__ == "__main__":
    main()
    