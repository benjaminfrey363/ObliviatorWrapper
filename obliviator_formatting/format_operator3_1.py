# obliviator_formatting/format_operator3_1.py

import argparse
import pandas as pd # Using pandas for easy CSV column selection

def format_operator3_1(filepath: str, output_path: str, filter_key_col: str, id_col: str):
    """
    Formats a generic CSV input file for Obliviator's 'operator_3/3_1' (Filter/Projection).
    It extracts values from a specified 'filter_key_col' and 'id_col' to form
    the input for obliviator as '<filter_key_value> <id_value>'.

    Args:
        filepath (str): Path to the input CSV file.
        output_path (str): Path to the output formatted file for relabeling.
        filter_key_col (str): The name of the column whose values will act as the filter key.
        id_col (str): The name of the column whose values will act as the ID for reconstruction.
    """
    # Read the CSV file
    df = pd.read_csv(filepath)

    # Validate that specified columns exist
    if filter_key_col not in df.columns:
        raise ValueError(f"Filter key column '{filter_key_col}' not found in input CSV.")
    if id_col not in df.columns:
        raise ValueError(f"ID column '{id_col}' not found in input CSV.")

    lines_to_process = []
    # For each row, construct the '<filter_key_value> <id_value>' string
    for index, row in df.iterrows():
        filter_val = str(row[filter_key_col])
        id_val = str(row[id_col])
        
        lines_to_process.append(f"{filter_val} {id_val}")

    # The header for relabel_ids format: num_rows for table1, 0 for table2
    header_output = f"{len(lines_to_process)} {0}"
    
    with open(output_path, "w") as outfile:
        outfile.write("\n".join([header_output] + lines_to_process))
    print(f"Formatted input for Operator 3, Step 1 written to {output_path}.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath", required=True, help="Path to the input CSV file.")
    parser.add_argument("--output_path", required=True, help="Path to the output formatted file.")
    parser.add_argument("--filter_key_col", required=True, help="Name of the column to use as the filter key.")
    parser.add_argument("--id_col", required=True, help="Name of the column to use as the ID for reconstruction.")
    args = parser.parse_args()
    format_operator3_1(args.filepath, args.output_path, args.filter_key_col, args.id_col)

if __name__ == "__main__":
    main()

