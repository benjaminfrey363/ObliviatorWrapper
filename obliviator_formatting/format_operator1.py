# obliviator_formatting/format_operator1.py (Pad String to 27 Bytes)

import argparse
import pandas as pd

# elem_t.data size from operator_1/common/elem_t.h
EXPECTED_DATA_FIELD_LENGTH = 27 
# The string length that obliviator_1 projects as output (e.g., 'nbiz' is 4 chars)
PROJECTED_OUTPUT_STRING_LENGTH = 4 

def format_operator1(filepath: str, output_path: str, id_col: str, string_to_project_col: str):
    """
    Formats a generic CSV input file for Obliviator's 'operator_1'.
    It prepares the data to fit elem_t struct: <ID> <padded_string_value>.
    The string_to_project_col value will be padded/truncated to EXPECTED_DATA_FIELD_LENGTH (27 bytes).
    """
    df = pd.read_csv(filepath)

    if id_col not in df.columns:
        raise ValueError(f"ID column '{id_col}' not found in input CSV.")
    if string_to_project_col not in df.columns:
        raise ValueError(f"String to project column '{string_to_project_col}' not found in input CSV.")

    lines_to_process = []
    for index, row in df.iterrows():
        original_id = str(row[id_col])
        raw_string_data = str(row[string_to_project_col])
        
        # Pad or truncate the string to the exact expected length (27 bytes)
        # Fill with spaces or null characters if shorter, truncate if longer.
        # It's safer to pad with spaces and ensure ASCII.
        padded_string_data = (raw_string_data + ' ' * EXPECTED_DATA_FIELD_LENGTH)[:EXPECTED_DATA_FIELD_LENGTH]
        
        lines_to_process.append(f"{original_id} {padded_string_data}")

    header_output = f"{len(lines_to_process)} {0}"
    
    with open(output_path, "w") as outfile:
        outfile.write("\n".join([header_output] + lines_to_process))
    print(f"Formatted input for Operator 1 written to {output_path}.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath", required=True, help="Path to the input CSV file.")
    parser.add_argument("--output_path", required=True, help="Path to the output formatted file.")
    parser.add_argument("--id_col", required=True, help="Column to use as the unique ID.")
    parser.add_argument("--string_to_project_col", required=True, help="Column containing the string to be projected.")
    args = parser.parse_args()
    format_operator1(args.filepath, args.output_path, args.id_col, args.string_to_project_col)

if __name__ == "__main__":
    main()
