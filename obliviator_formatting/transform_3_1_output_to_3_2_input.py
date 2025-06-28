# obliviator_formatting/transform_3_1_output_to_3_2_input.py (Generic Two-Table Join Input)

import argparse
import pandas as pd

def transform_3_1_output_to_3_2_input(
    original_csv_filepath: str,
    step1_filtered_ids_filepath: str,
    output_path: str,
    id_col_in_original_csv: str,
    join_key_col_3_2_A: str, # Column from original_csv_filepath to use as JOIN_KEY_A (for Table 1 data)
    join_key_col_3_2_B_and_values: str, # Columns from original_csv_filepath for JOIN_VALUE_A (for Table 1 data)
    second_table_filepath: str, # Path to the second table's source CSV
    second_table_key_col: str, # Column from second_table_filepath to use as JOIN_KEY_B (for Table 2 data)
    second_table_other_cols: str # Columns from second_table_filepath for JOIN_VALUE_B (for Table 2 data)
):
    """
    Transforms the filtered output of Operator 3, Step 1 (using original CSV)
    and a second input CSV into the two-table concatenated input format for Operator 3, Step 2.

    Table 1 data is derived from original_csv_filepath, filtered by step1_filtered_ids_filepath.
    Table 2 data is derived from second_table_filepath.
    """
    # --- Prepare Table 1 Data (from original_csv_filepath filtered by Step 1 output) ---
    print("Preparing Table 1 data for Operator 3, Step 2...")
    filtered_ids = set()
    with open(step1_filtered_ids_filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                filtered_ids.add(line)
    
    if not filtered_ids:
        print("Warning: Step 3_1 filter yielded no results. Table 1 for Step 3_2 will be empty.")

    df_original = pd.read_csv(original_csv_filepath)

    all_req_cols_table1 = [id_col_in_original_csv, join_key_col_3_2_A] + [c.strip() for c in join_key_col_3_2_B_and_values.split(',')]
    missing_cols_table1 = [col for col in all_req_cols_table1 if col not in df_original.columns]
    if missing_cols_table1:
        raise ValueError(f"Missing required columns in original CSV for Table 1 (Step 3_2 transformation): {', '.join(missing_cols_table1)}")

    df_filtered_table1 = df_original[df_original[id_col_in_original_csv].astype(str).isin(filtered_ids)].copy()

    lines_table1 = []
    cols_for_table1_value = [col.strip() for col in join_key_col_3_2_B_and_values.split(',')]

    for index, row in df_filtered_table1.iterrows():
        # Key for Table 1 will be join_key_col_3_2_A
        key_value_table1 = str(row[join_key_col_3_2_A])
        
        # Value for Table 1 will be concatenation of other specified columns
        value_parts_table1 = [str(row[col]) for col in cols_for_table1_value]
        value_str_table1 = ','.join(value_parts_table1)
        lines_table1.append(f"{key_value_table1} {value_str_table1}")

    # --- Prepare Table 2 Data (from second_table_filepath) ---
    print(f"Preparing Table 2 data from {second_table_filepath} for Operator 3, Step 2...")
    df_second_table = pd.read_csv(second_table_filepath)

    all_req_cols_table2 = [second_table_key_col] + [c.strip() for c in second_table_other_cols.split(',')]
    missing_cols_table2 = [col for col in all_req_cols_table2 if col not in df_second_table.columns]
    if missing_cols_table2:
        raise ValueError(f"Missing required columns in second table CSV for Table 2 (Step 3_2 transformation): {', '.join(missing_cols_table2)}")

    lines_table2 = []
    cols_for_table2_value = [col.strip() for col in second_table_other_cols.split(',')]

    for index, row in df_second_table.iterrows():
        # Key for Table 2 will be second_table_key_col
        key_value_table2 = str(row[second_table_key_col])
        
        # Value for Table 2 will be concatenation of other specified columns
        value_parts_table2 = [str(row[col]) for col in cols_for_table2_value]
        value_str_table2 = ','.join(value_parts_table2)
        lines_table2.append(f"{key_value_table2} {value_str_table2}")
    
    # --- Concatenate and Write to Output ---
    header_output = f"{len(lines_table1)} {len(lines_table2)}"
    
    all_lines = [header_output] + lines_table1 + lines_table2

    with open(output_path, "w") as outfile:
        outfile.write("\n".join(all_lines))
    print(f"Transformed and concatenated two-table input for Operator 3, Step 2 written to {output_path}.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--original_csv_filepath", required=True)
    parser.add_argument("--step1_filtered_ids_filepath", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--id_col_in_original_csv", required=True)
    parser.add_argument("--join_key_col_3_2_A", required=True)
    parser.add_argument("--join_key_col_3_2_B_and_values", required=True)
    parser.add_argument("--second_table_filepath", required=True) # Renamed and now required
    parser.add_argument("--second_table_key_col", required=True)
    parser.add_argument("--second_table_other_cols", required=True)
    args = parser.parse_args()
    transform_3_1_output_to_3_2_input(
        args.original_csv_filepath,
        args.step1_filtered_ids_filepath,
        args.output_path,
        args.id_col_in_original_csv,
        args.join_key_col_3_2_A,
        args.join_key_col_3_2_B_and_values,
        args.second_table_filepath,
        args.second_table_key_col,
        args.second_table_other_cols
    )

if __name__ == "__main__":
    main()
