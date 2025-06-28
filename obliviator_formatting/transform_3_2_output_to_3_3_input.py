# obliviator_formatting/transform_3_2_output_to_3_3_input.py (Revised for 3-Column Aggregation Input)

import argparse

def transform_3_2_output_to_3_3_input(
    step2_raw_output_filepath: str, # Input is the raw output from obliviator_3_2
    output_path: str,
    # These args define how to extract the 3 columns for obliviator_3_3 input
    # Each refers to a part of the `Table1_Value` or `Table2_Value` strings from step2 output
    col1_from_step2_output: str, # e.g., "table1_id_part", "table2_quantity_part"
    col2_from_step2_output: str, # e.g., "table1_numeric_value", "table2_id_part"
    col3_from_step2_output: str  # e.g., "table2_numeric_value"
):
    """
    Transforms the output of Operator 3, Step 2 into the 3-column input format for Operator 3, Step 3 (Aggregate).
    The output format for obliviator 3_3 is assumed to be:
    <num_rows>
    <col1_value> <col2_value> <col3_value>
    """
    lines_to_process = []
    
    with open(step2_raw_output_filepath, 'r') as infile:
        for line_num, line in enumerate(infile):
            line = line.strip()
            if not line: continue

            try:
                # Example input line from step2 output: R2,20.0 ORD001,Laptop,1
                parts_split_by_space = line.split(' ', 1) # Split into "R2,20.0" and "ORD001,Laptop,1"
                if len(parts_split_by_space) != 2:
                    print(f"Warning: Step 2 output line {line_num+1} malformed (not 2 parts after first space): '{line}'. Skipping.")
                    continue
                
                table1_val_str = parts_split_by_space[0] # e.g., "R2,20.0"
                table2_val_str = parts_split_by_space[1] # e.g., "ORD001,Laptop,1"

                # Parse table1_val_str (e.g., R2,20.0) -> id_part, numeric_part
                table1_parts = table1_val_str.split(',')
                t1_id = table1_parts[0] if len(table1_parts) > 0 else ""
                t1_numeric = table1_parts[1] if len(table1_parts) > 1 else ""

                # Parse table2_val_str (e.g., ORD001,Laptop,1) -> id_part, item, quantity
                table2_parts = table2_val_str.split(',')
                t2_id = table2_parts[0] if len(table2_parts) > 0 else ""
                t2_item = table2_parts[1] if len(table2_parts) > 1 else ""
                t2_quantity = table2_parts[2] if len(table2_parts) > 2 else ""

                # --- Extract values based on user arguments (col1, col2, col3) ---
                # This section needs to be precise based on how you define the mapping.
                # For example, if col1_from_step2_output is "t1_id", it maps to t1_id.

                extracted_col_values = {}
                # Define a simple internal mapping of user-friendly names to parsed parts
                # This needs to cover all possible values you might want to extract.
                extracted_col_values["t1_id"] = t1_id
                extracted_col_values["t1_numeric"] = t1_numeric
                extracted_col_values["t2_id"] = t2_id
                extracted_col_values["t2_item"] = t2_item
                extracted_col_values["t2_quantity"] = t2_quantity

                val1 = extracted_col_values.get(col1_from_step2_output, "")
                val2 = extracted_col_values.get(col2_from_step2_output, "")
                val3 = extracted_col_values.get(col3_from_step2_output, "")

                if not (val1 and val2 and val3): # Basic check to ensure all values were found
                     print(f"Warning: Could not extract all required values from line: '{line}'. Mapped values: {val1}, {val2}, {val3}. Skipping.")
                     continue
                
                # Output format for obliviator_3_3: <col1_value> <col2_value> <col3_value>
                lines_to_process.append(f"{val1} {val2} {val3}")

            except Exception as e:
                print(f"Error parsing step 2 output line {line_num+1}: '{line}': {e}. Skipping.")
                continue

    header_output = f"{len(lines_to_process)} {0}" # Still single-table for relabeler
    
    with open(output_path, "w") as outfile:
        outfile.write("\n".join([header_output] + lines_to_process))
    print(f"Transformed data for Operator 3, Step 3 input written to {output_path}.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step2_raw_output_filepath", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--col1_from_step2_output", required=True)
    parser.add_argument("--col2_from_step2_output", required=True)
    parser.add_argument("--col3_from_step2_output", required=True)
    args = parser.parse_args()
    transform_3_2_output_to_3_3_input(
        args.step2_raw_output_filepath,
        args.output_path,
        args.col1_from_step2_output,
        args.col2_from_step2_output,
        args.col3_from_step2_output
    )

if __name__ == "__main__":
    main()

