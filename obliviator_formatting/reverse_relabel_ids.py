# reverse_relabel_ids.py (Fix FK Join 4-Column Relabeling)

import argparse

def reverse_relabel_ids ( input_path, output_path, mapping_path, key_index_to_relabel: int = 0 ):
    """
    Reverse-relabels IDs in the input file based on a mapping.
    Args:
        input_path (str): Path to the input file (e.g., obliviator raw output).
        output_path (str): Path to the output reverse-relabeled file.
        mapping_path (str): Path to the mapping file (relabeled_id -> original_id).
        key_index_to_relabel (int): 0-indexed position of the column to reverse-relabel.
                                    If -1, no columns are relabeled.
    """
    # Build reverse mapping: int_id (as str) -> original_id (as str)
    reverse_map = {}
    with open(mapping_path, "r") as f:
        for line in f:
            # The mapping file itself is `mapped original`. Use maxsplit=1 for robustness.
            parts = line.strip().split(maxsplit=1)
            if len(parts) == 2:
                mapped, original = parts
                reverse_map[mapped] = original
            else:
                print(f"Warning: Mapping file line malformed: '{line.strip()}'. Skipping.")
    
    # print(f"DEBUG reverse_relabel: Loaded mapping: {reverse_map}") # DEBUG PRINT (uncomment if needed)

    # Apply reverse mapping to output
    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        for line_num, line in enumerate(infile):
            # Use split() without maxsplit to allow multiple spaces between columns.
            # Then check len(parts) for 2, 3, or 4 columns.
            parts = line.strip().split() 

            # print(f"DEBUG reverse_relabel: Processing line {line_num+1}: '{line.strip()}', split into {len(parts)} parts: {parts}") # DEBUG PRINT

            if len(parts) == 2:
                # Handle 2-column output (e.g., from join_kks or Operator 1)
                col1_str, col2_str = parts
                
                output_cols = [col1_str, col2_str]
                # Always attempt to relabel both columns if they are in the map.
                output_cols[0] = reverse_map.get(col1_str, col1_str)
                output_cols[1] = reverse_map.get(col2_str, col2_str)
                
                outfile.write(f"{output_cols[0]} {output_cols[1]}\n")

            elif len(parts) == 3:
                # Handle 3-column output (e.g., from Operator 3 aggregation)
                col1_str, col2_str, col3_str = parts

                output_cols = [col1_str, col2_str, col3_str]
                # Always attempt to relabel all columns if they are in the map.
                output_cols[0] = reverse_map.get(col1_str, col1_str)
                output_cols[1] = reverse_map.get(col2_str, col2_str)
                output_cols[2] = reverse_map.get(col3_str, col3_str)
                
                outfile.write(f"{output_cols[0]} {output_cols[1]} {output_cols[2]}\n")

            elif len(parts) == 4:
                # CRITICAL FIX: Handle 4-column output (e.g., from fk_join)
                # Attempt to reverse-relabel ALL four columns if their values are in the map.
                col1_str, col2_str, col3_str, col4_str = parts
                
                output_cols = [
                    reverse_map.get(col1_str, col1_str),
                    reverse_map.get(col2_str, col2_str),
                    reverse_map.get(col3_str, col3_str),
                    reverse_map.get(col4_str, col4_str)
                ]
                
                outfile.write(f"{output_cols[0]} {output_cols[1]} {output_cols[2]} {output_cols[3]}\n")

            else:
                # If unexpected number of columns, write original line and log error
                print(f"Warning: Line {line_num+1}: Unexpected number of columns ({len(parts)}) in line: '{line.strip()}'. Writing as-is.")
                outfile.write(line + "\n") # Ensure newline is present if original was stripped


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    parser.add_argument("--key_index_to_relabel", type=int, default=0, # This arg is now less critical
                        help="0-indexed position of the column to reverse-relabel (primarily for backward compat).")
    args = parser.parse_args()
    reverse_relabel_ids(args.input_path, args.output_path, args.mapping_path, args.key_index_to_relabel)


if __name__ == "__main__":
    main()

