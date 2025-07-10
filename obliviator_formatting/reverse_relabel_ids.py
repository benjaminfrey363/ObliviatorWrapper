# obliviator_formatting/reverse_relabel_ids.py

import argparse

def reverse_relabel_ids(input_path, output_path, mapping_path):
    """
    Reverse-relabels IDs in the input file based on a mapping.
    This version is now smarter about delimiters and handles the specific
    output formats for each operator.
    """
    reverse_map = {}
    try:
        # The mapping file is pipe-delimited for robustness.
        with open(mapping_path, "r", encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|', 1)
                if len(parts) == 2:
                    mapped, original = parts
                    reverse_map[mapped] = original
    except FileNotFoundError:
        print(f"Error: Mapping file not found at {mapping_path}")
        raise

    with open(input_path, "r", encoding='utf-8') as infile, open(output_path, "w") as outfile:
        for line in infile:
            # --- FIX: Check for pipe delimiter to identify join output ---
            if '|' in line:
                # This is a JOIN result, which is pipe-delimited.
                parts = line.strip().split('|')
                if len(parts) == 3:
                    key_id, p1_id, p2_id = parts
                    
                    original_key = reverse_map.get(key_id, f"UNMAPPED_{key_id}")
                    original_p1 = reverse_map.get(p1_id, f"UNMAPPED_{p1_id}")
                    original_p2 = reverse_map.get(p2_id, f"UNMAPPED_{p2_id}")
                    
                    # Write the intermediate file in the format reconstruct_fk_join_csv.py expects
                    outfile.write(f"{original_key}|{original_p1}|{original_p2}\n")
                else:
                    print(f"Warning: Skipping malformed join line in raw C output: '{line.strip()}'")

            else:
                # This is a FILTER result, which is space-delimited.
                parts = line.strip().split()
                if len(parts) == 2:
                    key_id, p_id = parts
                    original_key = reverse_map.get(key_id, f"UNMAPPED_{key_id}")
                    original_p = reverse_map.get(p_id, f"UNMAPPED_{p_id}")
                    outfile.write(f"{original_key} {original_p}\n")
                else:
                    print(f"Warning: Skipping malformed filter line in raw C output: '{line.strip()}'")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    args = parser.parse_args()
    reverse_relabel_ids(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()
