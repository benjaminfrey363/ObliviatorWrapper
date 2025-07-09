# obliviator_formatting/reverse_relabel_ids.py

import argparse

def reverse_relabel_ids(input_path, output_path, mapping_path):
    """
    Reverse-relabels IDs in the input file based on a mapping.
    This version correctly handles both join and filter outputs.
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
            # The C program's output is always space-delimited.
            parts = line.strip().split()
            
            # A join result will have 4 parts: key_t2_id p_t2_id key_t1_id p_t1_id
            if len(parts) == 4:
                key_t2_id, p_t2_id, key_t1_id, p_t1_id = parts
                
                original_key = reverse_map.get(key_t1_id, f"UNMAPPED_{key_t1_id}")
                original_p1 = reverse_map.get(p_t1_id, f"UNMAPPED_{p_t1_id}")
                original_p2 = reverse_map.get(p_t2_id, f"UNMAPPED_{p_t2_id}")
                
                # Write the intermediate file in the pipe-delimited format that
                # reconstruct_fk_join_csv.py expects.
                outfile.write(f"{original_key}|{original_p1}|{original_p2}\n")

            # A filter result will have 2 parts: key_id payload_id
            elif len(parts) == 2:
                key_id, p_id = parts
                original_key = reverse_map.get(key_id, f"UNMAPPED_{key_id}")
                original_p = reverse_map.get(p_id, f"UNMAPPED_{p_id}")
                # The reconstruction script for the filter expects space-separated output
                outfile.write(f"{original_key} {original_p}\n")
            
            else:
                print(f"Warning: Skipping malformed line in raw C output: '{line.strip()}'")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    args = parser.parse_args()
    reverse_relabel_ids(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()
