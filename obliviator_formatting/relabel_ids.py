# obliviator_formatting/relabel_ids.py (Flexible Relabeling)

import argparse

id_map = {}
next_id = 0

def get_or_assign_id(original_id_str):
    global next_id
    if original_id_str not in id_map:
        id_map[original_id_str] = next_id
        next_id += 1
    return str(id_map[original_id_str]) # Ensure string output


def relabel_ids ( input_path , output_path , mapping_path, key_index_to_relabel: int = 0 ):
    """
    Relabels IDs in the input file.
    Args:
        input_path (str): Path to the input file.
        output_path (str): Path to the output relabeled file.
        mapping_path (str): Path to the mapping file (original_id -> relabeled_id).
        key_index_to_relabel (int): 0-indexed position of the column to relabel.
                                    If -1, no columns are relabeled, but a map is still built.
    """
    global id_map, next_id
    id_map = {} # Reset map for each run
    next_id = 0 # Reset counter for each run

    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        # Write the header line unmodified
        header = infile.readline()
        outfile.write(header)
        
        for line in infile:
            parts = line.strip().split(maxsplit=1) # Split only on the first space (assuming ID VALUE format)

            if len(parts) == 2:
                original_key_str, original_value_str = parts
                output_parts = [original_key_str, original_value_str] # Default to no change

                # Relabel the specified key if index is valid
                if key_index_to_relabel == 0:
                    output_parts[0] = get_or_assign_id(original_key_str)
                elif key_index_to_relabel == 1: # If relabeling the second part (value)
                    output_parts[1] = get_or_assign_id(original_value_str)
                elif key_index_to_relabel != -1:
                    print(f"Warning: relabel_ids: key_index_to_relabel {key_index_to_relabel} not supported for 2-column input. No relabeling performed for this line.")

                outfile.write(f"{output_parts[0]} {output_parts[1]}\n")
            else:
                # For non-2-column inputs (like headers or malformed lines), just write as-is
                outfile.write(line)
                print(f"Warning: relabel_ids encountered unexpected line format (not 2 columns): {line.strip()}")

    # Write mapping file: mapped_id original_id
    with open(mapping_path, "w") as f:
        for original, mapped in id_map.items():
            f.write(f"{mapped} {original}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    parser.add_argument("--key_index_to_relabel", type=int, default=0,
                        help="0-indexed position of the column to relabel (0 for first, 1 for second). Use -1 for no relabeling.")
    args = parser.parse_args()
    relabel_ids(args.input_path, args.output_path, args.mapping_path, args.key_index_to_relabel)


if __name__ == "__main__":
    main()

