# obliviator_formatting/relabel_fk_join.py

import argparse
from pathlib import Path

def relabel_for_fk_join(input_path: str, output_path: str, mapping_path: str):
    """
    Relabels data specifically for the FK Join operator.

    - Reads the formatted file and creates a global mapping for all unique strings.
    - Writes a new file where keys/payloads are replaced by integer IDs.
    - Writes a robust, pipe-delimited mapping file.
    """
    value_map = {}
    next_mapped_id = 0

    def get_or_assign_id(original_value: str) -> int:
        nonlocal next_mapped_id
        if original_value not in value_map:
            value_map[original_value] = next_mapped_id
            next_mapped_id += 1
        return value_map[original_value]

    print(f"--- Running FK Join Relabeling ---")
    
    lines_to_write = []
    header = ""
    with open(input_path, "r", encoding='utf-8') as infile:
        header = infile.readline() # Read and preserve the header
        
        for line in infile:
            parts = line.strip().split(maxsplit=1)
            if len(parts) != 2:
                print(f"Warning: Skipping malformed line in {input_path}: {line.strip()}")
                continue
            
            original_key, original_payload = parts
            
            mapped_key_id = get_or_assign_id(original_key)
            mapped_payload_id = get_or_assign_id(original_payload)
            
            lines_to_write.append(f"{mapped_key_id} {mapped_payload_id}\n")

    # Write the relabeled output file for the C program
    with open(output_path, 'w', encoding='utf-8') as outfile:
        if header:
            outfile.write(header)
        outfile.writelines(lines_to_write)

    # --- FIX: Write the mapping file using a pipe delimiter ---
    # This ensures that complex payloads with spaces are handled correctly.
    with open(mapping_path, "w", encoding='utf-8') as map_file:
        for original_val, mapped_id in value_map.items():
            map_file.write(f"{mapped_id}|{original_val}\n")
    
    print("FK Join relabeling complete.")


def main():
    parser = argparse.ArgumentParser(description="Relabel script tailored for Obliviator FK Join.")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    args = parser.parse_args()
    relabel_for_fk_join(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()
