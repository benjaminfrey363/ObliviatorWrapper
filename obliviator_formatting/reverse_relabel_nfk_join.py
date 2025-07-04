# obliviator_formatting/reverse_relabel_nfk_join.py

import argparse

def reverse_relabel_nfk_join(input_path, output_path, mapping_path):
    """
    Reverse-relabels IDs from the NFK Join C program's output.

    This script correctly parses the NFK-specific output format:
    `mapped_key1 mapped_payload1 mapped_key2 mapped_payload2`
    """
    reverse_map = {}
    with open(mapping_path, "r", encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split(maxsplit=1)
            if len(parts) == 2:
                mapped, original = parts
                reverse_map[mapped] = original
    
    with open(input_path, "r", encoding='utf-8') as infile, open(output_path, "w", encoding='utf-8') as outfile:
        for line in infile:
            parts = line.strip().split()
            
            if len(parts) == 4:
                key1_id, p1_id, key2_id, p2_id = parts
                
                # In a join, key1 and key2 should be the same. We use key1 as the canonical key.
                original_key = reverse_map.get(key1_id, f"UNMAPPED_{key1_id}")
                original_p1 = reverse_map.get(p1_id, f"UNMAPPED_{p1_id}") # Payload from Table 1
                original_p2 = reverse_map.get(p2_id, f"UNMAPPED_{p2_id}") # Payload from Table 2
                
                # Write the intermediate file in the logical order: key|payload1|payload2
                outfile.write(f"{original_key}|{original_p1}|{original_p2}\n")
            else:
                print(f"Warning: Skipping malformed line in NFK raw output: '{line.strip()}'")


def main():
    parser = argparse.ArgumentParser(description="Reverse relabel script for NFK Join.")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    args = parser.parse_args()
    reverse_relabel_nfk_join(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()
