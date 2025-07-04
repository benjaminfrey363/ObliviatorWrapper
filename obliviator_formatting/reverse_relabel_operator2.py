# obliviator_formatting/reverse_relabel_operator2.py

import argparse

def reverse_relabel_for_operator2(input_path: str, output_path: str, mapping_path: str):
    """
    Reverse-relabels IDs from the Aggregation (Operator 2) C program's output.

    This script correctly parses the aggregation-specific output format:
    `<mapped_key> <agg_val_1> <agg_val_2> <mapped_payload>`
    
    It writes an intermediate file in the format:
    `<original_key>|<agg_val_1>|<agg_val_2>|<original_payload>`
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
            parts = line.strip().split(maxsplit=3)
            
            if len(parts) == 4:
                key_id, agg1, agg2, payload_id = parts
                
                original_key = reverse_map.get(key_id, f"UNMAPPED_{key_id}")
                original_payload = reverse_map.get(payload_id, f"UNMAPPED_{payload_id}")
                
                # Write the intermediate file using a pipe delimiter
                outfile.write(f"{original_key}|{agg1}|{agg2}|{original_payload}\n")
            else:
                print(f"Warning: Skipping malformed line in raw aggregation output: '{line.strip()}'")


def main():
    parser = argparse.ArgumentParser(description="Reverse relabel script for Aggregation (Operator 2).")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    args = parser.parse_args()
    reverse_relabel_for_operator2(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()
