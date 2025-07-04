# obliviator_formatting/relabel_operator2.py

import argparse
from pathlib import Path

def relabel_for_operator2(input_path: str, output_path: str, mapping_path: str):
    """
    Relabels data for the Aggregation operator.
    - Input format: <group_key> <numeric_agg_value> <payload>
    - It relabels the `group_key` and `payload` to integers.
    - It passes the `numeric_agg_value` through unchanged.
    - Output format: <mapped_key> <numeric_agg_value> <mapped_payload>
    """
    value_map = {}
    next_mapped_id = 0

    def get_or_assign_id(original_value: str) -> int:
        nonlocal next_mapped_id
        if original_value not in value_map:
            value_map[original_value] = next_mapped_id
            next_mapped_id += 1
        return value_map[original_value]

    print(f"--- Running Aggregation (Operator 2) Relabeling ---")
    
    lines_to_write = []
    header = ""
    with open(input_path, "r", encoding='utf-8') as infile:
        header = infile.readline()
        
        for line in infile:
            parts = line.strip().split(maxsplit=2)
            if len(parts) != 3:
                continue
            
            group_key, agg_value, payload = parts
            
            mapped_key_id = get_or_assign_id(group_key)
            mapped_payload_id = get_or_assign_id(payload)
            
            lines_to_write.append(f"{mapped_key_id} {agg_value} {mapped_payload_id}\n")

    with open(output_path, 'w', encoding='utf-8') as outfile:
        if header:
            outfile.write(header)
        outfile.writelines(lines_to_write)

    with open(mapping_path, "w", encoding='utf-8') as map_file:
        for original_val, mapped_id in value_map.items():
            map_file.write(f"{mapped_id} {original_val}\n")
    
    print("Aggregation relabeling complete.")


def main():
    parser = argparse.ArgumentParser(description="Relabel script for Obliviator Aggregation.")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    args = parser.parse_args()
    relabel_for_operator2(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()
