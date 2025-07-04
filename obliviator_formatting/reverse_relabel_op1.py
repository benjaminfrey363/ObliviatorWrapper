# obliviator_formatting/reverse_relabel_op1.py

import argparse
from pathlib import Path

def reverse_relabel_for_operator1(input_path: str, output_path: str, mapping_path: str):
    """
    Reverses the relabeling process for Operator 1's output.

    - Reads the C program's output, which consists of (original_id, mapped_value_id).
    - Uses the mapping file to convert mapped_value_id back to the original string value.
    - Writes the final output as (original_id, original_string_value).

    Args:
        input_path (str): Path to the raw output from the C program.
        output_path (str): Path for the final, human-readable output file.
        mapping_path (str): Path to the value mapping file.
    """
    reverse_map = {}
    print(f"--- Running Operator 1 Reverse Relabeling ---")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Mapping: {mapping_path}")

    with open(mapping_path, "r") as map_file:
        for line in map_file:
            parts = line.strip().split(maxsplit=1)
            if len(parts) == 2:
                mapped_id, original_value = parts
                reverse_map[mapped_id] = original_value

    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        for line in infile:
            parts = line.strip().split()
            if len(parts) != 2:
                print(f"Warning: Skipping malformed line in C output: {line.strip()}")
                continue
            
            original_id, mapped_value_id = parts

            # Look up the original string value, defaulting to a placeholder if not found
            original_value = reverse_map.get(mapped_value_id, f"UNMAPPED_ID_{mapped_value_id}")

            outfile.write(f"{original_id} {original_value}\n")

    print("Operator 1 reverse relabeling complete.")


def main():
    parser = argparse.ArgumentParser(description="Reverse relabeling script for Obliviator Operator 1.")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    args = parser.parse_args()
    reverse_relabel_for_operator1(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()
