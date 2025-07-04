# obliviator_formatting/relabel_op1.py

import argparse
from pathlib import Path

def relabel_for_operator1(input_path: str, output_path: str, mapping_path: str):
    """
    Prepares data specifically for Operator 1 (Filter).

    - The ID (first column) is passed through UNCHANGED for numeric filtering.
    - The Value (second column) is mapped to a new, sequential integer ID.
    - A mapping file is created to allow reversing the value mapping later.

    Args:
        input_path (str): Path to the formatted input file (e.g., op1_format.txt).
        output_path (str): Path for the relabeled file to be fed into the C program.
        mapping_path (str): Path to write the value mapping to (mapped_id -> original_value).
    """
    value_map = {}
    next_mapped_id = 0

    print(f"--- Running Operator 1 Relabeling ---")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Mapping: {mapping_path}")

    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        # The first line of the formatted file is the header (e.g., "10 0")
        # which contains the count of lines. We pass it through.
        header = infile.readline()
        if header:
            outfile.write(header)
        else:
            print("Warning: Input file for relabeling is empty.")
            return

        for line in infile:
            parts = line.strip().split(maxsplit=1)
            if len(parts) != 2:
                print(f"Warning: Skipping malformed line in {input_path}: {line.strip()}")
                continue

            original_id, original_value = parts

            # Get or create a new integer ID for the string value
            if original_value not in value_map:
                value_map[original_value] = next_mapped_id
                next_mapped_id += 1
            
            mapped_value_id = value_map[original_value]

            # Write the output with the ORIGINAL ID and the NEW MAPPED VALUE ID
            outfile.write(f"{original_id} {mapped_value_id}\n")

    # Write the mapping file: mapped_id original_value
    # This will be used to restore the original string after the C program runs.
    with open(mapping_path, "w") as map_file:
        # We need to invert the map for writing
        for original_val, mapped_id in value_map.items():
            map_file.write(f"{mapped_id} {original_val}\n")
    
    print("Operator 1 relabeling complete.")


def main():
    parser = argparse.ArgumentParser(description="Relabel script tailored for Obliviator Operator 1.")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    args = parser.parse_args()
    relabel_for_operator1(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()
