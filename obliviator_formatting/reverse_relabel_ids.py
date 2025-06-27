# reverse_relabel_ids.py

import argparse

def reverse_relabel_ids ( input_path, output_path, mapping_path ):
    # Build reverse mapping: int_id (as str) -> original_id (as str)
    reverse_map = {}
    with open(mapping_path, "r") as f:
        for line in f:
            mapped, original = line.strip().split()
            reverse_map[mapped] = original

    # Apply reverse mapping to output
    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        for line in infile:
            a, b = line.strip().split()
            a_orig = reverse_map.get(a, a)
            b_orig = reverse_map.get(b, b)
            outfile.write(f"{a_orig} {b_orig}\n")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path")
    parser.add_argument("--output_path")
    parser.add_argument("--mapping_path")
    args = parser.parse_args()
    reverse_relabel_ids(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()
