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
            parts = line.strip().split()
            
            # Dynamically determine if it's 2-column or 4-column output
            if len(parts) == 2:
                # Handle 2-column output (e.g., from join_kks)
                a_str, b_str = parts
                a_orig = reverse_map.get(a_str, a_str)
                b_orig = reverse_map.get(b_str, b_str)
                outfile.write(f"{a_orig} {b_orig}\n")
            elif len(parts) == 4:
                # Handle 4-column output (e.g., from fk_join)
                a_str, b_str, c_str, d_str = parts
                a_orig = reverse_map.get(a_str, a_str)
                b_orig = reverse_map.get(b_str, b_str)
                c_orig = reverse_map.get(c_str, c_str)
                d_orig = reverse_map.get(d_str, d_str)
                outfile.write(f"{a_orig} {b_orig} {c_orig} {d_orig}\n")
            else:
                # If unexpected number of columns, write original line and log error
                print(f"Warning: Unexpected number of columns ({len(parts)}) in line: {line.strip()}")
                outfile.write(line) # Write original line if format is unexpected


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path")
    parser.add_argument("--output_path")
    parser.add_argument("--mapping_path")
    args = parser.parse_args()
    reverse_relabel_ids(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()

