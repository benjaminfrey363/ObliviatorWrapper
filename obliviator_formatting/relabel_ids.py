# relabel_ids.py

import argparse

id_map = {}
next_id = 0

def get_or_assign_id(original_id):
    global next_id
    if original_id not in id_map:
        id_map[original_id] = next_id
        next_id += 1
    return id_map[original_id]


def relabel_ids ( input_path , output_path , mapping_path ):
    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        # Write the header line unmodified
        header = infile.readline()
        outfile.write(header)
        # Process the rest of the lines with remapping
        for line in infile:
            a_str, b_str = line.strip().split()
            a_mapped = get_or_assign_id(a_str)
            b_mapped = get_or_assign_id(b_str)
            outfile.write(f"{a_mapped} {b_mapped}\n")
    # Write mapping file
    with open(mapping_path, "w") as f:
        for original, mapped in id_map.items():
            f.write(f"{mapped} {original}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path")
    parser.add_argument("--output_path")
    parser.add_argument("--mapping_path")
    args = parser.parse_args()
    relabel_ids(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()

