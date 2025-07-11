import argparse

def reverse_relabel_nfk_join(input_path: str, output_path: str, mapping_path: str):
    """
    Reverses the relabeling for NFK join output.
    Input from C is `key_id1 payload1_id key_id2 payload2_id`.
    Output is `key_str|payload1_str|payload2_str`.
    """
    print("--- Running NFK Join Reverse Relabeling ---")
    reverse_map = {}
    with open(mapping_path, "r") as map_file:
        for line in map_file:
            # Use maxsplit=1 in case the payload itself contains a pipe
            uid, value = line.strip().split('|', 1)
            reverse_map[uid] = value

    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        for line in infile:
            parts = line.strip().split()
            # Expect the 4-column format from the C program
            if len(parts) != 4:
                continue
            
            key_id1, p1_id, key_id2, p2_id = parts
            
            # The join keys (key_id1 and key_id2) should be the same.
            # We only need to look up one of them.
            key_str = reverse_map.get(key_id1, f"UNMAPPED_{key_id1}")
            p1_str = reverse_map.get(p1_id, f"UNMAPPED_{p1_id}")
            p2_str = reverse_map.get(p2_id, f"UNMAPPED_{p2_id}")
            
            outfile.write(f"{key_str}|{p1_str}|{p2_str}\n")

    print("Reverse relabeling complete.")

def main():
    parser = argparse.ArgumentParser(description="Reverse relabeling for NFK join.")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    args = parser.parse_args()
    reverse_relabel_nfk_join(args.input_path, args.output_path, args.mapping_path)

if __name__ == "__main__":
    main()
