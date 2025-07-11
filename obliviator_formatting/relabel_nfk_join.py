import argparse

def relabel_nfk_join(input_path: str, output_path: str, mapping_path: str):
    """
    Relabels all unique values (keys and payloads) to unique integer IDs.
    The output format for the C program is: `key_id payload_id`
    """
    print("--- Running NFK Join Relabeling ---")
    value_map = {}
    next_id = 0

    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        # Pass the header (e.g., "512 512") directly to the C program
        header = infile.readline()
        outfile.write(header)

        for line in infile:
            # The format from the previous step is key|payload|table_id
            key, payload, table_id = line.strip().split('|')
            
            # Map the key string to an integer ID
            if key not in value_map:
                value_map[key] = next_id
                next_id += 1
            
            # Map the payload string to an integer ID
            if payload not in value_map:
                value_map[payload] = next_id
                next_id += 1
                
            key_id = value_map[key]
            payload_id = value_map[payload]
            
            # Write the data in the 2-column format required by the C program.
            # The table_id is implicit in the row order.
            outfile.write(f"{key_id} {payload_id}\n")

    # Write the mapping file: id|original_value
    with open(mapping_path, "w") as map_file:
        for value, uid in value_map.items():
            map_file.write(f"{uid}|{value}\n")
    
    print("NFK Join relabeling complete.")

def main():
    parser = argparse.ArgumentParser(description="Relabel script for NFK joins.")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    args = parser.parse_args()
    relabel_nfk_join(args.input_path, args.output_path, args.mapping_path)

if __name__ == "__main__":
    main()
    