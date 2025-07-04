# obliviator_formatting/reverse_relabel_ids.py

import argparse

def reverse_relabel_ids ( input_path, output_path, mapping_path ):
    """
    Reverse-relabels IDs in the input file based on a mapping.
    This version is now smarter about delimiters and handles the specific
    output formats for each operator.
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
            
            # --- FIX: Correctly parse the 4-column FK Join output ---
            # C++ Format is: key_t2, payload_t2, key_t1, payload_t1
            if len(parts) == 4:
                key_t2_id, p_t2_id, key_t1_id, p_t1_id = parts
                
                # The keys are the same, so we use key_t1 as the canonical key.
                # We need to get the payloads in the correct T1 -> T2 order for reconstruction.
                original_key = reverse_map.get(key_t1_id, f"UNMAPPED_{key_t1_id}")
                original_p1 = reverse_map.get(p_t1_id, f"UNMAPPED_{p_t1_id}") # Payload from Table 1
                original_p2 = reverse_map.get(p_t2_id, f"UNMAPPED_{p_t2_id}") # Payload from Table 2
                
                # Write the intermediate file in the logical order: key|payload1|payload2
                outfile.write(f"{original_key}|{original_p1}|{original_p2}\n")
            
            # Filter op has 2 columns: key, payload
            elif len(parts) == 2:
                key_id, p_id = parts
                original_key = reverse_map.get(key_id, f"UNMAPPED_{key_id}")
                original_p = reverse_map.get(p_id, f"UNMAPPED_{p_id}")
                outfile.write(f"{original_key} {original_p}\n")
            
            else:
                print(f"Warning: Skipping malformed line in raw output: '{line.strip()}'")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--mapping_path", required=True)
    args = parser.parse_args()
    reverse_relabel_ids(args.input_path, args.output_path, args.mapping_path)


if __name__ == "__main__":
    main()
