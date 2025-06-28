# obliviator_formatting/format_operator2.py

import argparse

def format_operator2(filepath: str, output_path: str):
    """
    Formats a single input file for Obliviator's 'operator_2'.
    Assumes input lines are: <ID> <Float> <Rest_of_comma_separated_string>
    The output format will be:
    <num_rows>
    <original_ID> <entire_rest_of_line_after_first_ID>
    """
    lines_to_process = []
    
    with open(filepath, "r") as infile:
        # Read and potentially ignore the first line (header)
        header_line = infile.readline().strip()
        if not header_line.isdigit() and header_line:
            print(f"Warning: format_operator2: Expected first line to be a number (row count), got '{header_line}'.")

        # Read and potentially ignore the second line (blank line)
        second_line = infile.readline().strip()
        if second_line:
            # If it's not empty, it might be a data line. Rewind if needed.
            # For simplicity with `head test.txt` format, we assume first two lines are header and blank.
            pass

        for line_num, line in enumerate(infile): # line_num will be 0-indexed for actual data lines
            line = line.strip()
            if not line: # Skip empty lines
                continue

            try:
                # Find the first space to separate ID from the rest of the line content
                first_space_idx = line.find(' ')
                if first_space_idx == -1:
                    print(f"Warning: Line (after headers) {line_num+1}: '{line}' does not contain expected space separator for ID. Skipping.")
                    continue
                
                original_id = line[:first_space_idx]
                rest_of_line_content = line[first_space_idx+1:]

                # For relabel_ids.py, we need two parts: key and value.
                # Key: original_id (so we can reverse relabel it later).
                # Value: the entire rest of the line, as a single string. This preserves all original data
                # for obliviator, as its output implies it processes multiple fields from this.
                lines_to_process.append(f"{original_id} {rest_of_line_content}")

            except Exception as e:
                print(f"Error parsing line (after headers) {line_num+1}: '{line}': {e}. Skipping.")
                continue

    # Header for relabel_ids: number of rows in this table, 0 for second table
    header_output = f"{len(lines_to_process)} {0}"
    
    with open(output_path, "w") as outfile:
        outfile.write("\n".join([header_output] + lines_to_process))
    print(f"Formatted input for Operator 2 written to {output_path}.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath")
    parser.add_argument("--output_path")
    args = parser.parse_args()
    format_operator2(args.filepath, args.output_path)

if __name__ == "__main__":
    main()

    