# obliviator_formatting/format_operator1.py

import argparse

##########################################
# Format data for an operator 1 - filter #
##########################################

# To perform a filter, obliviator expects data to be passed in the form
#  <num_rows>
#
# <filter_key[0]> <content[0]>
# ...             ...
#
# where filter_key[i] is the key on which filter is applied, and content[i]
# is any set of contexts associated with this entry, for practical use an ID.
# 
# Output of form
#
# <filtered_key[0]> <first 4 digits of content[0]>
# ...               ...
#
# As filter cuts all but the first four digits of content, IDs are relabeled
# to be within range.


def format_operator1(filepath: str, output_path: str):
    """
    Formats a single input file for Obliviator's 'operator_1'.
    Assumes input lines are: <ID> <long_string>,<ID_again>,<number>
    The output format will be:
    <num_rows>
    <ID> <long_string_part1> (where long_string_part1 is the string before the first comma)
    """
    lines_to_process = []
    # Store original lines to extract the desired projected string part after relabeling
    original_line_parts_map = {}

    with open(filepath, "r") as infile:
        # Read the first line (expected to be the count header)
        num_rows_header = infile.readline().strip()
        if not num_rows_header.isdigit():
            print(f"Warning: Expected first line to be a number (row count), got '{num_rows_header}'. Proceeding anyway.")
        
        # Skip the blank line if present
        second_line = infile.readline().strip()
        if second_line: # If it's not empty, it's the first data line
            infile.seek(0) # Rewind to start
            infile.readline() # Read header
            # No, this approach is bad if there's no blank line.
            # Simpler: just iterate and skip if blank.

        # Re-read file to properly handle blank line
        infile.seek(0)
        infile.readline() # Read first header line (e.g., "360000")
        
        for line_num, line in enumerate(infile):
            line = line.strip()
            if not line: # Skip empty lines
                continue

            try:
                # Find the first space to separate ID from the rest of the string
                first_space_idx = line.find(' ')
                if first_space_idx == -1:
                    print(f"Warning: Line {line_num+2}: '{line}' does not contain expected space separator. Skipping.")
                    continue
                
                original_id = line[:first_space_idx]
                rest_of_line = line[first_space_idx+1:]

                # The projected string part is the segment before the first comma
                projected_string_part = rest_of_line.split(',')[0]

                # Store the original_id and the projected_string_part for later use
                # When relabel_ids runs, it gives each unique original_id a new int_id.
                # We need to map new_int_id -> (original_id, projected_string_part)
                # For `format_operator1.py`, we prepare input for `relabel_ids.py`
                # which expects `key value`. Here, `key` is `original_id` and `value` is `projected_string_part`.
                lines_to_process.append(f"{original_id} {projected_string_part}")

            except Exception as e:
                print(f"Error parsing line {line_num+2}: '{line}': {e}. Skipping.")
                continue

    header_output = f"{len(lines_to_process)} {0}" # Assuming 0 for second table rows
    
    with open(output_path, "w") as outfile:
        outfile.write("\n".join([header_output] + lines_to_process))
    print(f"Formatted input for Operator 1 written to {output_path}.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath")
    parser.add_argument("--output_path")
    args = parser.parse_args()
    format_operator1(args.filepath, args.output_path)

if __name__ == "__main__":
    main()
