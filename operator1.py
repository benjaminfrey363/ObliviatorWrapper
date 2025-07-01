import os
import subprocess
from pathlib import Path
import argparse
import shutil # Import shutil for directory removal
import re # Import regex module for string replacement
from typing import Optional # Import Optional for Python < 3.10 type hints

###########################
# OBLIVIATOR OPERATOR 1 WRAPPER #
###########################

# Path to the source file containing the hardcoded filter for Operator 1
OP1_FILTER_SOURCE_FILE_REL_PATH = Path("enclave/scalable_oblivious_join.c")

# Unique placeholders for the filters (MUST be manually in C code now)
OP1_PLACEHOLDER_MT = "FILTER_PLACEHOLDER_VALUE_OP1_MT" # Multi-threaded filter placeholder
OP1_PLACEHOLDER_ST = "FILTER_PLACEHOLDER_VALUE_OP1_ST" # Single-threaded filter placeholder

# Regex patterns to find the ENTIRE LINE containing the placeholder.
# This makes it robust to minor whitespace/formatting differences around the placeholder.
# We will then reconstruct the line.
OP1_PATTERN_MT_CAPTURE = r"(cb1\[i\] = \()(\s*)(" + re.escape(OP1_PLACEHOLDER_MT) + r")(\s*)( < arr1\[i\]\.key\);)"
OP1_PATTERN_ST_CAPTURE = r"(control_bit\[i\] = \()(\s*)(" + re.escape(OP1_PLACEHOLDER_ST) + r")(\s*)( < arr\[i\]\.key;)"


def _replace_filter_value_in_any_source(
    source_file_abs_path: Path, 
    search_regex_pattern: str, # Regex to use for searching
    search_value: str, # The string to search for (placeholder or number)
    replace_value: str # The string to replace with (number or placeholder)
):
    """
    Modifies a C source file to replace a specific string (placeholder or number) with a target value.
    """
    if not source_file_abs_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_file_abs_path}. Cannot set filter.")

    print(f"\n--- DEBUG (Internal): Attempting to modify filter in: {source_file_abs_path} ---")
    print(f"DEBUG (Internal): Searching for: '{search_value}', Replacing with: '{replace_value}'")
    print(f"DEBUG (Internal): Using Regex pattern: '{search_regex_pattern}'")

    original_content = ""
    with open(source_file_abs_path, 'r') as f_read: 
        original_content = f_read.read()

    # DEBUG: Print content as Python reads it
    print(f"\n--- DEBUG (Internal): Content as Python reads it (lines with search_value) ---")
    found_lines_in_read = [line.strip() for line in original_content.splitlines() if search_value in line]
    if found_lines_in_read:
        for line in found_lines_in_read:
            print(f"  '{line}'")
        print(f"DEBUG (Internal): Search value '{search_value}' WAS found when Python read the file.")
    else:
        print(f"DEBUG (Internal): Search value '{search_value}' was NOT found when Python read the file.")
    print("--- END Content Preview ---")

    # Perform the replacement using the provided regex pattern
    new_content = re.sub(search_regex_pattern, r"\g<1>\g<2>" + replace_value + r"\g<4>\g<5>", original_content)
    
    if original_content == new_content:
        # If no change, it's a critical error
        print(f"WARNING (Internal): Regex replacement failed. No change made to {source_file_abs_path}.")
        if search_value not in original_content:
            print(f"WARNING (Internal): Search value '{search_value}' was NOT found anywhere in the file content.")
        else:
            print(f"WARNING (Internal): Search value '{search_value}' was found, but regex pattern did not match the context.")
        raise ValueError("Filter modification failed: Search value not matched in source file for replacement.")
        
    with open(source_file_abs_path, 'w') as f_write:
        f_write.write(new_content)
        os.fsync(f_write.fileno()) 
    print(f"DEBUG (Internal): File {source_file_abs_path} successfully modified and synced.")
    print("Source file modification complete.")


def _cleanup_temp_dir(temp_dir_path: Path):
    """Removes the specified temporary directory and its contents."""
    if temp_dir_path.exists() and temp_dir_path.is_dir():
        print(f"\nCleaning up temporary directory: {temp_dir_path}...")
        try:
            shutil.rmtree(temp_dir_path)
            print("Temporary directory cleaned up successfully.")
        except OSError as e:
            print(f"Error cleaning up temporary directory {temp_dir_path}: {e}")
    else:
        print(f"Temporary directory {temp_dir_path} does not exist or is not a directory. Skipping cleanup.")

def obliviator_operator1 (
    filepath: str,
    temp_dir: Path,
    ultimate_final_output_path: Path,
    operator1_variant: str = "default",
    id_col: str = "",
    string_to_project_col: str = "",
    filter_threshold_op1: Optional[int] = None
):
    """
    Runs Obliviator's "Operator 1" which performs a projection.

    Args:
        filepath (str): Path to the input data file (e.g., test.csv).
        temp_dir (Path): The temporary directory for this run.
        ultimate_final_output_path (Path): The path for the final output file.
        operator1_variant (str): Specifies which Operator 1 implementation to use.
        id_col (str): Column in filepath to use as the unique ID.
        string_to_project_col (str): Column containing the string to be projected.
        filter_threshold_op1 (int): Numerical threshold for the filter in Operator 1.
    """
    print(f"Running oblivious Operator 1 (variant: {operator1_variant}) on {filepath}")

    print("Created temp directory " + str(temp_dir))

    #########################################
    # 1. Format input for Obliviator Operator 1 #
    #########################################

    print("Formatting input for Obliviator Operator 1...")
    format_path = temp_dir / "op1_format.txt"
    subprocess.run([
        "python", "obliviator_formatting/format_operator1.py",
        "--filepath", filepath,
        "--output_path", str(format_path),
        "--id_col", id_col,
        "--string_to_project_col", string_to_project_col
    ], check=True, cwd=Path(__file__).parent)
    print("Formatted input written to " + str(format_path) + ".")

    ###################################################
    # 2. Relabel IDs to reduce into Obliviators range #
    ###################################################

    print("Relabeling IDs for Operator 1 input...")
    relabel_path = temp_dir / "op1_relabel.txt"
    mapping_path = temp_dir / "op1_map.txt"
    subprocess.run([
        "python", "obliviator_formatting/relabel_ids.py",
        "--input_path", str(format_path),
        "--output_path", str(relabel_path),
        "--mapping_path", str(mapping_path),
        "--key_index_to_relabel", "0" # Relabel the ID (first column of format.txt)
    ], check=True, cwd=Path(__file__).parent)
    print("Relabeled input written to " + str(relabel_path) + ", relabel map written to " + str(mapping_path) + ".")

    ##############################
    # 3. Run Obliviator Operator 1 #
    ##############################

    print(f"Running Obliviator Operator 1 ({operator1_variant} variant)...")

    code_dir = None
    if operator1_variant == "default":
        code_dir = Path(os.path.expanduser("~/obliviator/operator_1"))
    elif operator1_variant == "opaque_shared_memory":
        code_dir = Path(os.path.expanduser("~/obliviator/opaque_shared_memory/operator_1"))
    else:
        raise ValueError(f"Unknown operator1_variant: {operator1_variant}. Choose 'default' or 'opaque_shared_memory'.")

    # --- Apply filter modification if a threshold is provided ---
    filter_modified_in_source = False
    if filter_threshold_op1 is not None:
        try:
            # Set MT filter
            _replace_filter_value_in_any_source(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                OP1_PATTERN_MT_CAPTURE, # Use the MT CAPTURE regex
                OP1_PLACEHOLDER_MT, # Search for MT placeholder string
                str(filter_threshold_op1) # Replace with numerical value
            )
            # Set ST filter (if different)
            _replace_filter_value_in_any_source(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                OP1_PATTERN_ST_CAPTURE, # Use the ST CAPTURE regex
                OP1_PLACEHOLDER_ST, # Search for ST placeholder string
                str(filter_threshold_op1) # Replace with numerical value
            )
            filter_modified_in_source = True
        except Exception as e:
            print(f"\nFATAL: Failed to correctly parameterize Operator 1 filter in C source code. "
                  f"Please check the manual setup step in README.md and the source file: {code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH}")
            print(f"Error details: {e}")
            raise
            
    try:
        print(f"Building Obliviator Operator 1 ({operator1_variant})...")
        subprocess.run(["make", "clean"], cwd=code_dir, check=True)
        subprocess.run(["make"], cwd=code_dir, check=True)

        absolute_path_to_input = (Path(__file__).parent / relabel_path).resolve()

        print(f"Build completed. Executing Operator 1 with input: {absolute_path_to_input} (absolute path)")
        print(f"obliviator executable will run from CWD: {code_dir}")

        subprocess.run(
            ["./host/parallel", "./enclave/parallel_enc.signed", "1", str(absolute_path_to_input)],
            cwd=code_dir
        )
        print("Exited Obliviator Operator 1 successfully.")

        # Find and Copy Obliviator's Raw Output
        obliviator_raw_output_filename = Path(absolute_path_to_input).stem + "_output.txt"
        obliviator_raw_output_path_absolute = temp_dir / obliviator_raw_output_filename

        print(f"DEBUG: Expected raw Obliviator output filename: {obliviator_raw_output_filename}")
        print(f"DEBUG: Python will look for Obliviator output at: {obliviator_raw_output_path_absolute}")

        if not obliviator_raw_output_path_absolute.exists():
            print(f"DEBUG: Contents of {temp_dir}: {[item.name for item in temp_dir.iterdir()]}")
            raise FileNotFoundError(f"Obliviator output file not found: {obliviator_raw_output_path_absolute}")

        # The output from obliviator_1 is `ID projected_string_part`
        # We need to reverse relabel the ID part (column 0).
        subprocess.run([
            "python", "obliviator_formatting/reverse_relabel_ids.py",
            "--input_path", str(obliviator_raw_output_path_absolute),
            "--output_path", str(ultimate_final_output_path), # Write directly to final location
            "--mapping_path", str(mapping_path), # Use the map generated for op1_relabel.txt
            "--key_index_to_relabel", "0" # Relabel the ID (first column of obliviator output)
        ], check=True, cwd=Path(__file__).parent)
        print("Reverse-relabeled Operator 1 output written to " + str(ultimate_final_output_path) + ".\n\n")
        print(f"✅ Output of Obliviator Operator 1 written to: {ultimate_final_output_path}\n\n")

    except Exception as e:
        print(f"\nFATAL ERROR during execution: {e}")
        raise
    finally:
        # --- Revert filter modification after execution ---
        if filter_modified_in_source:
            # Revert MT filter
            _replace_filter_value_in_any_source(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                OP1_PATTERN_MT_CAPTURE, # Regex to find the numerical value
                str(filter_threshold_op1), # Search for the numerical value
                OP1_PLACEHOLDER_MT # Replace with the placeholder string
            )
            # Revert ST filter
            _replace_filter_value_in_any_source(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                OP1_PATTERN_ST_CAPTURE, # Regex to find the numerical value
                str(filter_threshold_op1), # Search for the numerical value
                OP1_PLACEHOLDER_ST # Replace with the placeholder string
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath", required=True)
    parser.add_argument("--id_col", required=True, help="Column to use as the unique ID.")
    parser.add_argument("--string_to_project_col", required=True, help="Column containing the string to be projected.")
    parser.add_argument("--filter_threshold_op1", type=int, default=None,
                        help="Numerical threshold for the filter in Operator 1 (e.g., 88).")
    parser.add_argument("--operator1_variant", choices=["default", "opaque_shared_memory"], default="default",
                        help="Specify the Operator 1 variant.")
    parser.add_argument("--no_cleanup", action="store_true",
                        help="Do not clean up temporary directories after execution. Useful for debugging.")
    args = parser.parse_args()

    temp_dir_name = "tmp_operator1"
    temp_dir = Path(temp_dir_name)
    temp_dir.mkdir(exist_ok=True)
    print("Created temporary directory for intermediate files: " + str(temp_dir))

    final_output_filename = "op1_output.txt"
    ultimate_final_output_path = Path(os.getcwd()) / final_output_filename

    try:
        obliviator_operator1(
            os.path.expanduser(args.filepath),
            temp_dir,
            ultimate_final_output_path,
            args.operator1_variant,
            args.id_col,
            args.string_to_project_col,
            args.filter_threshold_op1
        )
    except Exception as e:
        print(f"\nFATAL ERROR during execution: {e}")
        raise
    finally:
        if not args.no_cleanup:
            _cleanup_temp_dir(temp_dir)
        else:
            print(f"\nSkipping cleanup of {temp_dir}. Temporary files preserved for debugging.")


if __name__ == "__main__":
    main()

