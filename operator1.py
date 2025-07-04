import os
import subprocess
from pathlib import Path
import argparse
import shutil
import re
from typing import Optional

###########################
# OBLIVIATOR OPERATOR 1 WRAPPER #
###########################

# Path to the source file containing the hardcoded filter for Operator 1
OP1_FILTER_SOURCE_FILE_REL_PATH = Path("enclave/scalable_oblivious_join.c")

# Unique placeholders for the filters (MUST be manually in C code now)
OP1_PLACEHOLDER_MT = "FILTER_PLACEHOLDER_VALUE_OP1_MT" # Multi-threaded filter placeholder
OP1_PLACEHOLDER_ST = "FILTER_PLACEHOLDER_VALUE_OP1_ST" # Single-threaded filter placeholder
# condition placeholders - <, >, ==
OP1_PLACEHOLDER_MT_COND = "FILTER_PLACEHOLDER_COND_OP1_MT"
OP1_PLACEHOLDER_ST_COND = "FILTER_PLACEHOLDER_COND_OP1_ST"

# Unique comment markers for precise replacement
MT_COMMENT_START = "/*MT_FILTER_START*/"
MT_COMMENT_END = "/*MT_FILTER_END*/"
ST_COMMENT_START = "/*ST_FILTER_START*/"
ST_COMMENT_END = "/*ST_FILTER_END*/"
MT_COND_START = "/*MT_COND_START*/"
MT_COND_END = "/*MT_COND_END*/"
ST_COND_START = "/*ST_COND_START*/"
ST_COND_END = "/*ST_COND_END*/"


def _modify_source_file(
    source_file_abs_path: Path,
    comment_start: str,
    value_to_find_inside_comments: str,
    comment_end: str,
    value_to_replace_with: str,
    is_revert_operation: bool = False
):
    """
    Modifies a C source file by replacing a specific value within unique inline comments.
    This ensures extremely precise replacement.
    """
    if not source_file_abs_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_file_abs_path}. Cannot set filter.")

    print(f"\n--- DEBUG (Internal): Attempting to modify file: {source_file_abs_path} ---")
    print(f"DEBUG (Internal): Operation: {'REVERT' if is_revert_operation else 'SET'}")
    print(f"DEBUG (Internal): Searching for content within: {comment_start}...{comment_end}")
    print(f"DEBUG (Internal): Replacing '{value_to_find_inside_comments}' with '{value_to_replace_with}'")

    original_content = ""
    with open(source_file_abs_path, 'r') as f_read:
        original_content = f_read.read()

    full_string_to_find = f"{comment_start}{value_to_find_inside_comments}{comment_end}"
    full_string_to_replace_with = f"{comment_start}{value_to_replace_with}{comment_end}"

    if full_string_to_find not in original_content:
        print(f"ERROR (Internal): Search string '{full_string_to_find}' was NOT found in the file content.")
        raise ValueError("Filter modification failed: Search string not found.")

    new_content = original_content.replace(full_string_to_find, full_string_to_replace_with)

    if original_content == new_content:
        print(f"WARNING (Internal): String replacement failed. No change made to {source_file_abs_path}.")
        raise ValueError("Filter modification failed: Replacement resulted in no change.")

    with open(source_file_abs_path, 'w') as f_write:
        f_write.write(new_content)
        os.fsync(f_write.fileno())
    print(f"DEBUG (Internal): File {source_file_abs_path} successfully modified and synced.")


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
    filter_threshold_op1: Optional[int] = None,
    filter_condition_op1: str = "<"
):
    """
    Runs Obliviator's "Operator 1" which performs a projection with an optional filter.
    """
    print(f"Running oblivious Operator 1 (variant: {operator1_variant}) on {filepath}")

    # Create the temporary directory if it doesn't exist
    temp_dir.mkdir(exist_ok=True)
    print("Created temp directory " + str(temp_dir))

    # --- Input formatting and relabeling ---
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

    print("Relabeling IDs for Operator 1 input...")
    relabel_path = temp_dir / "op1_relabel.txt"
    mapping_path = temp_dir / "op1_map.txt"
    subprocess.run([
        "python", "obliviator_formatting/relabel_ids.py",
        "--input_path", str(format_path),
        "--output_path", str(relabel_path),
        "--mapping_path", str(mapping_path),
        "--key_index_to_relabel", "0"
    ], check=True, cwd=Path(__file__).parent)
    print("Relabeled input written to " + str(relabel_path) + ", relabel map written to " + str(mapping_path) + ".")


    print(f"Running Obliviator Operator 1 ({operator1_variant} variant)...")

    code_dir = None
    if operator1_variant == "default":
        code_dir = Path(os.path.expanduser("~/obliviator/operator_1"))
    elif operator1_variant == "opaque_shared_memory":
        code_dir = Path(os.path.expanduser("~/obliviator/opaque_shared_memory/operator_1"))
    else:
        raise ValueError(f"Unknown operator1_variant: {operator1_variant}. Choose 'default' or 'opaque_shared_memory'.")

    filter_modified_in_source = False
    condition_modified_in_source = False

    try:
        # --- Apply filter modification if a threshold is provided ---
        if filter_threshold_op1 is not None:
            print(f"\nApplying filter to source: key {filter_condition_op1} {filter_threshold_op1}")
            # Set MT filter
            _modify_source_file(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                MT_COMMENT_START, OP1_PLACEHOLDER_MT, MT_COMMENT_END,
                str(filter_threshold_op1), False
            )
            # Set ST filter
            _modify_source_file(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                ST_COMMENT_START, OP1_PLACEHOLDER_ST, ST_COMMENT_END,
                str(filter_threshold_op1), False
            )
            filter_modified_in_source = True

            # --- Validate filter condition before applying ---
            valid_conditions = ['<', '>', '==', '<=', '>=', '!=']
            if filter_condition_op1 not in valid_conditions:
                raise ValueError(
                    f"\n\n--- INVALID FILTER OPERATOR ---\n"
                    f"Received: '{filter_condition_op1}'.\n"
                    f"Valid operators are: {valid_conditions}.\n"
                    f"HINT: If using '<' or '>', you MUST quote it in the shell command.\n"
                    f"Example: --filter_condition_op1 \">\"\n"
                )

            # Set MT condition
            _modify_source_file(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                MT_COND_START, OP1_PLACEHOLDER_MT_COND, MT_COND_END,
                filter_condition_op1, False
            )
            # Set ST condition
            _modify_source_file(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                ST_COND_START, OP1_PLACEHOLDER_ST_COND, ST_COND_END,
                filter_condition_op1, False
            )
            condition_modified_in_source = True

        print(f"\nBuilding Obliviator Operator 1 ({operator1_variant})...")
        # Use capture_output to hide verbose make output unless there's an error
        subprocess.run(["make", "clean"], cwd=code_dir, check=True, capture_output=True)
        subprocess.run(["make"], cwd=code_dir, check=True)

        absolute_path_to_input = (Path(__file__).parent / relabel_path).resolve()
        print(f"Build completed. Executing Operator 1 with input: {absolute_path_to_input}")

        # --- EXECUTION WITH MODIFIED ERROR CHECKING ---
        execution_command = ["./host/parallel", "./enclave/parallel_enc.signed", "1", str(absolute_path_to_input)]
        completed_process = subprocess.run(
            execution_command,
            cwd=code_dir,
            capture_output=True, # Capture stdout/stderr
            text=True # Decode stdout/stderr as text
        )

        # The C program exits with 1 on success, so we check for other non-zero codes.
        if completed_process.returncode != 0 and completed_process.returncode != 1:
            print("\n--- FATAL ERROR: Obliviator Execution Failed ---")
            print(f"Command '{' '.join(execution_command)}' returned an unexpected error code: {completed_process.returncode}.")
            if completed_process.stdout:
                print("--- STDOUT ---")
                print(completed_process.stdout)
            if completed_process.stderr:
                print("--- STDERR ---")
                print(completed_process.stderr)
            print("-------------------------------------------------")
            # Raise an error to stop the script
            raise subprocess.CalledProcessError(completed_process.returncode, execution_command)

        print("Exited Obliviator Operator 1 successfully.")

        # --- Output processing and reverse relabeling ---
        obliviator_raw_output_filename = Path(absolute_path_to_input).stem + "_output.txt"
        obliviator_raw_output_path_absolute = temp_dir / obliviator_raw_output_filename

        if not obliviator_raw_output_path_absolute.exists():
            raise FileNotFoundError(f"Obliviator output file not found: {obliviator_raw_output_path_absolute}")

        subprocess.run([
            "python", "obliviator_formatting/reverse_relabel_ids.py",
            "--input_path", str(obliviator_raw_output_path_absolute),
            "--output_path", str(ultimate_final_output_path),
            "--mapping_path", str(mapping_path),
            "--key_index_to_relabel", "0"
        ], check=True, cwd=Path(__file__).parent)
        print(f"✅ Output of Obliviator Operator 1 written to: {ultimate_final_output_path}\n")

    except subprocess.CalledProcessError as e:
        # This block will now primarily catch errors from 'make'
        print("\n--- FATAL ERROR: Build Failed ---")
        print(f"Command '{e.cmd}' returned non-zero exit status {e.returncode}.")
        # Make prints errors to stderr
        if e.stderr:
            print("--- STDERR ---")
            print(e.stderr.decode())
        print("---------------------------------")
        raise
    except Exception as e:
        print(f"\nFATAL ERROR during script execution: {e}")
        raise
    finally:
        # --- Revert modifications after execution ---
        if filter_modified_in_source:
            _modify_source_file(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                MT_COMMENT_START, str(filter_threshold_op1), MT_COMMENT_END,
                OP1_PLACEHOLDER_MT, True
            )
            _modify_source_file(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                ST_COMMENT_START, str(filter_threshold_op1), ST_COMMENT_END,
                OP1_PLACEHOLDER_ST, True
            )
        if condition_modified_in_source:
            _modify_source_file(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                MT_COND_START, filter_condition_op1, MT_COND_END,
                OP1_PLACEHOLDER_MT_COND, True
            )
            _modify_source_file(
                code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH,
                ST_COND_START, filter_condition_op1, ST_COND_END,
                OP1_PLACEHOLDER_ST_COND, True
            )


def main():
    parser = argparse.ArgumentParser(description="Wrapper for Obliviator's Operator 1 (Projection).")
    parser.add_argument("--filepath", required=True)
    parser.add_argument("--id_col", required=True, help="Column to use as the unique ID.")
    parser.add_argument("--string_to_project_col", required=True, help="Column containing the string to be projected.")
    parser.add_argument("--filter_threshold_op1", type=int, help="Numerical threshold for the filter. If not provided, no filter is applied.")
    parser.add_argument("--filter_condition_op1", type=str, default="<", help="Operator for the filter (e.g., '>', '<', '=='). Remember to quote operators like '>' or '<'.")
    parser.add_argument("--operator1_variant", choices=["default", "opaque_shared_memory"], default="default", help="Specify the Operator 1 variant.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories after execution.")
    args = parser.parse_args()

    temp_dir_name = "tmp_operator1"
    temp_dir = Path(temp_dir_name)
    
    final_output_filename = "op1_output.txt"
    ultimate_final_output_path = Path.cwd() / final_output_filename

    try:
        obliviator_operator1(
            os.path.expanduser(args.filepath),
            temp_dir,
            ultimate_final_output_path,
            args.operator1_variant,
            args.id_col,
            args.string_to_project_col,
            args.filter_threshold_op1,
            args.filter_condition_op1
        )
    except Exception:
        # The specific error is already printed in the obliviator_operator1 function
        print("\nExecution aborted due to an error.")
    finally:
        if not args.no_cleanup:
            _cleanup_temp_dir(temp_dir)
        else:
            print(f"\nSkipping cleanup of {temp_dir}. Temporary files preserved for debugging.")


if __name__ == "__main__":
    main()
