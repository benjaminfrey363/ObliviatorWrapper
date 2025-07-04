import os
import subprocess
from pathlib import Path
import argparse
import shutil
import re
from typing import Optional, List

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
    operator1_variant: str,
    filter_col: str,
    payload_cols: List[str],
    filter_threshold_op1: Optional[int],
    filter_condition_op1: str
):
    """
    Runs Obliviator's "Operator 1" which performs a projection with an optional filter.
    """
    print(f"Running oblivious Operator 1 (variant: {operator1_variant}) on {filepath}")

    temp_dir.mkdir(exist_ok=True)
    print("Created temp directory " + str(temp_dir))

    # --- Step 1: Initial formatting from CSV to "ID VALUE" format ---
    print("\nStep 1: Formatting input for Obliviator...")
    format_path = temp_dir / "op1_format.txt"
    format_cmd = [
        "python", "obliviator_formatting/format_operator1.py",
        "--filepath", filepath,
        "--output_path", str(format_path),
        "--filter_col", filter_col,
        "--payload_cols", *payload_cols
    ]
    subprocess.run(format_cmd, check=True, cwd=Path(__file__).parent)
    print("Initial formatting complete.")

    # --- Step 2: Relabel data specifically for Operator 1's needs ---
    print("\nStep 2: Relabeling data for Operator 1...")
    relabel_path = temp_dir / "op1_relabel_for_c.txt"
    mapping_path = temp_dir / "op1_value_map.txt"
    subprocess.run([
        "python", "obliviator_formatting/relabel_op1.py",
        "--input_path", str(format_path),
        "--output_path", str(relabel_path),
        "--mapping_path", str(mapping_path)
    ], check=True, cwd=Path(__file__).parent)
    print("Operator 1 relabeling complete.")


    print(f"\nStep 3: Running Obliviator C program ({operator1_variant} variant)...")

    code_dir = Path(os.path.expanduser(f"~/obliviator/{operator1_variant}/operator_1")) if operator1_variant == "opaque_shared_memory" else Path(os.path.expanduser("~/obliviator/operator_1"))
    if not code_dir.exists():
        raise FileNotFoundError(f"Could not find operator code directory: {code_dir}")

    filter_modified = False
    condition_modified = False

    try:
        if filter_threshold_op1 is not None:
            print(f"Applying filter to source: key {filter_condition_op1} {filter_threshold_op1}")
            _modify_source_file(code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH, MT_COMMENT_START, OP1_PLACEHOLDER_MT, MT_COMMENT_END, str(filter_threshold_op1))
            _modify_source_file(code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH, ST_COMMENT_START, OP1_PLACEHOLDER_ST, ST_COMMENT_END, str(filter_threshold_op1))
            filter_modified = True

            valid_conditions = ['<', '>', '==', '<=', '>=', '!=']
            if filter_condition_op1 not in valid_conditions:
                raise ValueError(f"Invalid filter operator '{filter_condition_op1}'. Valid are: {valid_conditions}")
            
            _modify_source_file(code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH, MT_COND_START, OP1_PLACEHOLDER_MT_COND, MT_COND_END, filter_condition_op1)
            _modify_source_file(code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH, ST_COND_START, OP1_PLACEHOLDER_ST_COND, ST_COND_END, filter_condition_op1)
            condition_modified = True

        print(f"\nBuilding Obliviator Operator 1...")
        subprocess.run(["make", "clean"], cwd=code_dir, check=True, capture_output=True)
        subprocess.run(["make"], cwd=code_dir, check=True)

        absolute_path_to_input = (Path(__file__).parent / relabel_path).resolve()
        print(f"Build completed. Executing Operator 1 with input: {absolute_path_to_input}")

        execution_command = ["./host/parallel", "./enclave/parallel_enc.signed", "1", str(absolute_path_to_input)]
        completed_process = subprocess.run(execution_command, cwd=code_dir, capture_output=True, text=True)

        if completed_process.returncode not in [0, 1]:
            raise subprocess.CalledProcessError(completed_process.returncode, execution_command, completed_process.stdout, completed_process.stderr)

        print("Exited Obliviator Operator 1 successfully.")
        
        try:
            time_output = completed_process.stdout.strip().splitlines()[0]
            time_value = float(time_output)
            time_file_path = ultimate_final_output_path.with_suffix('.time')
            with open(time_file_path, 'w') as tf:
                tf.write(str(time_value))
            print(f"Captured execution time: {time_value}s. Saved to {time_file_path}")
        except (ValueError, IndexError) as e:
            print(f"Warning: Could not parse execution time from C program output. Error: {e}")

        # --- Step 4: Reverse the relabeling ---
        print("\nStep 4: Reversing relabeling for intermediate output...")
        obliviator_raw_output_path = temp_dir / (relabel_path.stem + "_output.txt")
        intermediate_output_path = temp_dir / "op1_intermediate_output.txt"

        if not obliviator_raw_output_path.exists():
            raise FileNotFoundError(f"Obliviator output file not found: {obliviator_raw_output_path}")

        subprocess.run([
            "python", "obliviator_formatting/reverse_relabel_op1.py",
            "--input_path", str(obliviator_raw_output_path),
            "--output_path", str(intermediate_output_path),
            "--mapping_path", str(mapping_path)
        ], check=True, cwd=Path(__file__).parent)
        print("Reverse relabeling complete.")

        # --- Step 5: Reconstruct final CSV ---
        print("\nStep 5: Reconstructing final CSV file...")
        reconstruct_cmd = [
            "python", "obliviator_formatting/reconstruct_csv.py",
            "--intermediate_path", str(intermediate_output_path),
            "--final_csv_path", str(ultimate_final_output_path),
            "--filter_col", filter_col,
            "--payload_cols", *payload_cols
        ]
        subprocess.run(reconstruct_cmd, check=True, cwd=Path(__file__).parent)
        print(f"✅ Process complete. Final CSV output written to: {ultimate_final_output_path}\n")

    except subprocess.CalledProcessError as e:
        print("\n--- FATAL ERROR: Build or Execution Failed ---")
        print(f"Command '{e.cmd}' returned non-zero exit status {e.returncode}.")
        if e.stdout: print("--- STDOUT ---\n" + e.stdout)
        if e.stderr: print("--- STDERR ---\n" + e.stderr)
        raise
    finally:
        if filter_modified:
            _modify_source_file(code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH, MT_COMMENT_START, str(filter_threshold_op1), MT_COMMENT_END, OP1_PLACEHOLDER_MT, True)
            _modify_source_file(code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH, ST_COMMENT_START, str(filter_threshold_op1), ST_COMMENT_END, OP1_PLACEHOLDER_ST, True)
        if condition_modified:
            _modify_source_file(code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH, MT_COND_START, filter_condition_op1, MT_COND_END, OP1_PLACEHOLDER_MT_COND, True)
            _modify_source_file(code_dir / OP1_FILTER_SOURCE_FILE_REL_PATH, ST_COND_START, filter_condition_op1, ST_COND_END, OP1_PLACEHOLDER_ST_COND, True)


def main():
    parser = argparse.ArgumentParser(description="Wrapper for Obliviator's Operator 1 (Projection).")
    parser.add_argument("--filepath", required=True, help="Path to the input CSV file.")
    parser.add_argument("--output_path", required=True, help="Path for the final output CSV file.")
    parser.add_argument("--filter_col", required=True, help="Column name in the CSV to use for filtering.")
    parser.add_argument("--payload_cols", nargs='+', required=True, help="One or more columns to include in the payload, separated by spaces.")
    parser.add_argument("--filter_threshold_op1", type=int, help="Numerical threshold for the filter. If not provided, no filter is applied.")
    parser.add_argument("--filter_condition_op1", type=str, default="<", help="Operator for the filter (e.g., '>', '<', '=='). Remember to quote operators like '>' or '<'.")
    parser.add_argument("--operator1_variant", choices=["default", "opaque_shared_memory"], default="default", help="Specify the Operator 1 variant.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories after execution.")
    args = parser.parse_args()

    temp_dir = Path(f"tmp_operator1_{os.getpid()}")
    
    ultimate_final_output_path = Path(os.path.expanduser(args.output_path))
    ultimate_final_output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        obliviator_operator1(
            os.path.expanduser(args.filepath),
            temp_dir,
            ultimate_final_output_path,
            args.operator1_variant,
            args.filter_col,
            args.payload_cols,
            args.filter_threshold_op1,
            args.filter_condition_op1
        )
    except Exception:
        print("\nExecution aborted due to an error.")
    finally:
        if not args.no_cleanup:
            _cleanup_temp_dir(temp_dir)
        else:
            print(f"\nSkipping cleanup of {temp_dir}. Temporary files preserved for debugging.")


if __name__ == "__main__":
    main()
