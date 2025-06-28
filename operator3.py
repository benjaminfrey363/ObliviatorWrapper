# operator3.py (Fixed Filter & Relabeling for Step 3_1)

import os
import subprocess
from pathlib import Path
import argparse
import time
import re # Import regex module for string replacement

###########################
# OBLIVIATOR OPERATOR 3 WRAPPER #
###########################

# Path to the source file containing the hardcoded filter, relative to the step's code_dir
SCALABLE_OBLIVIOUS_JOIN_C_PATH_REL_TO_CODE_DIR = Path("enclave/scalable_oblivious_join.c")
# The string literal placeholder that MUST exist in the C code (manual setup required)
FILTER_PLACEHOLDER_STRING = "FILTER_PLACEHOLDER_VALUE"
# Regex to find the placeholder and capture the parts around it
# This pattern ensures we target the exact line where the filter value is.
FILTER_PATTERN_REGEX = r"(control_bit\[i\] = \()(\s*)(" + re.escape(FILTER_PLACEHOLDER_STRING) + r")(\s*)( <= arr\[i\]\.key\);)"


def _replace_filter_value_in_source(code_dir: Path, target_value: str):
    """
    Modifies the scalable_oblivious_join.c file to replace FILTER_PLACEHOLDER_VALUE
    with the given target_value (which can be a number or the placeholder string itself).
    """
    source_file_abs_path = code_dir / SCALABLE_OBLIVIOUS_JOIN_C_PATH_REL_TO_CODE_DIR
    
    if not source_file_abs_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_file_abs_path}. Cannot set filter.")

    print(f"Modifying filter value in {source_file_abs_path} to '{target_value}'...")
    
    original_content = ""
    with open(source_file_abs_path, 'r') as f:
        original_content = f.read()

    # Perform the replacement using the regex pattern
    new_content = re.sub(FILTER_PATTERN_REGEX, r"\g<1>\g<2>" + target_value + r"\g<4>\g<5>", original_content)
    
    if original_content == new_content:
        # This means the pattern (including the placeholder) was not found or already replaced
        if FILTER_PLACEHOLDER_STRING not in original_content:
            print(f"Warning: Placeholder '{FILTER_PLACEHOLDER_STRING}' not found *anywhere* in {source_file_abs_path}. "
                  "Please ensure manual setup of the placeholder in the C file is correct.")
        else:
            print(f"Warning: Regex pattern '{FILTER_PATTERN_REGEX}' did not match or value was already set. "
                  f"Placeholder '{FILTER_PLACEHOLDER_STRING}' exists in file, but not in the expected context. Content unchanged.")
        
    with open(source_file_abs_path, 'w') as f:
        f.write(new_content)
    
    # Force write buffers to disk immediately
    os.fsync(f.fileno())
    
    print("Source file modification complete (and synced to disk).")


def _run_obliviator_step(
    step_name: str,
    input_filepath: Path, # The ABSOLUTE path to the original input file for this step
    obliviator_base_dir: Path, # The base directory for the specific obliviator variant (e.g., ~/obliviator/operator_3)
    temp_dir: Path, # The temporary directory for this overall Operator 3 run
    operator_variant: str,
    # Arguments specific to step 3_1
    filter_key_col: str = "",
    id_col: str = "",
    filter_threshold_3_1: int = 0 # The numerical threshold for the filter in step 3_1
) -> Path:
    """
    Helper function to run a single obliviator operator step for Operator 3.
    """
    step_subdir = Path(step_name) # e.g., "3_1"

    # Define the code directory for this specific step (e.g., ~/obliviator/operator_3/3_1)
    code_dir = obliviator_base_dir / step_subdir
    
    print(f"\n--- Running Obliviator Operator 3, Step {step_name} ({operator_variant} variant) ---")

    # 1. Format input using the specific formatter for this step
    print(f"Formatting input for Operator 3, Step {step_name}...")
    format_path = temp_dir / f"op3_{step_name}_format.txt"
    
    formatter_script_path = Path(__file__).parent / f"obliviator_formatting/format_operator3_{step_name}.py"
    
    formatter_cmd = [
        "python", str(formatter_script_path),
        "--filepath", str(input_filepath),
        "--output_path", str(format_path)
    ]
    
    # Add specific arguments for format_operator3_1.py
    if step_name == "3_1":
        if not filter_key_col:
            raise ValueError(f"For step {step_name}, 'filter_key_col' must be specified.")
        if not id_col:
            raise ValueError(f"For step {step_name}, 'id_col' must be specified.")
        formatter_cmd.extend([
            "--filter_key_col", filter_key_col,
            "--id_col", id_col
        ])

    subprocess.run(formatter_cmd, check=True, cwd=Path(__file__).parent)
    print(f"Formatted input written to {format_path}.")

    # 2. Relabel IDs
    print(f"Relabeling IDs for Operator 3, Step {step_name} input...")
    relabel_path = temp_dir / f"op3_{step_name}_relabel.txt"
    mapping_path = temp_dir / f"op3_{step_name}_map.txt"
    
    relabel_cmd = [
        "python", "obliviator_formatting/relabel_ids.py",
        "--input_path", str(format_path),
        "--output_path", str(relabel_path),
        "--mapping_path", str(mapping_path)
    ]
    
    # CRITICAL: For step 3_1, we must NOT relabel the filter_key_value (column 0).
    # We only care about relabeling the ID for reconstruction if needed, but for the filter
    # the original value of the filter key needs to pass through.
    if step_name == "3_1":
        relabel_cmd.append("--key_index_to_relabel")
        relabel_cmd.append("-1") # Do not relabel any key for this specific operator's input
        
    subprocess.run(relabel_cmd, check=True, cwd=Path(__file__).parent)
    print(f"Relabeled input written to {relabel_path}, relabel map written to {mapping_path}.")

    # --- Apply filter modification if it's step 3_1 and a threshold is provided ---
    filter_modified_in_source = False
    if step_name == "3_1" and filter_threshold_3_1 is not None:
        try:
            _replace_filter_value_in_source(code_dir, str(filter_threshold_3_1)) # Set the numerical value
            filter_modified_in_source = True
        except Exception as e:
            print(f"ERROR: Failed to modify filter source code: {e}. Proceeding without modification.")
            # Do not re-raise, allow the rest to proceed for further debugging if needed.
    
    try: # Use a try-finally to ensure source is reverted even if compilation/run fails
        # 3. Run Obliviator binary - Build ALWAYS after potential source modification
        print(f"Building Obliviator Operator 3, Step {step_name} ({operator_variant})...")
        subprocess.run(["make", "clean"], cwd=code_dir, check=True)
        subprocess.run(["make"], cwd=code_dir, check=True)

        absolute_path_to_relabel_input = relabel_path.resolve()

        print(f"Build completed. Executing Operator 3, Step {step_name} with input: {absolute_path_to_relabel_input} (absolute path)")
        print(f"obliviator executable will run from CWD: {code_dir}")

        subprocess.run(
            ["./host/parallel", "./enclave/parallel_enc.signed", "1", str(absolute_path_to_relabel_input)],
            cwd=code_dir
        )
        print(f"Exited Obliviator Operator 3, Step {step_name} successfully.")

        # Find and Copy Obliviator's Raw Output
        obliviator_raw_output_filename = relabel_path.stem + "_output.txt"
        obliviator_raw_output_path_absolute = temp_dir / obliviator_raw_output_filename

        print(f"DEBUG: Expected raw Obliviator output filename: {obliviator_raw_output_filename}")
        print(f"DEBUG: Python will look for Obliviator output at: {obliviator_raw_output_path_absolute}")

        if not obliviator_raw_output_path_absolute.exists():
            print(f"DEBUG: Contents of {temp_dir}: {[item.name for item in temp_dir.iterdir()]}")
            raise FileNotFoundError(f"Obliviator output file not found: {obliviator_raw_output_path_absolute}")

        return obliviator_raw_output_path_absolute

    except Exception as e:
        print(f"Error during Obliviator Step {step_name} execution or output retrieval: {e}")
        raise # Re-raise to propagate the error
    finally:
        # --- Revert filter modification after execution ---
        if filter_modified_in_source: # Only revert if we actually changed it
            _replace_filter_value_in_source(code_dir, FILTER_PLACEHOLDER_STRING) # Revert to placeholder


def obliviator_operator3_pipeline (
    initial_filepath: str, # The very first input CSV for the entire pipeline
    filter_key_col_3_1: str, # Column to use as filter key in step 3_1
    id_col_3_1: str, # Column to use as ID for reconstruction in step 3_1
    filter_threshold_3_1: int = 0, # The numerical threshold for the filter in step 3_1
    operator3_variant: str = "default" # "default" or "opaque_shared_memory"
):
    """
    Runs the full Obliviator "Operator 3" pipeline (Filter -> Join -> Aggregate)
    using a generic CSV input for the first step, and pre-staged inputs for subsequent steps.
    """
    print(f"Running oblivious Operator 3 Pipeline (variant: {operator3_variant}) starting with {initial_filepath}")

    temp_dir = Path("tmp_operator3_pipeline")
    temp_dir.mkdir(exist_ok=True)
    print("Created temp directory " + str(temp_dir))

    obliviator_base_dir_path = None
    if operator3_variant == "default":
        obliviator_base_dir_path = Path(os.path.expanduser("~/obliviator/operator_3"))
    elif operator3_variant == "opaque_shared_memory":
        obliviator_base_dir_path = Path(os.path.expanduser("~/obliviator/opaque_shared_memory/operator_3"))
    else:
        raise ValueError(f"Unknown operator3_variant: {operator3_variant}.")

    # --- Step 3_1: Filter/Projection ---
    print("\n--- Initiating Operator 3: Step 3_1 (Filter/Projection) ---")
    step1_output_path = _run_obliviator_step(
        step_name="3_1",
        input_filepath=Path(initial_filepath).resolve(),
        obliviator_base_dir=obliviator_base_dir_path,
        temp_dir=temp_dir,
        operator_variant=operator3_variant,
        filter_key_col=filter_key_col_3_1,
        id_col=id_col_3_1,
        filter_threshold_3_1=filter_threshold_3_1 # Pass the filter threshold
    )
    print(f"Step 3_1 completed. Raw output: {step1_output_path}")
    
    # --- Transformation 3_1_output -> 3_2_input ---
    # As discussed, 3_2 uses a pre-staged file of a different format.
    filepath_3_2 = os.path.expanduser("~/obliviator/data/big_data_benchmark/bdb_query3_step2_sample_input.txt")
    step_3_2_input_actual = Path(filepath_3_2).resolve() 
    print(f"Step 3_1 output conceptually linked to Step 3_2 input: {step_3_2_input_actual}")


    # --- Step 3_2: Join ---
    print("\n--- Initiating Operator 3: Step 3_2 (Join) ---")
    step2_output_path = _run_obliviator_step(
        step_name="3_2",
        input_filepath=step_3_2_input_actual,
        obliviator_base_dir=obliviator_base_dir_path,
        temp_dir=temp_dir,
        operator_variant=operator3_variant
    )
    print(f"Step 3_2 completed. Raw output: {step2_output_path}")

    # --- Transformation 3_2_output -> 3_3_input ---
    # Similar to above, 3_3 uses a pre-staged file.
    filepath_3_3 = os.path.expanduser("~/obliviator/data/big_data_benchmark/bdb_query3_step3_sample_input.txt")
    step_3_3_input_actual = Path(filepath_3_3).resolve()
    print(f"Step 3_2 output conceptually linked to Step 3_3 input: {step_3_3_input_actual}")


    # --- Step 3_3: Aggregate ---
    print("\n--- Initiating Operator 3: Step 3_3 (Aggregate) ---")
    step3_output_path = _run_obliviator_step(
        step_name="3_3",
        input_filepath=step_3_3_input_actual,
        obliviator_base_dir=obliviator_base_dir_path,
        temp_dir=temp_dir,
        operator_variant=operator3_variant
    )
    print(f"Step 3_3 completed. Raw output: {step3_output_path}")

    # --- Final Output Copy (for the entire pipeline) ---
    final_wrapper_output_path = temp_dir / "final_op3_pipeline_output.txt"
    try:
        with open(step3_output_path, 'r') as src, open(final_wrapper_output_path, 'w') as dest:
            dest.write(src.read())
        print(f"\n✅ Obliviator Operator 3 Pipeline completed. Final output written to: {final_wrapper_output_path}\n\n")
    except Exception as e:
        print(f"Error copying final pipeline output: {e}")
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--initial_filepath", required=True,
                        help="Path to the initial CSV input for the entire Operator 3 pipeline.")
    parser.add_argument("--filter_key_col_3_1", required=True,
                        help="Name of the column to use as the filter key for Step 3_1.")
    parser.add_argument("--id_col_3_1", required=True,
                        help="Name of the column to use as the ID for reconstruction in Step 3_1.")
    parser.add_argument("--filter_threshold_3_1", type=int, default=None,
                        help="Numerical threshold for the filter in Step 3_1 (e.g., 19800101).")
    parser.add_argument("--operator3_variant", choices=["default", "opaque_shared_memory"], default="default",
                        help="Specify the Operator 3 variant.")
    args = parser.parse_args()

    # Expand user path for the initial input file
    args.initial_filepath = os.path.expanduser(args.initial_filepath)

    obliviator_operator3_pipeline(
        args.initial_filepath,
        args.filter_key_col_3_1,
        args.id_col_3_1,
        args.filter_threshold_3_1,
        args.operator3_variant
    )


if __name__ == "__main__":
    main()

