# operator3.py (Final Output Streamlined)

import os
import subprocess
from pathlib import Path
import argparse
import time
import re
from typing import Optional # Import Optional for Python < 3.10 type hints

###########################
# OBLIVIATOR OPERATOR 3 WRAPPER #
###########################

# Path to the source file containing the hardcoded filter, relative to the step's code_dir
SCALABLE_OBLIVIOUS_JOIN_C_PATH_REL_TO_CODE_DIR = Path("enclave/scalable_oblivious_join.c")
# The string literal placeholder that MUST exist in the C code (manual setup required)
FILTER_PLACEHOLDER_STRING = "FILTER_PLACEHOLDER_VALUE"
# Regex to find the placeholder and capture the parts around it
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
    raw_input_filepath: Optional[Path],
    transformed_input_filepath: Optional[Path], # This is the main input for relabeling and obliviator binary
    obliviator_base_dir: Path,
    temp_dir: Path,
    operator_variant: str,
    filter_key_col: str = "",
    id_col: str = "",
    filter_threshold_3_1: Optional[int] = None
) -> Path:
    """
    Helper function to run a single obliviator operator step for Operator 3.
    This function now handles the formatting/relabeling internally based on the step.
    """
    step_subdir = Path(step_name)

    code_dir = obliviator_base_dir / step_subdir
    
    print(f"\n--- Running Obliviator Operator 3, Step {step_name} ({operator_variant} variant) ---")

    actual_input_to_obliviator_binary = None
    # mapping_path is now passed explicitly from pipeline function, as it's needed for final output revert
    # mapping_path = temp_dir / f"op3_{step_name}_map.txt"

    if step_name == "3_1":
        # For step 3_1, we format the original raw input CSV
        print(f"Formatting initial CSV for Operator 3, Step {step_name}...")
        format_path = temp_dir / f"op3_{step_name}_format.txt"
        formatter_script_path = Path(__file__).parent / "obliviator_formatting" / "format_operator3_1.py"
        formatter_cmd = [
            "python", str(formatter_script_path),
            "--filepath", str(raw_input_filepath), # Original raw input for 3_1
            "--output_path", str(format_path),
            "--filter_key_col", filter_key_col,
            "--id_col", id_col
        ]
        subprocess.run(formatter_cmd, check=True, cwd=Path(__file__).parent)
        print(f"Formatted input written to {format_path}.")

        # Then relabel the formatted input
        print(f"Relabeling IDs for Operator 3, Step {step_name} input... (key_index_to_relabel=-1 for 3_1)")
        relabel_path = temp_dir / f"op3_{step_name}_relabel.txt"
        # Mapping path for step 3_1
        mapping_path_3_1 = temp_dir / f"op3_3_1_map.txt"
        relabel_cmd = [
            "python", "obliviator_formatting/relabel_ids.py",
            "--input_path", str(format_path),
            "--output_path", str(relabel_path),
            "--mapping_path", str(mapping_path_3_1),
            "--key_index_to_relabel", "-1" # Do not relabel the filter key
        ]
        subprocess.run(relabel_cmd, check=True, cwd=Path(__file__).parent)
        print(f"Relabeled input written to {relabel_path}, relabel map written to {mapping_path_3_1}.")
        actual_input_to_obliviator_binary = relabel_path.resolve()
    
    elif step_name in ["3_2", "3_3"]:
        # For steps 3_2 and 3_3, `transformed_input_filepath` is already the formatted data
        # (e.g., from transform_3_1_output_to_3_2_input.py or transform_3_2_output_to_3_3_input.py).
        # We only need to relabel its first column.
        print(f"Relabeling IDs for Operator 3, Step {step_name} input from transformed data...")
        relabel_path = temp_dir / f"op3_{step_name}_relabel.txt"
        # Mapping path for step 3_2 or 3_3
        current_mapping_path = temp_dir / f"op3_{step_name}_map.txt" # Define it here
        relabel_cmd = [
            "python", "obliviator_formatting/relabel_ids.py",
            "--input_path", str(transformed_input_filepath), # Input is already transformed/formatted
            "--output_path", str(relabel_path),
            "--mapping_path", str(current_mapping_path), # Use current step's map path
            "--key_index_to_relabel", "0" # Assume first column is key and needs relabeling.
        ]
        subprocess.run(relabel_cmd, check=True, cwd=Path(__file__).parent)
        print(f"Relabeled input written to {relabel_path}, relabel map written to {current_mapping_path}.")
        actual_input_to_obliviator_binary = relabel_path.resolve()
    
    else:
        raise ValueError(f"Unsupported step_name: {step_name}")


    # --- Apply filter modification if it's step 3_1 and a threshold is provided ---
    filter_modified_in_source = False
    if step_name == "3_1" and filter_threshold_3_1 is not None:
        try:
            _replace_filter_value_in_source(code_dir, str(filter_threshold_3_1))
            filter_modified_in_source = True
        except Exception as e:
            print(f"ERROR: Failed to modify filter source code: {e}. Proceeding without modification.")
            
    try:
        # 3. Run Obliviator binary - Build ALWAYS after potential source modification
        print(f"Building Obliviator Operator 3, Step {step_name} ({operator_variant})...")
        subprocess.run(["make", "clean"], cwd=code_dir, check=True)
        subprocess.run(["make"], cwd=code_dir, check=True)

        print(f"Build completed. Executing Operator 3, Step {step_name} with input: {actual_input_to_obliviator_binary} (absolute path)")
        print(f"obliviator executable will run from CWD: {code_dir}")

        subprocess.run(
            ["./host/parallel", "./enclave/parallel_enc.signed", "1", str(actual_input_to_obliviator_binary)],
            cwd=code_dir
        )
        print(f"Exited Obliviator Operator 3, Step {step_name} successfully.")

        # Find and Copy Obliviator's Raw Output
        obliviator_raw_output_filename = Path(actual_input_to_obliviator_binary).stem + "_output.txt"
        obliviator_raw_output_path_absolute = temp_dir / obliviator_raw_output_filename

        print(f"DEBUG: Expected raw Obliviator output filename: {obliviator_raw_output_filename}")
        print(f"DEBUG: Python will look for Obliviator output at: {obliviator_raw_output_path_absolute}")

        if not obliviator_raw_output_path_absolute.exists():
            print(f"DEBUG: Contents of {temp_dir}: {[item.name for item in temp_dir.iterdir()]}")
            raise FileNotFoundError(f"Obliviator output file not found: {obliviator_raw_output_path_absolute}")

        return obliviator_raw_output_path_absolute

    except Exception as e:
        print(f"Error during Obliviator Step {step_name} execution or output retrieval: {e}")
        raise
    finally:
        # --- Revert filter modification after execution ---
        if filter_modified_in_source:
            _replace_filter_value_in_source(code_dir, FILTER_PLACEHOLDER_STRING)


def obliviator_operator3_pipeline (
    initial_filepath: str, # The very first input CSV for the entire pipeline (Table 1 source)
    filter_key_col_3_1: str,
    id_col_3_1: str,
    filter_threshold_3_1: Optional[int] = None,
    # Arguments for 3_2 transformation
    join_key_col_3_2_A: str = "",
    join_key_col_3_2_B_and_values: str = "",
    
    # Arguments for 3_2 transformation (Table 2 source)
    second_table_filepath_3_2: str = "",
    second_table_key_col_3_2: str = "",
    second_table_other_cols_3_2: str = "",

    # Arguments for 3_3 transformation
    col1_from_step2_output_3_3: str = "", # Maps to <col1_value> in obliviator_3_3 input
    col2_from_step2_output_3_3: str = "", # Maps to <col2_value> in obliviator_3_3 input
    col3_from_step2_output_3_3: str = "", # Maps to <col3_value> in obliviator_3_3 input
    
    operator3_variant: str = "default"
):
    """
    Runs the full Obliviator "Operator 3" pipeline (Filter -> Join -> Aggregate)
    using a generic CSV input for the first step, and transforms data between steps.
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
        raw_input_filepath=Path(initial_filepath).resolve(), # Original CSV here
        transformed_input_filepath=None, # Not used for step 3_1 here
        obliviator_base_dir=obliviator_base_dir_path,
        temp_dir=temp_dir,
        operator_variant=operator3_variant,
        filter_key_col=filter_key_col_3_1,
        id_col=id_col_3_1,
        filter_threshold_3_1=filter_threshold_3_1
    )
    print(f"Step 3_1 completed. Raw output: {step1_output_path}")
    
    # --- Transformation 3_1_output -> 3_2_input ---
    print("\n--- Transforming Step 3_1 Output to Step 3_2 Input ---")
    step_3_2_input_transformed_path = temp_dir / "op3_3_2_input_transformed.txt"
    
    second_table_source_path_abs = Path(second_table_filepath_3_2).resolve()

    subprocess.run([
        "python", str(Path(__file__).parent / "obliviator_formatting/transform_3_1_output_to_3_2_input.py"),
        "--original_csv_filepath", str(Path(initial_filepath).resolve()), # Table 1 source
        "--step1_filtered_ids_filepath", str(step1_output_path),
        "--output_path", str(step_3_2_input_transformed_path),
        "--id_col_in_original_csv", id_col_3_1,
        "--join_key_col_3_2_A", join_key_col_3_2_A,
        "--join_key_col_3_2_B_and_values", join_key_col_3_2_B_and_values,
        "--second_table_filepath", str(second_table_source_path_abs), # Pass custom second table
        "--second_table_key_col", second_table_key_col_3_2,
        "--second_table_other_cols", second_table_other_cols_3_2
    ], check=True, cwd=Path(__file__).parent)
    print(f"Transformed input for Step 3_2 written to: {step_3_2_input_transformed_path}")


    # --- Step 3_2: Join ---
    print("\n--- Initiating Operator 3: Step 3_2 (Join) ---")
    step2_output_path = _run_obliviator_step(
        step_name="3_2",
        raw_input_filepath=None, # Not used for step 3_2
        transformed_input_filepath=step_3_2_input_transformed_path, # Transformed input here
        obliviator_base_dir=obliviator_base_dir_path,
        temp_dir=temp_dir,
        operator_variant=operator3_variant
    )
    print(f"Step 3_2 completed. Raw output: {step2_output_path}")

    # --- Transformation 3_2_output -> 3_3_input ---
    print("\n--- Transforming Step 3_2 Output to Step 3_3 Input ---")
    step_3_3_input_transformed_path = temp_dir / "op3_3_3_input_transformed.txt"
    subprocess.run([
        "python", str(Path(__file__).parent / "obliviator_formatting/transform_3_2_output_to_3_3_input.py"),
        "--step2_raw_output_filepath", str(step2_output_path),
        "--output_path", str(step_3_3_input_transformed_path),
        "--col1_from_step2_output", col1_from_step2_output_3_3,
        "--col2_from_step2_output", col2_from_step2_output_3_3,
        "--col3_from_step2_output", col3_from_step2_output_3_3
    ], check=True, cwd=Path(__file__).parent)
    print(f"Transformed input for Step 3_3 written to: {step_3_3_input_transformed_path}")


    # --- Step 3_3: Aggregate ---
    # This mapping_path is implicitly created by _run_obliviator_step for 3_3
    mapping_path_3_3_for_revert = temp_dir / f"op3_3_3_map.txt" 

    print("\n--- Initiating Operator 3: Step 3_3 (Aggregate) ---")
    step3_raw_output_path = _run_obliviator_step( # Renamed output variable for clarity
        step_name="3_3",
        raw_input_filepath=None,
        transformed_input_filepath=step_3_3_input_transformed_path,
        obliviator_base_dir=obliviator_base_dir_path,
        temp_dir=temp_dir,
        operator_variant=operator3_variant
    )
    print(f"Step 3_3 completed. Raw output: {step3_raw_output_path}")

    # --- Final Output Reverse Relabeling ---
    print("\n--- Reverting IDs in Final Aggregation Output ---")
    # Directly write the reverse-relabelled output to the final pipeline output file
    ultimate_final_output_path = temp_dir / "final_op3_pipeline_output.txt" # This is the target
    
    # Capture stdout/stderr of reverse_relabel_ids.py for debugging
    reverse_relabel_process = subprocess.run(
        ["python", "obliviator_formatting/reverse_relabel_ids.py",
         "--input_path", str(step3_raw_output_path), # Output of obliviator_3_3
         "--output_path", str(ultimate_final_output_path), # Direct output to final file
         "--mapping_path", str(mapping_path_3_3_for_revert), # Use the mapping for Step 3_3's input
         "--key_index_to_relabel", "0" # Relabel only the first column (the aggregated key)
        ], 
        check=True, 
        cwd=Path(__file__).parent,
        capture_output=True, # Capture output
        text=True # Decode as text
    )
    print(f"Reverse Relabeling stdout:\n{reverse_relabel_process.stdout}") # Print captured stdout
    if reverse_relabel_process.stderr:
        print(f"Reverse Relabeling stderr:\n{reverse_relabel_process.stderr}") # Print captured stderr

    print(f"\n✅ Obliviator Operator 3 Pipeline completed. Final output written to: {ultimate_final_output_path}\n\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--initial_filepath", required=True,
                        help="Path to the initial CSV input for the entire Operator 3 pipeline (Table 1 source).")
    parser.add_argument("--filter_key_col_3_1", required=True,
                        help="Name of the column to use as the filter key for Step 3_1.")
    parser.add_argument("--id_col_3_1", required=True,
                        help="Name of the column to use as the ID for reconstruction in Step 3_1.")
    parser.add_argument("--filter_threshold_3_1", type=int, default=None,
                        help="Numerical threshold for the filter in Step 3_1 (e.g., 19800101).")
    # Arguments for 3_2 transformation (Table 1 data derivation)
    parser.add_argument("--join_key_col_3_2_A", required=True,
                        help="Column from initial_filepath to use as JOIN_KEY_A for Step 3_2 input.")
    parser.add_argument("--join_key_col_3_2_B_and_values", required=True,
                        help="Comma-separated columns from initial_filepath for JOIN_KEY_B_AND_OTHER_DATA for Step 3_2 input.")
    
    # Arguments for 3_2 transformation (Table 2 data derivation)
    parser.add_argument("--second_table_filepath_3_2", required=True,
                        help="Path to the custom CSV file to use as the second table for Step 3_2 join.")
    parser.add_argument("--second_table_key_col_3_2", required=True,
                        help="Key column from the second_table_filepath_3_2 (e.g., 'long_string_col').")
    parser.add_argument("--second_table_other_cols_3_2", required=True,
                        help="Comma-separated other columns from the second_table_filepath_3_2 (e.g., 'id_val,number_val').")

    # Arguments for 3_3 transformation
    parser.add_argument("--col1_from_step2_output_3_3", required=True,
                        help="Maps to <col1_value> in obliviator_3_3 input (e.g., 't1_id').")
    parser.add_argument("--col2_from_step2_output_3_3", required=True,
                        help="Maps to <col2_value> in obliviator_3_3 input (e.g., 't1_numeric').")
    parser.add_argument("--col3_from_step2_output_3_3", required=True,
                        help="Maps to <col3_value> in obliviator_3_3 input (e.g., 't2_quantity').")

    parser.add_argument("--operator3_variant", choices=["default", "opaque_shared_memory"], default="default",
                        help="Specify the Operator 3 variant.")
    args = parser.parse_args()

    # Expand user paths for input files
    args.initial_filepath = os.path.expanduser(args.initial_filepath)
    args.second_table_filepath_3_2 = os.path.expanduser(args.second_table_filepath_3_2)

    obliviator_operator3_pipeline(
        args.initial_filepath,
        args.filter_key_col_3_1,
        args.id_col_3_1,
        args.filter_threshold_3_1,
        args.join_key_col_3_2_A,
        args.join_key_col_3_2_B_and_values,
        args.second_table_filepath_3_2,
        args.second_table_key_col_3_2,
        args.second_table_other_cols_3_2,
        args.col1_from_step2_output_3_3,
        args.col2_from_step2_output_3_3,
        args.col3_from_step2_output_3_3,
        args.operator3_variant
    )


if __name__ == "__main__":
    main()

