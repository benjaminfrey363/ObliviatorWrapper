import os
import subprocess
from pathlib import Path
import argparse
import shutil # Import shutil for directory removal

###########################
# OBLIVIATOR OPERATOR 1 WRAPPER #
###########################

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
    temp_dir: Path, # Moved before operator1_variant
    ultimate_final_output_path: Path, # Moved before operator1_variant
    operator1_variant: str = "default" # Now it correctly follows non-default args
):
    """
    Runs Obliviator's "Operator 1" which performs a projection.

    Args:
        filepath (str): Path to the input data file (e.g., test.txt).
        temp_dir (Path): The temporary directory for this run.
        ultimate_final_output_path (Path): The path for the final output file.
        operator1_variant (str): Specifies which Operator 1 implementation to use.
                                "default" for ~/operator_1/
                                "opaque_shared_memory" for ~/opaque_shared_memory/operator_1/
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
        "--output_path", str(format_path)
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
        "--mapping_path", str(mapping_path)
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

    obliv_op1_output_path = temp_dir / "obliv_op1_output.txt" # Intermediate copy for consistency

    try:
        if not obliviator_raw_output_path_absolute.exists():
            print(f"DEBUG: Contents of {temp_dir}: {[item.name for item in temp_dir.iterdir()]}")
            raise FileNotFoundError(f"Obliviator output file not found: {obliviator_raw_output_path_absolute}")

        with open(obliviator_raw_output_path_absolute, "r") as infile:
            content = infile.read()

        with open(obliv_op1_output_path, "w") as outfile:
            outfile.write(content)
        print(f"Copied raw Obliviator output from {obliviator_raw_output_path_absolute} to {obliv_op1_output_path}.")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the Obliviator Operator 1 executable correctly writes its output.")
        raise # Re-raise to be caught by main's try-except
    except Exception as e:
        print(f"An unexpected error occurred while processing Obliviator output: {e}")
        raise

    ######################
    # 4. Reverse Relabel #
    ######################

    print("Reverse-relabeling IDs for Operator 1 output and projecting string parts...")
    # Direct output to the ultimate final path
    subprocess.run([
        "python", "obliviator_formatting/reverse_relabel_ids.py",
        "--input_path", str(obliv_op1_output_path),
        "--output_path", str(ultimate_final_output_path), # Write directly to final location
        "--mapping_path", str(mapping_path),
        "--key_index_to_relabel", "0" # Assume first col needs relabeling
    ], check=True, cwd=Path(__file__).parent)
    print("Reverse-relabeled Operator 1 output written to " + str(ultimate_final_output_path) + ".\n\n")

    print(f"✅ Output of Obliviator Operator 1 written to: {ultimate_final_output_path}\n\n")
    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath", default="data/test_operator1.txt")
    parser.add_argument("--operator1_variant", choices=["default", "opaque_shared_memory"], default="default",
                        help="Specify the Operator 1 variant.")
    parser.add_argument("--no_cleanup", action="store_true",
                        help="Do not clean up temporary directories after execution. Useful for debugging.")
    args = parser.parse_args()

    # Define temp_dir specific to this script
    temp_dir_name = "tmp_operator1"
    temp_dir = Path(temp_dir_name)
    temp_dir.mkdir(exist_ok=True)
    print("Created temporary directory for intermediate files: " + str(temp_dir))

    # Define the final output file location
    final_output_filename = "op1_output.txt" # A specific name for this script's output
    ultimate_final_output_path = Path(os.getcwd()) / final_output_filename # Output to current working directory

    try:
        obliviator_operator1(
            os.path.expanduser(args.filepath),
            temp_dir, # Pass temp_dir
            ultimate_final_output_path, # Pass final output path
            args.operator1_variant # Pass operator1_variant last
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

