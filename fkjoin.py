import os
import subprocess
from pathlib import Path
import argparse
import shutil # Import shutil for directory removal

#######################################
# OBLIVIATOR FOREIGN KEY JOIN WRAPPER #
#######################################

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


def obliviator_fk_join(
    filepath1: str,
    filepath2: str,
    join_key1: str,
    id_col1: str, # NEW ARG
    join_key2: str,
    id_col2: str, # NEW ARG
    temp_dir: Path,
    ultimate_final_output_path: Path,
    fk_join_variant: str = "default"
):
    """
    Runs an oblivious foreign key join using Obliviator.

    Args:
        filepath1 (str): Path to the first CSV file.
        filepath2 (str): Path to the second CSV file.
        join_key1 (str): The column name to use as the join key in filepath1.
        id_col1 (str): The column name to use as the unique ID in filepath1.
        join_key2 (str): The column name to use as the join key in filepath2.
        id_col2 (str): The column name to use as the unique ID in filepath2.
        temp_dir (Path): The temporary directory for this run.
        ultimate_final_output_path (Path): The path for the final output file.
        fk_join_variant (str): Specifies which FK join implementation to use.
                                "default" for ~/fk_join/
                                "opaque_shared_memory" for ~/opaque_shared_memory/fk_join/
    """
    print(f"Running oblivious foreign key join (variant: {fk_join_variant}) "
          f"of {filepath1} with join key {join_key1} and {filepath2} with join key {join_key2}")

    print("Created temp directory " + str(temp_dir))

    #######################################
    # 1. Format input for obliviator join #
    #######################################

    print("Formatting input CSVs for obliviator FK join...")
    format_path = temp_dir / "fk_format.txt"
    subprocess.run([
        "python", "obliviator_formatting/format_join.py", # Reusing format_join.py
        "--filepath1", filepath1, 
        "--filepath2", filepath2,
        "--join_key1", join_key1,
        "--id_col1", id_col1, # Pass new arg
        "--join_key2", join_key2,
        "--id_col2", id_col2, # Pass new arg
        "--output_path", str(format_path)
    ], check=True, cwd=Path(__file__).parent)
    print("Formatted input written to " + str(format_path) + ".")

    ###################################################
    # 2. Relabel IDs to reduce into Obliviators range #
    ###################################################

    print("Relabeling IDs for FK join...")
    relabel_path = temp_dir / "fk_relabel.txt"
    mapping_path = temp_dir / "fk_map.txt"
    subprocess.run([
        "python", "obliviator_formatting/relabel_ids.py", # Reusing relabel_ids.py
        "--input_path", str(format_path),
        "--output_path", str(relabel_path),
        "--mapping_path", str(mapping_path)
    ], check=True, cwd=Path(__file__).parent)
    print("Relabeled input written to " + str(relabel_path) + ", relabel map written to " + str(mapping_path) + ".")

    ##############################
    # 3. Run Obliviator FK Join #
    ##############################

    print(f"Running Obliviator FK join ({fk_join_variant} variant)...")

    # Determine the correct code directory based on variant
    if fk_join_variant == "default":
        code_dir = Path(os.path.expanduser("~/obliviator/fk_join"))
    elif fk_join_variant == "opaque_shared_memory":
        code_dir = Path(os.path.expanduser("~/obliviator/opaque_shared_memory/fk_join"))
    else:
        raise ValueError(f"Unknown fk_join_variant: {fk_join_variant}. Choose 'default' or 'opaque_shared_memory'.")

    print(f"Building Obliviator FK join ({fk_join_variant})...")
    subprocess.run(["make", "clean"], cwd=code_dir, check=True)
    subprocess.run(["make"], cwd=code_dir, check=True)

    # Get the absolute path to the relabeled input file.
    absolute_path_to_input = (Path(__file__).parent / relabel_path).resolve()

    print(f"Build completed. Executing FK join with input: {absolute_path_to_input} (absolute path)")
    print(f"obliviator executable will run from CWD: {code_dir}")

    subprocess.run(
        ["./host/parallel", "./enclave/parallel_enc.signed", "1", str(absolute_path_to_input)],
        cwd=code_dir,
        check=True
    )
    print("Exited Obliviator FK join successfully.")

    # Now, explicitly read the output from the file created by obliviator
    obliviator_raw_output_filename = Path(absolute_path_to_input).stem + "_output.txt"
    obliviator_raw_output_path_absolute = temp_dir / obliviator_raw_output_filename

    print(f"DEBUG: Expected raw Obliviator output filename: {obliviator_raw_output_filename}")
    print(f"DEBUG: Python will look for Obliviator output at: {obliviator_raw_output_path_absolute}")

    obliv_fk_output_path = temp_dir / "obliv_fk_output.txt" # Intermediate copy for consistency

    try:
        if not obliviator_raw_output_path_absolute.exists():
            print(f"DEBUG: Contents of {temp_dir}: {[item.name for item in temp_dir.iterdir()]}")
            raise FileNotFoundError(f"Obliviator output file not found: {obliviator_raw_output_path_absolute}")

        with open(obliviator_raw_output_path_absolute, "r") as infile:
            content = infile.read()

        with open(obliv_fk_output_path, "w") as outfile:
            outfile.write(content)
        print(f"Copied raw Obliviator output from {obliviator_raw_output_path_absolute} to {obliv_fk_output_path}.")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the Obliviator FK join executable correctly writes its output.")
        raise # Re-raise to be caught by main's try-except
    except Exception as e:
        print(f"An unexpected error occurred while processing Obliviator output: {e}")
        raise

    ######################
    # 4. Reverse Relabel #
    ######################

    print("Reverse-relabeling IDs for FK join output...")
    # Direct output to the ultimate final path
    subprocess.run([
        "python", "obliviator_formatting/reverse_relabel_ids.py", # Reusing relabel_ids.py
        "--input_path", str(obliv_fk_output_path),
        "--output_path", str(ultimate_final_output_path), # Write directly to final location
        "--mapping_path", str(mapping_path),
        "--key_index_to_relabel", "0" # Assume first col needs relabeling
    ], check=True, cwd=Path(__file__).parent)
    print("Reverse-relabeled FK join output written to " + str(ultimate_final_output_path) + ".\n\n")

    print(f"✅ Output of Obliviator FK join written to: {ultimate_final_output_path}\n\n")
    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath1", default="flat_csv/dynamic__Person.csv")
    parser.add_argument("--filepath2", default="flat_csv/dynamic__Comment.csv")
    parser.add_argument("--join_key1", required=True)
    parser.add_argument("--id_col1", required=True, help="Column in filepath1 to use as unique ID.")
    parser.add_argument("--join_key2", required=True)
    parser.add_argument("--id_col2", required=True, help="Column in filepath2 to use as unique ID.")
    parser.add_argument("--fk_join_variant", choices=["default", "opaque_shared_memory"], default="default",
                        help="Specify the FK join variant.")
    parser.add_argument("--no_cleanup", action="store_true",
                        help="Do not clean up temporary directories after execution. Useful for debugging.")
    args = parser.parse_args()

    # Define temp_dir specific to this script
    temp_dir_name = "tmp_fk_join"
    temp_dir = Path(temp_dir_name)
    temp_dir.mkdir(exist_ok=True)
    print("Created temporary directory for intermediate files: " + str(temp_dir))

    # Define the final output file location
    final_output_filename = "fk_join_output.txt"
    ultimate_final_output_path = Path(os.getcwd()) / final_output_filename

    try:
        obliviator_fk_join(
            os.path.expanduser(args.filepath1),
            os.path.expanduser(args.filepath2),
            args.join_key1,
            args.id_col1, # Pass id_col1
            args.join_key2,
            args.id_col2, # Pass id_col2
            temp_dir,
            ultimate_final_output_path,
            args.fk_join_variant
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

