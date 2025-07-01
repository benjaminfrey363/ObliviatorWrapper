import os
import subprocess
from pathlib import Path
import argparse
import shutil # Import shutil for directory removal

###########################
# OBLIVIATOR JOIN WRAPPER #
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

def obliviator_join (
    filepath1: str,
    filepath2: str,
    join_key1: str,
    id_col1: str,
    join_key2: str,
    id_col2: str,
    temp_dir: Path,
    ultimate_final_output_path: Path
):
    
    print("Running oblivious join of " + filepath1 + " with join key " + join_key1 + " and " + filepath2 + " with join key " + join_key2)

    print("Created temp directory " + str(temp_dir))

    #######################################
    # 1. Format input for obliviator join #
    #######################################
    
    print("Formatting input CSVs for obliviator join...")
    format_path = temp_dir / "format.txt"
    subprocess.run([
        "python", "obliviator_formatting/format_join.py",
        "--filepath1", filepath1, 
        "--filepath2", filepath2,
        "--join_key1", join_key1,
        "--id_col1", id_col1, # Pass id_col1 to format_join.py
        "--join_key2", join_key2,
        "--id_col2", id_col2, # Pass id_col2 to format_join.py
        "--output_path", str(format_path)
  ], check=True, cwd=Path(__file__).parent)
    print("Formatted input written to " + str(format_path) + ".")

    ###################################################
    # 2. Relabel IDs to reduce into Obliviators range #
    ###################################################

    print("Relabeling IDs...")
    relabel_path = temp_dir / "relabel.txt"
    mapping_path = temp_dir / "map.txt"
    subprocess.run([
        "python", "obliviator_formatting/relabel_ids.py",
        "--input_path", str(format_path),
        "--output_path", str(relabel_path),
        "--mapping_path", str(mapping_path),
        "--key_index_to_relabel", "0" # Relabel the first column (the join key)
    ],check=True, cwd=Path(__file__).parent)
    print("Relabeled input written to " + str(relabel_path) + ", relabel map written to " + str(mapping_path) + ".")

    ##########################
    # 3. Run Obliviator Join #
    ##########################

    print("Running Obliviator join...")
    obliv_output_path = temp_dir / "obliv_output.txt"
    code_dir = os.path.expanduser("~/obliviator/join_kks")

    subprocess.run(["make", "clean"], cwd=code_dir, check=True)
    subprocess.run(["make", "L3=1"], cwd=code_dir, check=True)

    print("Build completed. Executing join with")
    print("\tInput path: " + str(relabel_path))
    print("\tOutput path: " + str(obliv_output_path))
    subprocess.run(
        ["./app", "../" + str(obliv_output_path), "../" + str(relabel_path)],
        cwd=code_dir,
        check=True
    )
    print("Exited Obliviator join successfully, raw output written to " + str(obliv_output_path) + ".")

    ######################
    # 4. Reverse Relabel #
    ######################

    print("Reverse-relabeling IDs...")
    # Direct output to the ultimate final path
    subprocess.run([
        "python", "obliviator_formatting/reverse_relabel_ids.py",
        "--input_path", str(obliv_output_path),
        "--output_path", str(ultimate_final_output_path), # Write directly to final location
        "--mapping_path", str(mapping_path),
        "--key_index_to_relabel", "0" # The join_kks output is: relabelled_key relabelled_matching_id. Relabel first col.
    ],check=True, cwd=Path(__file__).parent)
    print("Reverse-relabeled output written to " + str(ultimate_final_output_path) + ".\n\n")

    print(f"✅ Output of Obliviator join written to: {ultimate_final_output_path}\n\n")
    return


def main():
    parser = argparse.ArgumentParser()
    # Removed default paths to ensure clarity on required arguments.
    parser.add_argument("--filepath1", required=True)
    parser.add_argument("--filepath2", required=True)
    parser.add_argument("--join_key1", required=True)
    parser.add_argument("--id_col1", required=True, help="Column in filepath1 to use as unique ID for original data.")
    parser.add_argument("--join_key2", required=True)
    parser.add_argument("--id_col2", required=True, help="Column in filepath2 to use as unique ID for original data.")
    # Removed --output_path as it's now handled internally and defaults to 'join_output.txt'
    parser.add_argument("--no_cleanup", action="store_true",
                        help="Do not clean up temporary directories after execution. Useful for debugging.")
    args = parser.parse_args()

    # Define temp_dir specific to this script
    temp_dir_name = "tmp_join"
    temp_dir = Path(temp_dir_name)
    temp_dir.mkdir(exist_ok=True)
    print("Created temporary directory for intermediate files: " + str(temp_dir))

    # Define the final output file location
    final_output_filename = "join_output.txt" # A specific name for this script's output
    ultimate_final_output_path = Path(os.getcwd()) / final_output_filename # Output to current working directory

    try:
        obliviator_join(
            os.path.expanduser(args.filepath1),
            os.path.expanduser(args.filepath2),
            args.join_key1,
            args.id_col1, # Pass id_col1
            args.join_key2,
            args.id_col2, # Pass id_col2
            temp_dir,
            ultimate_final_output_path
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

