import os
import subprocess
from pathlib import Path
import argparse
import time

#######################################
# OBLIVIATOR FOREIGN KEY JOIN WRAPPER #
#######################################



def obliviator_fk_join(
    filepath1: str,
    filepath2: str,
    join_key1: str,
    join_key2: str,
    fk_join_variant: str = "default" # "default" for ~/fk_join, "opaque_shared_memory" for the other
):
    """
    Runs an oblivious foreign key join using Obliviator.

    Args:
        filepath1 (str): Path to the first CSV file.
        filepath2 (str): Path to the second CSV file.
        join_key1 (str): The join key column name in filepath1.
        join_key2 (str): The join key column name in filepath2.
        fk_join_variant (str): Specifies which FK join implementation to use.
                                "default" for ~/obliviator/fk_join/
                                "opaque_shared_memory" for ~/obliviator/opaque_shared_memory/fk_join/
    """
    print(f"Running oblivious foreign key join (variant: {fk_join_variant}) "
          f"of {filepath1} with join key {join_key1} and {filepath2} with join key {join_key2}")

    # Create temp directory for this FK join operation
    temp_dir = Path("tmp_fk_join") # Use a separate temp directory for FK join
    temp_dir.mkdir(exist_ok=True)
    print("Created temp directory " + str(temp_dir))

    #######################################
    # 1. Format input for obliviator join #
    #######################################

    print("Formatting input CSVs for obliviator FK join...")
    format_path = temp_dir / "fk_format.txt"
    subprocess.run([
        "python", "obliviator_formatting/format_join.py",
        "--filepath1", filepath1,
        "--filepath2", filepath2,
        "--join_key1", join_key1,
        "--join_key2", join_key2,
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
        "python", "obliviator_formatting/relabel_ids.py",
        "--input_path", str(format_path),
        "--output_path", str(relabel_path),
        "--mapping_path", str(mapping_path)
    ], check=True, cwd=Path(__file__).parent)
    print("Relabeled input written to " + str(relabel_path) + ", relabel map written to " + str(mapping_path) + ".")

    ##############################
    # 3. Run Obliviator FK Join #
    ##############################

    print(f"Running Obliviator FK join ({fk_join_variant} variant)...")

    # Determine the correct code directory for the obliviator binary
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
    # This path is passed to obliviator.
    absolute_path_to_input = (Path(__file__).parent / relabel_path).resolve()

    print(f"DEBUG: Absolute path to input for obliviator: {absolute_path_to_input}")
    print(f"DEBUG: obliviator executable will run from CWD: {code_dir}")

    # Execute the FK join.
    subprocess.run(
        ["./host/parallel", "./enclave/parallel_enc.signed", "1", str(absolute_path_to_input)],
        cwd=code_dir, # Run the subprocess from here
        check=True
    )
    print("Exited Obliviator FK join successfully.")

    # --- Crucial Fix Here ---
    # Based on your `ls tmp_fk_join` output, obliviator writes its output
    # to the same directory as the input file, using the input file's stem.
    # So, the output file will be found in `temp_dir`.
    obliviator_raw_output_filename = relabel_path.stem + "_output.txt"
    obliviator_raw_output_path_absolute = temp_dir / obliviator_raw_output_filename # Look in temp_dir, not code_dir

    print(f"DEBUG: Expected raw Obliviator output filename: {obliviator_raw_output_filename}")
    print(f"DEBUG: Python will look for Obliviator output at: {obliviator_raw_output_path_absolute}")

    # Define the path where the raw obliviator output will be copied to within our temp directory
    obliv_fk_output_path = temp_dir / "obliv_fk_output.txt"

    try:
        if not obliviator_raw_output_path_absolute.exists():
            print(f"DEBUG: Contents of {temp_dir} at time of error (where we expect the output):")
            try:
                for item in temp_dir.iterdir():
                    print(f"  - {item.name}")
            except Exception as list_e:
                print(f"  (Could not list directory contents: {list_e})")
            
            raise FileNotFoundError(f"Obliviator output file not found: {obliviator_raw_output_path_absolute}")

        # Read content from the obliviator's discovered output file
        with open(obliviator_raw_output_path_absolute, "r") as infile:
            content = infile.read()

        # Write content to our wrapper's temp output file
        with open(obliv_fk_output_path, "w") as outfile:
            outfile.write(content)
        print(f"Copied raw Obliviator output from {obliviator_raw_output_path_absolute} to {obliv_fk_output_path}.")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the Obliviator FK join executable correctly writes its output.")
        return
    except Exception as e:
        print(f"An unexpected error occurred while processing Obliviator output: {e}")
        return

    ######################
    # 4. Reverse Relabel #
    ######################

    print("Reverse-relabeling IDs for FK join output...")
    final_output_path = temp_dir / "fk_output.txt"

    subprocess.run([
        "python", "obliviator_formatting/reverse_relabel_ids.py",
        "--input_path", str(obliv_fk_output_path),
        "--output_path", str(final_output_path),
        "--mapping_path", str(mapping_path)
    ], check=True, cwd=Path(__file__).parent)
    print("Reverse-relabeled FK join output written to " + str(final_output_path) + ".\n\n")

    print(f"✅ Output of Obliviator FK join written to: {final_output_path}\n\n")
    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath1", default="flat_csv/dynamic__Person.csv")
    parser.add_argument("--filepath2", default="flat_csv/dynamic__Comment.csv")
    parser.add_argument("--join_key1", default="id")
    parser.add_argument("--join_key2", default="CreatorPersonId")
    parser.add_argument("--fk_join_variant", choices=["default", "opaque_shared_memory"], default="default",
                        help="Specify the FK join variant.")
    args = parser.parse_args()

    obliviator_fk_join(args.filepath1, args.filepath2, args.join_key1, args.join_key2, args.fk_join_variant)


if __name__ == "__main__":
    main()

