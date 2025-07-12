import os
import subprocess
from pathlib import Path
import argparse
import shutil
from typing import List

###########################################
# OBLIVIATOR NON-FOREIGN KEY JOIN WRAPPER #
###########################################

def _cleanup_temp_dir(temp_dir_path: Path):
    """Removes the specified temporary directory and its contents."""
    if temp_dir_path.exists() and temp_dir_path.is_dir():
        print(f"\nCleaning up temporary directory: {temp_dir_path}...")
        try:
            shutil.rmtree(temp_dir_path)
            print("Temporary directory cleaned up successfully.")
        except OSError as e:
            print(f"Error cleaning up temporary directory {temp_dir_path}: {e}")

def obliviator_nfk_join(
    table1_path: str,
    key1: str,
    payload1_cols: List[str],
    table2_path: str,
    key2: str,
    payload2_cols: List[str],
    temp_dir: Path,
    ultimate_final_output_path: Path,
    nfk_join_variant: str
):
    """
    Runs an oblivious non-foreign key (NFK) join using Obliviator.
    """
    print(f"Running oblivious NFK Join (variant: {nfk_join_variant})")
    temp_dir.mkdir(exist_ok=True)

    # --- Step 1: Format both CSVs into a single input file ---
    print("\nStep 1: Formatting input files for Obliviator...")
    format_path = temp_dir / "nfk_format.txt"
    format_cmd = [
        "python", "obliviator_formatting/format_fk_join.py",
        "--filepath1", table1_path, "--key1", key1, "--payload1_cols", *payload1_cols,
        "--filepath2", table2_path, "--key2", key2, "--payload2_cols", *payload2_cols,
        "--output_path", str(format_path)
    ]
    subprocess.run(format_cmd, check=True, cwd=Path(__file__).parent)
    print("Initial formatting complete.")

    # --- Step 2: Relabel all unique values to integer IDs ---
    print("\nStep 2: Relabeling data for C program...")
    relabel_path = temp_dir / "nfk_relabel_for_c.txt"
    mapping_path = temp_dir / "nfk_value_map.txt"
    subprocess.run([
        "python", "obliviator_formatting/relabel_fk_join.py",
        "--input_path", str(format_path),
        "--output_path", str(relabel_path),
        "--mapping_path", str(mapping_path)
    ], check=True, cwd=Path(__file__).parent)
    print("Relabeling complete.")

    # --- Step 3: Run the Obliviator C program ---
    print(f"\nStep 3: Running Obliviator NFK Join C program...")
    code_dir = Path(os.path.expanduser(f"~/obliviator/{nfk_join_variant}/join")) if nfk_join_variant == "opaque_shared_memory" else Path(os.path.expanduser("~/obliviator/join"))
    if not code_dir.exists():
        code_dir_fallback = Path(os.path.expanduser("~/obliviator/join_kks"))
        if code_dir_fallback.exists():
            code_dir = code_dir_fallback
        else:
            raise FileNotFoundError(f"Could not find NFK join code directory at {code_dir} or {code_dir_fallback}")
    
    print(f"Using code directory: {code_dir}")

    try:
        print(f"Building Obliviator NFK Join...")
        subprocess.run(["make", "clean"], cwd=code_dir, check=True, capture_output=True)
        subprocess.run(["make", "L3=1"], cwd=code_dir, check=True)

        absolute_path_to_input = (Path(__file__).parent / relabel_path).resolve()
        print(f"Build completed. Executing with input: {absolute_path_to_input}")

        execution_command = ["./host/parallel", "./enclave/parallel_enc.signed", "1", str(absolute_path_to_input)]
        completed_process = subprocess.run(execution_command, cwd=code_dir, capture_output=True, text=True)

        if completed_process.returncode not in [0, 1]:
            raise subprocess.CalledProcessError(completed_process.returncode, execution_command, completed_process.stdout, completed_process.stderr)

        print("Exited Obliviator NFK Join successfully.")

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
        raw_output_path = temp_dir / (relabel_path.stem + "_output.txt")
        intermediate_output_path = temp_dir / "nfk_intermediate_output.txt"

        if not raw_output_path.exists():
            raise FileNotFoundError(f"Obliviator output file not found: {raw_output_path}")

        # --- FIX: Use the new, dedicated reverse relabeler for NFK joins ---
        subprocess.run([
            "python", "obliviator_formatting/reverse_relabel_nfk_join.py",
            "--input_path", str(raw_output_path),
            "--output_path", str(intermediate_output_path),
            "--mapping_path", str(mapping_path)
        ], check=True, cwd=Path(__file__).parent)
        print("Reverse relabeling complete.")

        # --- Step 5: Reconstruct final CSV ---
        print("\nStep 5: Reconstructing final CSV file...")
        reconstruct_cmd = [
            "python", "obliviator_formatting/reconstruct_fk_join_csv.py",
            "--intermediate_path", str(intermediate_output_path),
            "--final_csv_path", str(ultimate_final_output_path),
            "--key_header", key1,
            "--payload1_headers", *payload1_cols,
            "--payload2_headers", *payload2_cols
        ]
        subprocess.run(reconstruct_cmd, check=True, cwd=Path(__file__).parent)
        print(f"✅ Process complete. Final CSV output written to: {ultimate_final_output_path}\n")

    except subprocess.CalledProcessError as e:
        print("\n--- FATAL ERROR: Build or Execution Failed ---")
        print(f"Command '{' '.join(e.cmd)}' returned non-zero exit status {e.returncode}.")
        if e.stdout: print("--- STDOUT ---\n" + e.stdout)
        if e.stderr: print("--- STDERR ---\n" + e.stderr)
        raise

def main():
    parser = argparse.ArgumentParser(description="Wrapper for Obliviator's Non-Foreign Key (NFK) Join.")
    parser.add_argument("--table1_path", required=True, help="Path to the first table CSV.")
    parser.add_argument("--key1", required=True, help="Name of the join key column in table 1.")
    parser.add_argument("--payload1_cols", nargs='*', required=False, default=[], help="Payload columns from table 1.")
    parser.add_argument("--table2_path", required=True, help="Path to the second table CSV.")
    parser.add_argument("--key2", required=True, help="Name of the join key column in table 2.")
    parser.add_argument("--payload2_cols", nargs='*', required=False, default=[], help="Payload columns from table 2.")
    parser.add_argument("--output_path", required=True, help="Path for the final output CSV file.")
    parser.add_argument("--nfk_join_variant", choices=["default", "opaque_shared_memory"], default="default")
    parser.add_argument("--no_cleanup", action="store_true")
    args = parser.parse_args()

    temp_dir = Path(f"tmp_nfk_join_{os.getpid()}")
    
    output_path = Path(os.path.expanduser(args.output_path))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        obliviator_nfk_join(
            os.path.expanduser(args.table1_path), args.key1, args.payload1_cols,
            os.path.expanduser(args.table2_path), args.key2, args.payload2_cols,
            temp_dir, output_path, args.nfk_join_variant
        )
    except Exception as e:
        print(f"\nExecution aborted due to an error: {e}")
    finally:
        if not args.no_cleanup:
            _cleanup_temp_dir(temp_dir)
        else:
            print(f"\nSkipping cleanup of {temp_dir}. Temporary files preserved for debugging.")

if __name__ == "__main__":
    main()
