import os
import subprocess
from pathlib import Path
import argparse
import shutil
from typing import List

################################
# OBLIVIATOR AGGREGATE WRAPPER #
################################

def _cleanup_temp_dir(temp_dir_path: Path):
    if temp_dir_path.exists() and temp_dir_path.is_dir():
        print(f"\nCleaning up temporary directory: {temp_dir_path}...")
        shutil.rmtree(temp_dir_path)
        print("Temporary directory cleaned up successfully.")

def obliviator_aggregate(
    filepath: str,
    output_path: Path,
    group_by_col: str,
    agg_col: str,
    payload_cols: List[str],
    temp_dir: Path,
    variant: str
):
    """
    Runs an oblivious aggregation using Obliviator's Operator 2.
    """
    print(f"Running oblivious Aggregation (variant: {variant})")
    temp_dir.mkdir(exist_ok=True)

    # --- Step 1: Format CSV for the operator ---
    print("\nStep 1: Formatting input file...")
    format_path = temp_dir / "op2_format.txt"
    format_cmd = [
        "python", "obliviator_formatting/format_operator2.py",
        "--filepath", filepath,
        "--output_path", str(format_path),
        "--group_by_col", group_by_col,
        "--agg_col", agg_col,
        "--payload_cols", *payload_cols
    ]
    subprocess.run(format_cmd, check=True, cwd=Path(__file__).parent)
    print("Formatting complete.")

    # --- Step 2: Relabel data for the C program ---
    print("\nStep 2: Relabeling data...")
    relabel_path = temp_dir / "op2_relabel_for_c.txt"
    mapping_path = temp_dir / "op2_map.txt"
    subprocess.run([
        "python", "obliviator_formatting/relabel_operator2.py",
        "--input_path", str(format_path),
        "--output_path", str(relabel_path),
        "--mapping_path", str(mapping_path)
    ], check=True, cwd=Path(__file__).parent)
    print("Relabeling complete.")

    # --- Step 3: Run the Obliviator C program ---
    print(f"\nStep 3: Running Obliviator Aggregation C program...")
    code_dir = Path(os.path.expanduser(f"~/obliviator/{variant}/operator_2")) if variant == "opaque_shared_memory" else Path(os.path.expanduser("~/obliviator/operator_2"))
    if not code_dir.exists():
        raise FileNotFoundError(f"Could not find operator code directory: {code_dir}")

    try:
        print(f"Building Obliviator Aggregation operator...")
        subprocess.run(["make", "clean"], cwd=code_dir, check=True, capture_output=True)
        subprocess.run(["make"], cwd=code_dir, check=True)

        absolute_path_to_input = (Path(__file__).parent / relabel_path).resolve()
        print(f"Build completed. Executing with input: {absolute_path_to_input}")

        execution_command = ["./host/parallel", "./enclave/parallel_enc.signed", "1", str(absolute_path_to_input)]
        completed_process = subprocess.run(execution_command, cwd=code_dir, capture_output=True, text=True)

        if completed_process.returncode not in [0, 1]:
            raise subprocess.CalledProcessError(completed_process.returncode, execution_command, completed_process.stdout, completed_process.stderr)

        print("Exited Obliviator Aggregation successfully.")

        try:
            time_output = completed_process.stdout.strip().splitlines()[0]
            time_value = float(time_output)
            time_file_path = output_path.with_suffix('.time')
            with open(time_file_path, 'w') as tf:
                tf.write(str(time_value))
            print(f"Captured execution time: {time_value}s. Saved to {time_file_path}")
        except (ValueError, IndexError) as e:
            print(f"Warning: Could not parse execution time from C program output. Error: {e}")

        # --- Step 4: Reverse the relabeling ---
        print("\nStep 4: Reversing relabeling for intermediate output...")
        raw_output_path = temp_dir / (relabel_path.stem + "_output.txt")
        intermediate_output_path = temp_dir / "op2_intermediate_output.txt"

        if not raw_output_path.exists():
            raise FileNotFoundError(f"Obliviator output file not found: {raw_output_path}")
        
        # --- FIX: Use the new, dedicated reverse relabeler for aggregation ---
        subprocess.run([
            "python", "obliviator_formatting/reverse_relabel_operator2.py",
            "--input_path", str(raw_output_path),
            "--output_path", str(intermediate_output_path),
            "--mapping_path", str(mapping_path)
        ], check=True, cwd=Path(__file__).parent)
        print("Reverse relabeling complete.")

        # --- Step 5: Reconstruct final CSV ---
        print("\nStep 5: Reconstructing final CSV file...")
        reconstruct_cmd = [
            "python", "obliviator_formatting/reconstruct_agg_csv.py",
            "--intermediate_path", str(intermediate_output_path),
            "--final_csv_path", str(output_path),
            "--group_by_header", group_by_col,
            "--payload_headers", *payload_cols
        ]
        subprocess.run(reconstruct_cmd, check=True, cwd=Path(__file__).parent)
        print(f"✅ Process complete. Final CSV output written to: {output_path}\n")

    except subprocess.CalledProcessError as e:
        print("\n--- FATAL ERROR: Build or Execution Failed ---")
        print(f"Command '{' '.join(e.cmd)}' returned non-zero exit status {e.returncode}.")
        if e.stdout: print("--- STDOUT ---\n" + e.stdout)
        if e.stderr: print("--- STDERR ---\n" + e.stderr)
        raise

def main():
    parser = argparse.ArgumentParser(description="Wrapper for Obliviator's Aggregation (Operator 2).")
    parser.add_argument("--filepath", required=True, help="Path to the input CSV file.")
    parser.add_argument("--output_path", required=True, help="Path for the final output CSV file.")
    parser.add_argument("--group_by_col", required=True, help="Column name to group the data by.")
    parser.add_argument("--agg_col", required=True, help="Column with numeric values to be aggregated.")
    parser.add_argument("--payload_cols", nargs='+', required=True, help="One or more payload columns to carry through.")
    parser.add_argument("--variant", choices=["default", "opaque_shared_memory"], default="default")
    parser.add_argument("--no_cleanup", action="store_true")
    args = parser.parse_args()

    temp_dir = Path(f"tmp_operator2_{os.getpid()}")
    output_path = Path(os.path.expanduser(args.output_path))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        obliviator_aggregate(
            os.path.expanduser(args.filepath),
            output_path,
            args.group_by_col,
            args.agg_col,
            args.payload_cols,
            temp_dir,
            args.variant
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
