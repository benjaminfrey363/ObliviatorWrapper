import subprocess
import re
import os
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import time

# --- Configuration ---
# Path to the C header file containing the DATA_LENGTH definition.
# IMPORTANT: Adjust this path to match your fk_join operator's location.
C_HEADER_PATH = Path(os.path.expanduser("~/obliviator/fk_join/common/elem_t.h"))

# Paths to the LDBC data files.
TABLE1_PATH = "Big_LDBC/Person.csv"
TABLE2_PATH = "Big_LDBC/Post.csv"

# --- Test Parameters ---
# Buffer sizes to test for the remapping method (to check for the "sweet spot").
PAYLOAD_SIZES_REMAPPED = [32, 50, 64, 128, 256]

# Buffer sizes to test for the direct payload method.
# NOTE: Start with a large size to avoid truncation errors, then work down.
PAYLOAD_SIZES_DIRECT = [1024, 512, 256, 128]

def set_data_length(size: int):
    """
    Finds and replaces the DATA_LENGTH macro in the specified C header file.
    This function is critical for automating the tests.
    """
    print(f"\n--- Setting DATA_LENGTH to {size} ---")
    if not C_HEADER_PATH.exists():
        raise FileNotFoundError(f"C header file not found at: {C_HEADER_PATH}")

    try:
        with open(C_HEADER_PATH, "r") as f:
            content = f.read()

        # Use a regular expression to safely find and replace the #define line.
        new_content, num_replacements = re.subn(
            r"(#define\s+DATA_LENGTH\s+)\d+",
            rf"\g<1>{size}",
            content
        )

        if num_replacements == 0:
            raise ValueError(f"Could not find '#define DATA_LENGTH' in {C_HEADER_PATH}")

        with open(C_HEADER_PATH, "w") as f:
            f.write(new_content)
        print(f"Successfully set DATA_LENGTH to {size} in {C_HEADER_PATH}")

    except Exception as e:
        print(f"Error modifying C header file: {e}")
        raise

def run_test(use_remapping: bool):
    """
    Runs the fkjoin.py script and returns the execution time.
    It recompiles the C code for each run to apply the new DATA_LENGTH.
    """
    # Define the command to run the FK join.
    output_file = "Big_LDBC/perf_test_output.csv"
    time_file = Path(output_file).with_suffix('.time')

    command = [
        "python", "fkjoin.py",
        "--table1_path", TABLE1_PATH,
        "--key1", "id",
        "--payload1_cols", "firstName", "lastName", "email", "LocationCityId",
        "--table2_path", TABLE2_PATH,
        "--key2", "CreatorPersonId",
        "--payload2_cols", "creationDate", "content", "imageFile",
        "--output_path", output_file
    ]

    # Conditionally add the --no_map flag.
    if not use_remapping:
        command.append("--no_map")
        print("Running test with DIRECT PAYLOADS...")
    else:
        print("Running test with REMAPPING...")

    try:
        # Run the command. This will recompile the C code with the new DATA_LENGTH.
        subprocess.run(command, check=True, capture_output=True, text=True)

        # Read the execution time from the generated .time file.
        if time_file.exists():
            with open(time_file, "r") as f:
                return float(f.read().strip())
        else:
            raise FileNotFoundError(f"Time file not found: {time_file}")

    except subprocess.CalledProcessError as e:
        print("--- TEST FAILED ---")
        print(f"Command failed with exit code {e.returncode}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return None # Return None to indicate failure

def main():
    """
    Main function to orchestrate the performance testing.
    """
    results = []

    # --- Test 1: Remapping Method ---
    print("\n\n=== STARTING REMAPPING PERFORMANCE TEST ===")
    for size in PAYLOAD_SIZES_REMAPPED:
        set_data_length(size)
        # Add a small delay to ensure file system changes are registered.
        time.sleep(1)
        exec_time = run_test(use_remapping=True)
        if exec_time is not None:
            results.append({
                "Method": "Remapping",
                "Payload Size (Bytes)": size,
                "Execution Time (s)": exec_time
            })

    # --- Test 2: Direct Payload Method ---
    print("\n\n=== STARTING DIRECT PAYLOAD PERFORMANCE TEST ===")
    for size in PAYLOAD_SIZES_DIRECT:
        set_data_length(size)
        time.sleep(1)
        exec_time = run_test(use_remapping=False)
        if exec_time is not None:
            results.append({
                "Method": "Direct Payload",
                "Payload Size (Bytes)": size,
                "Execution Time (s)": exec_time
            })

    # --- Process and Display Results ---
    if not results:
        print("\nNo successful tests were completed. Cannot generate plot.")
        return

    print(results)
    df = pd.DataFrame(results)
    print("\n\n--- PERFORMANCE TEST RESULTS ---")
    print(df)

    # --- Plotting ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))

    # Separate the data for plotting
    df_remapped = df[df["Method"] == "Remapping"]
    df_direct = df[df["Method"] == "Direct Payload"]

    ax.plot(df_remapped["Payload Size (Bytes)"], df_remapped["Execution Time (s)"],
            marker='o', linestyle='--', label="Remapping Payloads")

    ax.plot(df_direct["Payload Size (Bytes)"], df_direct["Execution Time (s)"],
            marker='s', linestyle='-', label="Direct Payloads (Truncated)")

    ax.set_title("Obliviator FK Join Performance: Remapping vs. Direct Payload", fontsize=16)
    ax.set_xlabel("Payload Buffer Size (Bytes)", fontsize=12)
    ax.set_ylabel("Execution Time (s)", fontsize=12)
    ax.set_xscale('log')
    ax.legend()
    ax.grid(True, which="both", ls="--")

    # Save the plot to a file
    output_image_path = "fk_join_performance.png"
    plt.savefig(output_image_path)
    print(f"\n✅ Performance graph saved to {output_image_path}")


if __name__ == "__main__":
    main()
