import subprocess
import re
import os
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import time

# --- Configuration ---
# IMPORTANT: Adjust this path to match your fk_join operator's location.
C_HEADER_PATH = Path(os.path.expanduser("~/obliviator/fk_join/common/elem_t.h"))
TABLE1_PATH = "Big_LDBC/Person.csv"
TABLE2_PATH = "Big_LDBC/Post.csv"

# --- Test Parameters ---
PAYLOAD_SIZES_REMAPPED = [32, 42, 46, 47, 48, 49, 50, 54, 64, 128, 256]
PAYLOAD_REMAPPED_TICKS = [32, 42, 46, 50, 54, 64, 128, 256, 512, 1024]
PAYLOAD_SIZES_DIRECT = [1024, 512, 256, 128]

def set_data_length(size: int):
    """Finds and replaces DATA_LENGTH in the C header file."""
    print(f"\n--- Setting DATA_LENGTH to {size} ---")
    if not C_HEADER_PATH.exists():
        raise FileNotFoundError(f"C header file not found at: {C_HEADER_PATH}")
    try:
        with open(C_HEADER_PATH, "r") as f:
            content = f.read()
        new_content, count = re.subn(r"(#define\s+DATA_LENGTH\s+)\d+", rf"\g<1>{size}", content)
        if count == 0:
            raise ValueError(f"Could not find '#define DATA_LENGTH' in {C_HEADER_PATH}")
        with open(C_HEADER_PATH, "w") as f:
            f.write(new_content)
        print(f"Successfully set DATA_LENGTH to {size}")
    except Exception as e:
        print(f"Error modifying C header file: {e}")
        raise

def run_test(use_remapping: bool):
    """Runs the fkjoin.py script and returns the execution time."""
    output_file = "Big_LDBC/perf_test_output.csv"
    time_file = Path(output_file).with_suffix('.time')
    command = [
        "python", "fkjoin.py",
        "--table1_path", TABLE1_PATH, "--key1", "id",
        "--payload1_cols", "firstName", "lastName", "email", "LocationCityId",
        "--table2_path", TABLE2_PATH, "--key2", "CreatorPersonId",
        "--payload2_cols", "creationDate", "content", "imageFile",
        "--output_path", output_file
    ]
    if not use_remapping:
        command.append("--no_map")
        print("Running test with DIRECT PAYLOADS...")
    else:
        print("Running test with REMAPPING...")
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        if time_file.exists():
            with open(time_file, "r") as f:
                return float(f.read().strip())
        else:
            raise FileNotFoundError(f"Time file not found: {time_file}")
    except subprocess.CalledProcessError as e:
        print("--- TEST FAILED ---")
        print(f"STDERR:", e.stderr)
        return None

def main():
    """Main function to orchestrate the performance testing."""
    results = []
    print("\n\n=== STARTING REMAPPING PERFORMANCE TEST ===")
    for size in PAYLOAD_SIZES_REMAPPED:
        set_data_length(size)
        time.sleep(1)
        exec_time = run_test(use_remapping=True)
        if exec_time is not None:
            results.append({"Method": "Remapping", "Payload Size (Bytes)": size, "Execution Time (s)": exec_time})

    print("\n\n=== STARTING DIRECT PAYLOAD PERFORMANCE TEST ===")
    for size in PAYLOAD_SIZES_DIRECT:
        set_data_length(size)
        time.sleep(1)
        exec_time = run_test(use_remapping=False)
        if exec_time is not None:
            results.append({"Method": "Direct Payload", "Payload Size (Bytes)": size, "Execution Time (s)": exec_time})

    if not results:
        print("\nNo successful tests were completed. Cannot generate plot.")
        return

    df = pd.DataFrame(results)
    print("\n\n--- PERFORMANCE TEST RESULTS ---")
    print(df)

    # --- Plotting ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))
    df_remapped = df[df["Method"] == "Remapping"]
    df_direct = df[df["Method"] == "Direct Payload"]
    ax.plot(df_remapped["Payload Size (Bytes)"], df_remapped["Execution Time (s)"], marker='o', linestyle='--', label="Remapping Payloads")
    ax.plot(df_direct["Payload Size (Bytes)"], df_direct["Execution Time (s)"], marker='s', linestyle='-', label="Direct Payloads (Truncated)")
    ax.set_title("Obliviator FK Join Performance: Remapping vs. Direct Payload", fontsize=16)
    ax.set_xlabel("Payload Buffer Size (Bytes)", fontsize=12)
    ax.set_ylabel("Execution Time (s)", fontsize=12)
    ax.set_xscale('log')
    ax.legend()
    ax.grid(True, which="both", ls="--")

    # --- NEW: Set explicit x-axis labels ---
    all_sizes = sorted(list(set(PAYLOAD_REMAPPED_TICKS + PAYLOAD_SIZES_DIRECT)))
    ax.set_xticks(all_sizes)
    ax.get_xaxis().set_major_formatter(mticker.ScalarFormatter())
    # --- End of new code ---

    output_image_path = "fk_join_performance.png"
    plt.savefig(output_image_path)
    print(f"\n✅ Performance graph saved to {output_image_path}")

if __name__ == "__main__":
    main()
