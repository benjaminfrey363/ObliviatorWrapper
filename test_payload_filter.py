import subprocess
import re
import os
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import time

# --- Configuration ---
# IMPORTANT: Adjust this path to match your filter operator's location.
C_HEADER_PATH = Path(os.path.expanduser("~/obliviator/operator_1/common/elem_t.h"))
OPERATOR_SCRIPT = "operator1.py" # Assumes a version of operator1.py that bypasses relabeling

# --- Test Parameters ---
PAYLOAD_SIZES = [32, 50, 64, 128, 256, 512, 1024]
TEST_MESSAGE_ID = "2336463350747" # The ID we know works

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

def run_test():
    """Runs the filter operator script and returns the execution time."""
    output_file = "Big_LDBC/sr4_perf_output.csv"
    time_file = Path(output_file).with_suffix('.time')
    command = [
        "python", OPERATOR_SCRIPT,
        "--filepath", "Big_LDBC/Post.csv",
        "--output_path", output_file,
        "--filter_col", "id",
        "--payload_cols", "content", "creationDate",
        "--filter_threshold_op1", TEST_MESSAGE_ID,
        "--filter_condition_op1", "==",
	"--no_map"
    ]
    try:
        print("Running test with direct payloads...")
        subprocess.run(command, check=True, capture_output=True, text=True)
        if time_file.exists():
            with open(time_file, "r") as f:
                return float(f.read().strip())
        else:
            raise FileNotFoundError(f"Time file not found: {time_file}")
    except subprocess.CalledProcessError as e:
        print("--- TEST FAILED ---")
        print(f"Command failed with exit code {e.returncode}")
        print("STDERR:", e.stderr)
        return None

def main():
    """Main function to orchestrate the performance testing."""
    results = []
    print("=== STARTING FILTER OPERATOR PERFORMANCE TEST ===")
    for size in PAYLOAD_SIZES:
        set_data_length(size)
        time.sleep(1) # Delay to ensure file system changes are registered
        exec_time = run_test()
        if exec_time is not None:
            results.append({"Payload Size (Bytes)": size, "Execution Time (s)": exec_time})

    if not results:
        print("\nNo successful tests were completed. Cannot generate plot.")
        return

    df = pd.DataFrame(results)
    print("\n\n--- PERFORMANCE TEST RESULTS ---")
    print(df)

    # --- Plotting ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))
    ax.plot(df["Payload Size (Bytes)"], df["Execution Time (s)"], marker='o', linestyle='-')
    ax.set_title("Filter Operator Performance vs. Payload Size", fontsize=16)
    ax.set_xlabel("Payload Buffer Size (Bytes)", fontsize=12)
    ax.set_ylabel("Execution Time (s)", fontsize=12)
    ax.set_xscale('log')
    ax.grid(True, which="both", ls="--")

    # --- NEW: Set explicit x-axis labels ---
    ax.set_xticks(PAYLOAD_SIZES)
    ax.get_xaxis().set_major_formatter(mticker.ScalarFormatter())
    # --- End of new code ---

    output_image_path = "filter_performance.png"
    plt.savefig(output_image_path)
    print(f"\n✅ Performance graph saved to {output_image_path}")

if __name__ == "__main__":
    main()
