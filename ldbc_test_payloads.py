import os
import subprocess
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Import the function from your payload setting script
from set_payload import set_payloads

# --- Configuration ---
LDBC_DIR = Path("LDBC_SF1")
PERSON_CSV = LDBC_DIR / "Person.csv"
POST_CSV = LDBC_DIR / "Post.csv"
RESULTS_CSV_PATH = Path("payload_evaluation_results.csv")
PLOT_PATH = Path("payload_evaluation_plot.png")

# Define the payload sizes you want to test
PAYLOAD_SIZES_TO_TEST = [10, 25, 50, 75, 100, 150, 200]

# --- Helper Functions ---

def rebuild_operators():
    """Runs 'make clean' and 'make' for all C++ operators."""
    print("\n--- Rebuilding all operators ---")
    operator_dirs = [
        Path(os.path.expanduser("~/obliviator/operator_1")),
        Path(os.path.expanduser("~/obliviator/fk_join")),
        Path(os.path.expanduser("~/obliviator/join")),
    ]
    
    for code_dir in operator_dirs:
        if not code_dir.exists():
            print(f"Warning: Directory not found, skipping build: {code_dir}")
            continue
        try:
            print(f"Building in {code_dir}...")
            # Using capture_output=True to keep the console clean
            subprocess.run(["make", "clean"], cwd=code_dir, check=True, capture_output=True)
            subprocess.run(["make"], cwd=code_dir, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            print(f"FATAL ERROR: Build failed in {code_dir}.")
            print(f"STDERR:\n{e.stderr.decode()}")
            raise  # Stop the script if a build fails
    print("--- Rebuild complete ---")


def run_all_queries():
    """Runs all 7 LDBC short read queries and returns the execution times."""
    query_times = []
    output_time_files = [LDBC_DIR / "sr_output" / f"sr{n}_output.time" for n in range(1, 8)]

    for query_num in range(1, 8):
        print(f"  Running SR Query {query_num}...")
        param = 0
        if query_num < 4:
            df = pd.read_csv(str(PERSON_CSV), sep='|')
            param = df['id'].sample(n=1).item()
            param_str = "--person_id"
        else:
            df = pd.read_csv(str(POST_CSV), sep='|')
            param = df['id'].sample(n=1).item()
            param_str = "--message_id"

        query_cmd = ["python", f"short{query_num}.py", param_str, str(param)]
        subprocess.run(query_cmd, check=True, cwd=Path(__file__).parent)

        with open(str(output_time_files[query_num - 1]), 'r') as tf:
            time_str = tf.read().strip()
            query_times.append(float(time_str))
            
    return query_times

# --- Main Execution Logic ---

def main():
    """Main function to run the evaluation."""
    all_results = []

    for size in PAYLOAD_SIZES_TO_TEST:
        print(f"\n{'='*50}\nEVALUATING PAYLOAD SIZE: {size}\n{'='*50}")
        
        # 1. Set payload size and rebuild the C++ code
        set_payloads(size)
        #rebuild_operators()
        
        # 2. Run all queries and get their times
        times = run_all_queries()
        
        # 3. Store the results
        for i, time_val in enumerate(times):
            query_name = f"Query {i+1}"
            all_results.append({
                "payload_size": size,
                "query": query_name,
                "time": time_val
            })
    
    # --- Process and Save Results ---
    
    # 4. Create a DataFrame from the results
    results_df = pd.DataFrame(all_results)
    
    print("\n--- Evaluation Complete ---")
    print("Results Table:")
    print(results_df)
    
    # 5. Save the table to a CSV file
    results_df.to_csv(RESULTS_CSV_PATH, index=False)
    print(f"\nResults saved to '{RESULTS_CSV_PATH}'")
    
    # 6. Create and save the plot
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 8))
    
    sns.lineplot(
        data=results_df,
        x="payload_size",
        y="time",
        hue="query",
        marker="o",
        ax=ax
    )
    
    ax.set_title("Query Execution Time vs. Payload Size", fontsize=16)
    ax.set_xlabel("Payload Size (bytes)", fontsize=12)
    ax.set_ylabel("Execution Time (seconds)", fontsize=12)
    ax.legend(title="LDBC Short Query")
    
    plt.tight_layout()
    plt.savefig(PLOT_PATH)
    print(f"Plot saved to '{PLOT_PATH}'")
    # plt.show() # Uncomment to display the plot directly

if __name__ == "__main__":
    main()