import subprocess
import re
import matplotlib.pyplot as plt

def set_data_length(size):
    """Finds and replaces DATA_LENGTH in the C header file."""
    header_path = "operator_1/common/elem_t.h"
    with open(header_path, "r") as f:
        content = f.read()
    
    # Use regex to find and replace the #define line
    new_content = re.sub(r"#define\s+DATA_LENGTH\s+\d+", f"#define DATA_LENGTH {size}", content)
    
    with open(header_path, "w") as f:
        f.write(new_content)
    print(f"Set DATA_LENGTH to {size}")

def run_test():
    """Runs the modified operator1.py script and returns the execution time."""
    # NOTE: This assumes you have a modified operator1_direct.py that bypasses relabeling
    cmd = [
        "python", "operator1.py", # Your modified script
        "--filepath", "Big_LDBC/Post.csv",
        "--output_path", "Big_LDBC/sr4_perf_output.csv",
        "--filter_col", "id",
        "--payload_cols", "content", "creationDate",
        "--filter_threshold_op1", "2336463350747",
        "--filter_condition_op1", "==",
	"--no_map"    
    ]
    subprocess.run(cmd, check=True)
    
    with open("Big_LDBC/sr4_perf_output.time", "r") as f:
        return float(f.read())

# --- Main Test Logic ---
payload_sizes = [32, 50, 64, 128, 256, 512, 1024]
execution_times = []

print("Running warm-up test with payload size 100B.")
# warm up test
set_data_length(100)
run_test()

for size in payload_sizes:
    set_data_length(size)
    time_taken = run_test()
    execution_times.append(time_taken)
    print(f"Execution time for {size}B payload: {time_taken:.4f}s")

# --- Plotting Results ---
plt.figure(figsize=(10, 6))
plt.plot(payload_sizes, execution_times, marker='o')
plt.title('Obliviator Performance vs. Payload Size')
plt.xlabel('Payload Buffer Size (Bytes)')
plt.ylabel('Execution Time (s)')
plt.grid(True)
plt.xscale('log') # Log scale might be useful for the x-axis
plt.savefig('performance_graph.png')
print("\nGraph saved to performance_graph.png")

print(execution_times)
