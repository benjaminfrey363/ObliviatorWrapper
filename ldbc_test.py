
import os
import subprocess
from pathlib import Path
import pandas as pd
import argparse
import shutil
import csv

# Run all LDBC short read queries and capture execution times.
# Randomly sample parameters directly from CSV database

LDBC_DIR = Path("LDBC_SF1")
PERSON_CSV = LDBC_DIR / "Person.csv"
POST_CSV = LDBC_DIR / "Post.csv"
OUTPUT_PATH = Path("ldbc_test_output.txt")

output_times = [ LDBC_DIR / "sr_output" / f"sr{n}_output.time" for n in range(1,8) ]
times = []

for query in range(1,8):
    print(f"\n\n\nRunning SR Query {query}...\n\n\n")
    # Randomly sample parameter
    param = 0
    if query < 4:
        # sample person id
        df = pd.read_csv(str(PERSON_CSV), sep='|')
        param = df['id'].sample(n=1).item()
        param_str = "--person_id"
    else:
        # sample message id
        df = pd.read_csv(str(POST_CSV), sep='|')
        param = df['id'].sample(n=1).item()
        param_str = "--message_id"

    # Run query
    query_cmd = [
        "python", f"short{query}.py",
        param_str, str(param)
    ]
    subprocess.run(query_cmd, check=True, cwd=Path(__file__).parent)

    # Capture time
    with open(str(output_times[query - 1]), 'r') as tf:
        times.append(tf.read())



# Write times to output
output_str = ""
for n in range(1,8):
    output_str += f"Query {n}: {times[n-1]}s\n"
with open(str(OUTPUT_PATH), 'w') as tf:
    tf.write(output_str)

