# OBLIVIATOR WRAPPER
# Project Overview
This project provides a suite of Python wrappers designed to automate and simplify the use of the OBLIVIATOR secure computation framework, particularly for its various join and operator functionalities. OBLIVIATOR (as described in "OBLIVIATOR: OBLIVIous Parallel Joins and other OperATORs in Shared Memory Environments") is a tool for privacy-preserving record linkage and other database operations within trusted execution environments (TEEs) like Intel SGX.

While OBLIVIATOR itself is a powerful C-based tool, its command-line interface often requires specific input formats and manual orchestration of multiple steps (e.g., formatting, relabeling, running the binary, and post-processing). This wrapper aims to abstract these complexities, making OBLIVIATOR more user-friendly and enabling seamless execution of complex query pipelines.

# Features
Generalized Join Operations: Wrappers for OBLIVIATOR's KKS Join (join_kks) and Foreign Key Join (fk_join).

Operator Support: Wrappers for OBLIVIATOR's "Operator 1" (projection/filter) and "Operator 2" (complex transformation).

Seamless Operator 3 Pipeline: Orchestrates the multi-step "Operator 3" (Filter -> Join -> Aggregate) as a single, configurable pipeline.

Automated Pre/Post-processing: Handles all necessary data formatting, ID relabeling, and reverse ID relabeling internally.

Dynamic Filter Parameterization: For Operator 3's filter step, allows modifying the hardcoded filter threshold in the OBLIVIATOR C source code at runtime (with automated recompilation and reversion).

Flexible CSV Inputs: Accepts standard CSV files as inputs for operations, with column selection for formatting.

Variant Support: Supports both default and opaque_shared_memory variants of OBLIVIATOR's operators.

Temporary File Management: Creates and manages temporary directories and intermediate files generated during execution.

# Prerequisites
Before using these Python wrappers, you must have the following set up in your Linux environment (e.g., an Azure VM with SGX enabled):

Python 3.8+: The scripts are written in Python 3.

pandas library:

pip install pandas

OBLIVIATOR Repository:

You must have the OBLIVIATOR repository cloned and built on your system.

The scripts expect the OBLIVIATOR root directory to be located at ~/obliviator/.

Ensure all necessary OBLIVIATOR binaries (e.g., host/parallel, enclave/parallel_enc.signed) are compiled and executable within their respective subdirectories (e.g., ~/obliviator/join_kks, ~/obliviator/fk_join, ~/obliviator/operator_1, ~/obliviator/operator_2, ~/obliviator/operator_3/3_1, etc.).

This typically involves running make in each operator's directory.

OpenEnclave SDK & Build Tools: Ensure all prerequisites for building OBLIVIATOR (OpenEnclave SDK, oesign, oeedger8r, openssl, mbedtls, mpich, gcc/cc) are correctly installed and configured in your environment as per OBLIVIATOR's documentation.

CRITICAL MANUAL STEP for Operator 3 Filter:

For the operator3.py wrapper to dynamically change the filter threshold, you must manually edit one line in OBLIVIATOR's source code:

File: /home/azureuser/obliviator/operator_3/3_1/enclave/scalable_oblivious_join.c

Find (around lines 94 and 114):

control_bit[i] = (19800101 <= arr[i].key);

Change 19800101 to:

control_bit[i] = (FILTER_PLACEHOLDER_VALUE <= arr[i].key);

Ensure FILTER_PLACEHOLDER_VALUE is typed exactly as shown (no quotes).

Save the file. This placeholder allows the Python script to replace the value and then revert it after use.

# Project Structure
.
├── join.py                 # Wrapper for KKS Join
├── fkjoin.py               # Wrapper for Foreign Key Join
├── operator1.py            # Wrapper for Operator 1
├── operator3.py            # Wrapper for Operator 3 Pipeline (Filter -> Join -> Aggregate)
├── obliviator_formatting/  # Contains helper scripts for data formatting and ID relabeling
│   ├── format_join.py
│   ├── format_operator1.py
│   ├── format_operator2.py
│   ├── format_operator3_1.py
│   ├── format_operator3_2.py
│   ├── format_operator3_3.py
│   ├── relabel_ids.py          # Generic ID relabeling script
│   ├── reverse_relabel_ids.py  # Generic reverse ID relabeling script
│   ├── transform_3_1_output_to_3_2_input.py # Transforms output of 3_1 for 3_2 input
│   └── transform_3_2_output_to_3_3_input.py # Transforms output of 3_2 for 3_3 input
└── data/                   # Directory to place your input CSV files for testing
    ├── my_filter_test_with_dates.csv
    └── my_join_second_table.csv
    └── ... (other input files)

# Usage Examples
Place your custom CSV input files (e.g., my_data.csv, my_second_table.csv) inside the data/ directory or provide their absolute paths.

1. KKS Join (join.py)
Performs a basic equi-join of two CSV files.

python join.py \
    --filepath1 ~/obliviator/flat_csv/dynamic__Person.csv \
    --filepath2 ~/obliviator/flat_csv/dynamic__Comment.csv \
    --join_key1 id \
    --join_key2 CreatorPersonId

2. Foreign Key Join (fkjoin.py)
Performs a foreign key join of two CSV files.

python fkjoin.py \
    --filepath1 ~/obliviator/data/test_person.csv \
    --filepath2 ~/obliviator/data/test_comment.csv \
    --join_key1 id \
    --join_key2 CreatorPersonId \
    --fk_join_variant default  # Or opaque_shared_memory

3. Operator 1 (operator1.py)
Performs a projection/filter-like operation on a single CSV file.

python operator1.py \
    --filepath ~/obliviator/data/my_sample_input.csv \
    --operator1_variant default # Or opaque_shared_memory

4. Operator 3 Pipeline (operator3.py)
This is the most complex wrapper, executing a Filter -> Join -> Aggregate pipeline. You provide the initial CSV, and define columns for filtering and joining at each stage.

Example Input CSVs:

data/my_filter_test_with_dates.csv (Your initial input for filter and Table 1 of join):

record_id,transaction_date,value,description
R1,19790101,10.5,"Before filter date"
R2,19800101,20.0,"Exact filter date"
R3,19850615,30.3,"After filter date"
R4,19701231,5.0,"Way before filter"
R5,20000101,40.1,"Well after filter"

data/my_join_second_table.csv (Your custom second table for the join operation):

order_id,tx_date,item,quantity
ORD001,19800101,Laptop,1
ORD002,19900101,Mouse,2
ORD003,20000101,Keyboard,1
ORD004,20050101,Monitor,1
ORD005,19850615,Webcam,1

Command to run the full pipeline:

python operator3.py \
    --initial_filepath ~/obliviator/data/my_filter_test_with_dates.csv \
    --filter_key_col_3_1 transaction_date \
    --id_col_3_1 record_id \
    --filter_threshold_3_1 19800101 \
    --join_key_col_3_2_A transaction_date \
    --join_key_col_3_2_B_and_values record_id,value \
    --second_table_filepath_3_2 ~/obliviator/data/my_join_second_table.csv \
    --second_table_key_col_3_2 tx_date \
    --second_table_other_cols_3_2 order_id,item,quantity \
    --col1_from_step2_output_3_3 t1_id \
    --col2_from_step2_output_3_3 t1_numeric \
    --col3_from_step2_output_3_3 t2_quantity \
    --operator3_variant default # Or opaque_shared_memory

Explanation of operator3.py Arguments:
--initial_filepath: Path to your primary CSV data.

--filter_key_col_3_1: Column in initial_filepath to filter on (e.g., transaction_date).

--id_col_3_1: Unique ID column in initial_filepath for tracking filtered rows (e.g., record_id).

--filter_threshold_3_1: Numerical threshold for the filter (e.g., 19800101).

--join_key_col_3_2_A: Column from initial_filepath to use as the join key for "Table 1" in the join step.

--join_key_col_3_2_B_and_values: Comma-separated columns from initial_filepath to include as the "value blob" for "Table 1" in the join step.

--second_table_filepath_3_2: Path to your custom CSV file for "Table 2" in the join step.

--second_table_key_col_3_2: Column from second_table_filepath_3_2 to use as the join key for "Table 2".

--second_table_other_cols_3_2: Comma-separated columns from second_table_filepath_3_2 to include as the "value blob" for "Table 2".

--col1_from_step2_output_3_3, --col2_from_step2_output_3_3, --col3_from_step2_output_3_3: These specify how to extract the three input columns for the final aggregation step from the complex string output of the join. The expected values are internal placeholders like t1_id, t1_numeric, t2_id, t2_item, t2_quantity etc., which map to parts parsed from the join output.

# Notes & Troubleshooting
Error Handling: The wrappers include try-except blocks for common issues, but OBLIVIATOR's C binaries can sometimes return non-zero exit codes for non-fatal conditions. If a script unexpectedly stops, check the console output for specific OBLIVIATOR messages.

Permissions: Ensure the user running the Python scripts has read/write access to all input files, output directories (e.g., tmp_*), and the OBLIVIATOR source directories (especially for operator_3/3_1 for filter modification).

Path Expansions: All ~ (home directory) paths in arguments are expanded by the script.

Cleaning: The scripts handle make clean internally. You can manually delete tmp_* directories if needed.

Contributing
Feel free to extend these wrappers for other OBLIVIATOR operations, add more robust error handling, or improve data transformation flexibility.
