import os
import subprocess
from pathlib import Path
import argparse
import shutil
import csv

def _cleanup_temp_dir(temp_dir_path: Path):
    if temp_dir_path.exists() and temp_dir_path.is_dir():
        print(f"\nCleaning up temporary directory: {temp_dir_path}...")
        shutil.rmtree(temp_dir_path)

def run_ldbc_sr2(
    person_id: int,
    person_file: str,
    post_file: str,
    comment_file: str,
    output_path: str,
    no_cleanup: bool
):
    """
    Executes LDBC Short Read 2 with a fully oblivious pipeline,
    including an oblivious sort via the aggregation operator.
    """
    print(f"--- Running LDBC Short Read 2 for Person ID: {person_id} ---")
    
    # Use absolute paths for all temporary files to avoid subprocess issues
    temp_dir = Path(f"tmp_ldbc_sr2_{os.getpid()}").resolve()
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:
        # --- Step 1: Unify Posts and Comments ---
        print("\nStep 1: Unifying message data...")
        unified_messages_path = temp_dir / "unified_messages.csv"
        format_cmd = [
            "python", "obliviator_formatting/format_sr2.py",
            "--post_file", post_file,
            "--comment_file", comment_file,
            "--output_path", str(unified_messages_path)
        ]
        subprocess.run(format_cmd, check=True, cwd=Path(__file__).parent)
        print("Message unification complete.")

        # --- Step 2: Join with Posts to get original poster's ID ---
        print("\nStep 2: Joining with original posts...")
        join1_output_path = temp_dir / "join1_output.csv"
        join1_cmd = [
            "python", "fkjoin.py",
            "--table1_path", post_file,
            "--key1", "id",
            "--payload1_cols", "hasCreator",
            "--table2_path", str(unified_messages_path),
            "--key2", "originalPostId",
            "--payload2_cols", "messageId", "messageCreationDate", "messageContent", "messageCreatorId",
            "--output_path", str(join1_output_path)
        ]
        subprocess.run(join1_cmd, check=True, cwd=Path(__file__).parent)
        print("Join with posts complete.")

        # --- Step 3: Join with Person to get original poster's name ---
        print("\nStep 3: Joining with Person table...")
        join2_output_path = temp_dir / "join2_output.csv"
        person_join_cmd = [
            "python", "fkjoin.py",
            "--table1_path", person_file,
            "--key1", "id",
            "--payload1_cols", "firstName", "lastName",
            "--table2_path", str(join1_output_path),
            "--key2", "hasCreator",
            "--payload2_cols", "originalPostId", "messageId", "messageCreationDate", "messageContent", "messageCreatorId",
            "--output_path", str(join2_output_path)
        ]
        subprocess.run(person_join_cmd, check=True, cwd=Path(__file__).parent)
        print("Join with Person table complete.")

        # --- Step 4: Filter for messages by the target person ---
        print("\nStep 4: Filtering for target person's messages...")
        filtered_path = temp_dir / "filtered_messages.csv"
        
        # We need to rename the ambiguous 'hasCreator' column before filtering
        temp_filter_input_path = temp_dir / "filter_input.csv"
        with open(join2_output_path, 'r', encoding='utf-8') as infile, \
             open(temp_filter_input_path, 'w', newline='', encoding='utf-8') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            header = next(reader)
            header[-1] = 'messageCreatorId' 
            writer.writerow(header)
            writer.writerows(reader)
            
        filter_payload_cols = [col for col in header if col != 'messageCreatorId']
        
        filter_cmd = [
            "python", "operator1.py",
            "--filepath", str(temp_filter_input_path),
            "--output_path", str(filtered_path),
            "--filter_col", "messageCreatorId",
            "--payload_cols", *filter_payload_cols,
            "--filter_threshold_op1", str(person_id),
            "--filter_condition_op1", "=="
        ]
        subprocess.run(filter_cmd, check=True, cwd=Path(__file__).parent)
        print("Filtering complete.")

        # --- Step 5: Prepare the filtered data for oblivious sorting ---
        print("\nStep 5: Preparing data for oblivious sort...")
        sort_input_path = temp_dir / "sort_input.csv"
        subprocess.run(["python", "obliviator_formatting/format_for_oblivious_sort.py", "--input_path", str(filtered_path), "--output_path", str(sort_input_path)], check=True)
        print("Data prepared for sorting.")

        # --- Step 6: Perform Oblivious Sort using the Aggregation Operator ---
        print("\nStep 6: Performing oblivious sort via aggregation...")
        sorted_data_path = temp_dir / "sorted_data.csv"
        agg_cmd = [
            "python", "operator2.py",
            "--filepath", str(sort_input_path),
            "--output_path", str(sorted_data_path),
            "--group_by_col", "sort_key",
            "--agg_col", "dummy_agg_value",
            "--payload_cols", "payload"
        ]
        subprocess.run(agg_cmd, check=True)
        print("Oblivious sort complete.")

        # --- Step 7: Final Processing and Limit ---
        print("\nStep 7: Finalizing output...")
        final_rows = []
        with open(sorted_data_path, 'r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                # The payload is in the 'payload' column from the agg output
                payload_str = row['payload']
                # The original header had 'sort_key', 'dummy_agg_value', 'payload'
                # We need to reconstruct the original dictionary from the payload string
                original_header = ['messageCreatorId', 'id', 'firstName', 'lastName', 'originalPostId', 'messageId', 'messageCreationDate', 'messageContent']
                original_values = payload_str.split(',')
                original_row = dict(zip(original_header, original_values))
                final_rows.append(original_row)

        # The data is already sorted, so we just take the top 10
        top_10_rows = final_rows[:10]

        final_data_to_write = []
        for row in top_10_rows:
            final_data_to_write.append({
                'messageId': row['messageId'],
                'messageContent': row['messageContent'],
                'messageCreationDate': row['messageCreationDate'],
                'originalPostId': row['originalPostId'],
                'originalPosterId': row['id'],
                'originalPosterFirstName': row['firstName'],
                'originalPosterLastName': row['lastName']
            })

        final_header = [
            'messageId', 'messageContent', 'messageCreationDate', 
            'originalPostId', 'originalPosterId', 'originalPosterFirstName', 'originalPosterLastName'
        ]
        with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=final_header)
            writer.writeheader()
            writer.writerows(final_data_to_write)

        print(f"\n✅ LDBC Short Read 2 complete. Final output at: {output_path}")

    except Exception as e:
        print(f"\n--- FATAL ERROR during LDBC SR2 execution: {e} ---")
        raise
    finally:
        if not no_cleanup:
            _cleanup_temp_dir(temp_dir)

def main():
    parser = argparse.ArgumentParser(description="Runs LDBC Interactive Short Read 2.")
    parser.add_argument("--person_id", type=int, required=True)
    parser.add_argument("--person_file", default="LDBC/data/Person.csv")
    parser.add_argument("--post_file", default="LDBC/data/Post.csv")
    parser.add_argument("--comment_file", default="LDBC/data/Comment.csv")
    parser.add_argument("--output_path", default="LDBC/data/sr2_output.csv")
    parser.add_argument("--no_cleanup", action="store_true")
    args = parser.parse_args()

    # Ensure all input file paths are absolute before passing them
    run_ldbc_sr2(
        args.person_id,
        os.path.abspath(os.path.expanduser(args.person_file)),
        os.path.abspath(os.path.expanduser(args.post_file)),
        os.path.abspath(os.path.expanduser(args.comment_file)),
        os.path.abspath(os.path.expanduser(args.output_path)),
        args.no_cleanup
    )

if __name__ == "__main__":
    main()
