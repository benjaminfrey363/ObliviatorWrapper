import os
from pathlib import Path
import argparse
import shutil
import csv

# Import the core logic functions from our operator wrappers
from operator1 import obliviator_operator1
from fkjoin import obliviator_fk_join
from operator2 import obliviator_aggregate

# Import the formatting helpers
from obliviator_formatting.format_sr2 import format_for_sr2
from obliviator_formatting.format_for_oblivious_sort import format_for_sort

def _cleanup_temp_dir(temp_dir_path: Path):
    if temp_dir_path.exists() and temp_dir_path.is_dir():
        print(f"\nCleaning up temporary directory: {temp_dir_path}...")
        shutil.rmtree(temp_dir_path)

def run_ldbc_sr2(
    person_id: int,
    person_file: Path,
    post_file: Path,
    comment_file: Path,
    output_path: Path,
    no_cleanup: bool
):
    """
    Executes LDBC Short Read 2 with a fully oblivious pipeline.
    """
    print(f"--- Running LDBC Short Read 2 for Person ID: {person_id} ---")
    
    temp_dir = Path(f"tmp_ldbc_sr2_{os.getpid()}").resolve()
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:
        # --- Step 1: Unify Posts and Comments ---
        print("\nStep 1: Unifying message data...")
        unified_messages_path = temp_dir / "unified_messages.csv"
        format_for_sr2(str(post_file), str(comment_file), str(unified_messages_path))

        # --- Step 2 & 3: Perform Joins ---
        print("\nStep 2: Joining messages with posts and persons...")
        join1_path = temp_dir / "join1_output.csv"
        obliviator_fk_join(str(post_file), "id", ["CreatorPersonId"], str(unified_messages_path), "originalPostId", ["messageId", "messageCreationDate", "messageContent", "messageCreatorId"], temp_dir / "t1", join1_path, "default")
        
        join2_path = temp_dir / "join2_output.csv"
        obliviator_fk_join(str(person_file), "id", ["firstName", "lastName"], str(join1_path), "CreatorPersonId", ["originalPostId", "messageId", "messageCreationDate", "messageContent", "messageCreatorId"], temp_dir / "t2", join2_path, "default")

        # --- Step 4: Filter for target person's messages ---
        print("\nStep 4: Filtering for target person's messages...")
        filtered_path = temp_dir / "filtered_messages.csv"
        obliviator_operator1(str(join2_path), temp_dir / "t3", filtered_path, "default", "messageCreatorId", ['id', 'firstName', 'lastName', 'originalPostId', 'messageId', 'messageCreationDate', 'messageContent'], person_id, "==")
        
        # --- Step 5: Oblivious Sort via Aggregation ---
        print("\nStep 5: Performing oblivious sort...")
        sort_input_path = temp_dir / "sort_input.csv"
        format_for_sort(str(filtered_path), str(sort_input_path))
        
        sorted_agg_output = temp_dir / "sorted_agg_output.csv"
        obliviator_aggregate(str(sort_input_path), sorted_agg_output, "sort_key", "dummy_agg_value", ["payload"], temp_dir / "t4", "default")

        # --- Step 6: Final Processing and Limit ---
        print("\nStep 6: Finalizing output...")
        final_rows = []
        with open(sorted_agg_output, 'r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            original_header = ['id', 'firstName', 'lastName', 'originalPostId', 'messageId', 'messageCreationDate', 'messageContent', 'messageCreatorId']
            for row in reader:
                payload_str = row['payload']
                original_values = payload_str.split(',')
                original_row = dict(zip(original_header, original_values))
                final_rows.append(original_row)
        
        top_10_rows = final_rows[:10]
        
        final_data_to_write = [{'messageId': r['messageId'], 'messageContent': r['messageContent'], 'messageCreationDate': r['messageCreationDate'], 'originalPostId': r['originalPostId'], 'originalPosterId': r['id'], 'originalPosterFirstName': r['firstName'], 'originalPosterLastName': r['lastName']} for r in top_10_rows]
        
        final_header = ['messageId', 'messageContent', 'messageCreationDate', 'originalPostId', 'originalPosterId', 'originalPosterFirstName', 'originalPosterLastName']
        with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=final_header)
            writer.writeheader()
            writer.writerows(final_data_to_write)

        print(f"\n✅ LDBC Short Read 2 complete. Final output at: {output_path}")

    finally:
        if not no_cleanup:
            _cleanup_temp_dir(temp_dir)

def main():
    parser = argparse.ArgumentParser(description="Runs LDBC Interactive Short Read 2.")
    parser.add_argument("--person_id", type=int, required=True)
    parser.add_argument("--person_file", default="LDBC/data/Person.csv")
    parser.add_argument("--post_file", default="LDBC/data/Post.csv")
    parser.add_argument("--comment_file", default="LDBC/data/Comment.csv")
    parser.add_argument("--output_path", default="sr2_output.csv")
    parser.add_argument("--no_cleanup", action="store_true")
    args = parser.parse_args()

    # Use absolute paths for all file inputs to ensure robustness
    run_ldbc_sr2(
        args.person_id,
        Path(args.person_file).resolve(),
        Path(args.post_file).resolve(),
        Path(args.comment_file).resolve(),
        Path(args.output_path).resolve(),
        args.no_cleanup
    )

if __name__ == "__main__":
    main()
