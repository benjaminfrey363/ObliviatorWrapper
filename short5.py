
import os
import subprocess
from pathlib import Path
import argparse
import shutil
import csv


def _cleanup_temp_dir(temp_dir_path: Path):
    """Removes the specified temporary directory and its contents."""
    if temp_dir_path.exists() and temp_dir_path.is_dir():
        print(f"\nCleaning up temporary directory: {temp_dir_path}...")
        try:
            shutil.rmtree(temp_dir_path)
        except OSError as e:
            print(f"Error cleaning up temporary directory {temp_dir_path}: {e}")

# LDBC Short Read 5
# Creator of a message (post)
# Given a Message with ID $messageId, retrieve its author.

# Using obliviator:
#   FK Join of Person.csv and Message.csv (for now Post.csv), with Message as primary key
#   Filter for specified message_id and retrieve details of person

def shortread5 (
    message_id: int,
    LDBC_dir_path: str,
    output_path: str,
    no_cleanup: bool
):
    print(f"--- Running LDBC Short Read 5 for Message ID {message_id} ---")
    # create temp directory for the query
    temp_dir = Path(f"tmp_ldbc_sr5_{os.getpid()}")
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:

        # --- Step 1: FK join of Perspn.csv with Message.csv (primary key) on CreatorPersonId
        print("Step 1: Joining Person.csv and Post.csv on CreatorPersonId")
        person_path = LDBC_dir_path + "/Person.csv"
        message_path = LDBC_dir_path + "/Post.csv"
        join_output_path = temp_dir / "sr5part1.csv"
        join_cmd = [
            "python", "fkjoin.py",
            "--table1_path", person_path,
            "--key1", "id",
            "--payload1_cols", "firstName", "lastName",
            "--table2_path", message_path,
            "--key2", "CreatorPersonId",
            "--payload2_cols", "id",
            "--output_path", str(join_output_path)
        ]
        if no_cleanup:
            join_cmd.append("--no_cleanup")
        subprocess.run(join_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator join exited successfully.")



        # --- Step 2: Filter for specified message_id
        print(f"Step 2: Filtering result of join on $message_id = {message_id}")
        filter_cmd = [
            "python", "operator1.py",
            "--filepath", str(join_output_path),
            "--output_path", output_path,
            "--filter_col", "t2.id",
            "--payload_cols", "t1.id", "t1.firstName", "t1.lastName",
            "--filter_threshold_op1", str(message_id),
            "--filter_condition_op1", "=="
        ]
        if no_cleanup:
            filter_cmd.append("--no_cleanup")
        subprocess.run(filter_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator filter exited successfully.")
        print(f"Output of short read 3 written to {output_path}.")

    
    except Exception as e:
        print(f"\n--- FATAL ERROR during LDBC SR5 execution: {e} ---")
        raise
    finally:
        if not no_cleanup:
            _cleanup_temp_dir(temp_dir)
            print("Temporary directory cleaned up successfully.")
        else:
            print(f"\nSkipping cleanup of {temp_dir}. Temporary files preserved.")


def main():
    parser = argparse.ArgumentParser(description="Runs LDBC Interactive Short Read 5.")
    parser.add_argument("--message_id", type=int, required=True, help="The ID of the person to look up.")
    parser.add_argument("--LDBC_dir_path", default="Big_LDBC", help="Path to LDBC database.")
    parser.add_argument("--output_path", default="Big_LDBC/sr_output/sr5_output.csv", help="Path for the final output CSV file.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories.")
    args = parser.parse_args()

    shortread5(
        args.message_id,
        args.LDBC_dir_path,
        args.output_path,
        args.no_cleanup
    )

if __name__ == "__main__":
    main()
