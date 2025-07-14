
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

# LDBC Short Read 4
# Content of a message
# Given a Message (post or comment) with ID $messageId, retrieve its content and creation date.

# Using obliviator:
#   Filter Post.csv and Message.csv on $messageId - retrieve unique message
#   Project to retrieve content and creation date. This is accomplished by specifying
#   payload columns in the obliviator filter call.

def shortread4 (
    message_id: int,
    LDBC_dir_path: str,
    output_path: str,
    no_cleanup: bool
):
    print(f"--- Running LDBC Short Read 4 for Message ID {message_id} ---")
    # create temp directory
    temp_dir = Path(f"tmp_ldbc_sr1_{os.getpid()}")
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:
        # TO-DO: Combine Post.csv and Comment.csv into a single Message.csv file.
        # deal with this later, documentation unclear and doesn't really matter as long as consistent
        
        # --- Step 1: Create "Message.csv" from Post.csv and Comment.csv ---



        # --- Step 2: Filter "Message.csv" on $messageId ---

        print("Step 2: Filtering combined Message.csv")
        message_path = LDBC_dir_path + "/Post.csv"  # temporary
        filter_cmd = [
            "python", "operator1.py",
            "--filepath", str(message_path),
            "--output_path", output_path,
            "--filter_col", "id",
            "--payload_cols", "content", "creationDate",
            "--filter_threshold_op1", str(message_id),
            "--filter_condition_op1", "==",
            "--operator1_variant", "default"
        ]
        if no_cleanup:
            filter_cmd.append("--no_cleanup")

        # Run obliviator operator
        print("Running obliviator filter with passed arguments...")
        subprocess.run(filter_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator filter exited successfully.")

        # That's it for this one!

    
    except Exception as e:
        print(f"\n--- FATAL ERROR during LDBC SR1 execution: {e} ---")
        raise
    finally:
        if not no_cleanup:
            _cleanup_temp_dir(temp_dir)
            print("Temporary directory cleaned up successfully.")
        else:
            print(f"\nSkipping cleanup of {temp_dir}. Temporary files preserved.")


def main():
    parser = argparse.ArgumentParser(description="Runs LDBC Interactive Short Read 1.")
    parser.add_argument("--message_id", type=int, required=True, help="The ID of the Post or Comment to look up.")
    parser.add_argument("--LDBC_dir_path", default="LDBC_SF1", help="Path to LDBC database.")
    parser.add_argument("--output_path", default="LDBC_SF1/sr_output/sr4_output.csv", help="Path for the final output CSV file.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories.")
    args = parser.parse_args()

    shortread4(
        args.message_id,
        args.LDBC_dir_path,
        args.output_path,
        args.no_cleanup
    )

if __name__ == "__main__":
    main()
