
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

# LDBC Short Read 7
# Replies of a message
# Given a Message with ID $messageId, retrieve the (1-hop) Comments that reply to it.
# In addition, return a boolean flag knows indicating if the author of the reply (replyAuthor) knows
# the author of the original message (messageAuthor). If author is same as original author, return
# False for knows flag

# For now just for posts

# Using Obliviator:
#
#   Posts:
#       FK join of Person.csv with Comment.csv on CreatorPersonId
#       Filter on ParentPostId to only get one-hop comments replying to specified post
#       Construct knows column
def shortread7 (
    message_id: int,
    LDBC_dir_path: str,
    output_path: str,
    no_cleanup: bool
):
    print(f"--- Running LDBC Short Read 7 for Message ID {message_id} ---")
    # create temp directory for the query
    temp_dir = Path(f"tmp_ldbc_sr7_{os.getpid()}")
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:

        # --- Step 1: FK join of Person.csv with Comment.csv on CreatorPersonId ---
        print("Step 1: Joining Person.csv with Comment.csv on CreatorPersonId")
        join_output_path = temp_dir / "sr7join.csv"
        comment_path = LDBC_dir_path + "/Comment.csv"
        person_path = LDBC_dir_path + "/Person.csv"
        join_cmd = [
            "python", "fkjoin.py",
            "--table1_path", person_path,
            "--key1", "id",
            "--payload1_cols", "firstName", "lastName",
            "--table2_path", comment_path,
            "--key2", "CreatorPersonId",
            "--payload2_cols", "id", "content", "creationDate", "ParentPostId",
            "--output_path", str(join_output_path)
        ]
        if no_cleanup:
            join_cmd.append("--no_cleanup")
        subprocess.run(join_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator join exited successfully.")

        # At this point columns are
        # t1.id     t1.firstName    t1.lastName     t2.id       t2.content      t2.creationDate     t2.ParentPostId


        # --- Step 2: Filter on t2.ParentPostId to only get comments that are one-hop
        #       replies to specified post ---
        #           Fix: can filter on a column that has missing row elements - these rows are just ignored
        print(f"Step 2: Filtering result of join on $ParentPostId = {message_id}")
        filter_cmd = [
            "python", "operator1.py",
            "--filepath", str(join_output_path),
            "--output_path", output_path,
            "--filter_col", "t2.ParentPostId",
            "--payload_cols", "t2.id", "t2.content", "t2.creationDate", "t1.id", "t1.firstName", "t1.lastName",
            "--filter_threshold_op1", str(message_id),
            "--filter_condition_op1", "=="
        ]
        if no_cleanup:
            filter_cmd.append("--no_cleanup")
        subprocess.run(filter_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator filter exited successfully.")


        # To-do: Construct knows column using edge file - how to do this obliviously?




        print(f"Output of short read 6 written to {output_path}.")

        # Finally, calculate composite time of all obliviator operations

        total_time = 0.0
        join_time_file = str(join_output_path.with_suffix(".time"))
        with open(join_time_file, 'r') as tf:
            total_time += float(tf.read().strip())
        output_path_obj = Path(output_path)
        filter_time_file = str(output_path_obj.with_suffix(".time"))
        with open(filter_time_file, 'r') as tf:
            total_time += float(tf.read().strip())
        # write this compiled time to the <output_path>.time file
        print(f"\n\nTotal time to execute Query 5: {total_time}\n\n")
        with open(filter_time_file, 'w') as tf:
            tf.write(str(total_time))





    except Exception as e:
        print(f"\n--- FATAL ERROR during LDBC SR7 execution: {e} ---")
        raise
    finally:
        if not no_cleanup:
            _cleanup_temp_dir(temp_dir)
            print("Temporary directory cleaned up successfully.")
        else:
            print(f"\nSkipping cleanup of {temp_dir}. Temporary files preserved.")


def main():
    parser = argparse.ArgumentParser(description="Runs LDBC Interactive Short Read 7.")
    parser.add_argument("--message_id", type=int, required=True, help="The ID of the message to look up.")
    parser.add_argument("--LDBC_dir_path", default="LDBC_SF1", help="Path to LDBC database.")
    parser.add_argument("--output_path", default="LDBC_SF1/sr_output/sr7_output.csv", help="Path for the final output CSV file.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories.")
    args = parser.parse_args()

    shortread7(
        args.message_id,
        args.LDBC_dir_path,
        args.output_path,
        args.no_cleanup
    )

if __name__ == "__main__":
    main()

