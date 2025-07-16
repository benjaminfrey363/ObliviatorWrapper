
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

# LDBC Short Read 2
# Recent messages of a person
# Given a start Person with ID $personId, retrieve the last 10 Messages created by that user. For each
# Message, return that Message, the original Post in its conversation (post), and the author of that Post
# (originalPoster). If any of the Messages is a Post, then the original Post (post) will be the same
# Message, i.e. that Message will appear twice in that result.

# For now just for posts

# Using Obliviator:
#
#   Posts:
#       Return last 10 posts by person (for now just all posts).
#       So just FK join Person.csv with Post.csv to get names of all posters, and then filter
#       by person_id to get all posts by the specified person, with their firstName and lastName
def shortread2 (
    person_id: int,
    LDBC_dir_path: str,
    output_path: str,
    no_cleanup: bool
):
    print(f"--- Running LDBC Short Read 2 for Person ID {person_id} ---")
    # create temp directory for the query
    temp_dir = Path(f"tmp_ldbc_sr2_{os.getpid()}")
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:

        # --- Step 1: FK join of Person.csv with Post.csv on CreatorPersonId ---
        print("Step 1: Joining Person.csv with Post.csv on CreatorPersonId")
        join_output_path = temp_dir / "sr7join.csv"
        person_path = LDBC_dir_path + "/Person.csv"
        post_path = LDBC_dir_path + "/Post.csv"
        join_cmd = [
            "python", "fkjoin.py",
            "--table1_path", person_path,
            "--key1", "id",
            "--payload1_cols", "firstName", "lastName",
            "--table2_path", post_path,
            "--key2", "CreatorPersonId",
            "--payload2_cols", "id", "content", "imageFile", "creationDate",
            "--output_path", str(join_output_path)
        ]
        if no_cleanup:
            join_cmd.append("--no_cleanup")
        subprocess.run(join_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator join exited successfully.")

        # At this point columns are
        # t1.id     t1.firstName    t1.lastName     t2.id   t2.content  t2.imageFile    t2.creationDate

        # --- Step 2: Filter by t1.id to get all posts by specified person ---
        print(f"Step 2: Filtering result of join on $person_id = {person_id}")
        filter_cmd = [
            "python", "operator1.py",
            "--filepath", str(join_output_path),
            "--output_path", output_path,
            "--filter_col", "t1.id",
            "--payload_cols", "t2.id", "t2.content", "t2.imageFile", "t2.creationDate", "t2.id", "t1.id", "t1.firstName", "t1.lastName",
            "--filter_threshold_op1", str(person_id),
            "--filter_condition_op1", "=="
        ]
        if no_cleanup:
            filter_cmd.append("--no_cleanup")
        subprocess.run(filter_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator filter exited successfully.")


        print(f"Output of short read 2 written to {output_path}.")

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
        print(f"\n\nTotal time to execute Query 2: {total_time}\n\n")
        with open(filter_time_file, 'w') as tf:
            tf.write(str(total_time))


    except Exception as e:
        print(f"\n--- FATAL ERROR during LDBC SR2 execution: {e} ---")
        raise
    finally:
        if not no_cleanup:
            _cleanup_temp_dir(temp_dir)
            print("Temporary directory cleaned up successfully.")
        else:
            print(f"\nSkipping cleanup of {temp_dir}. Temporary files preserved.")


def main():
    parser = argparse.ArgumentParser(description="Runs LDBC Interactive Short Read 2.")
    parser.add_argument("--person_id", type=int, required=True, help="The ID of the person to look up.")
    parser.add_argument("--LDBC_dir_path", default="LDBC_SF1", help="Path to LDBC database.")
    parser.add_argument("--output_path", default="LDBC_SF1/sr_output/sr2_output.csv", help="Path for the final output CSV file.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories.")
    args = parser.parse_args()

    shortread2(
        args.person_id,
        args.LDBC_dir_path,
        args.output_path,
        args.no_cleanup
    )

if __name__ == "__main__":
    main()


