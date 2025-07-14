
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

# LDBC Short Read 6
# Forum of a message
# Given a Message with ID $messageId, retrieve the Forum that contains it and the Person that moderates that Forum. 
# Since Comments are not directly contained in Forums, for Comments, return the
# Forum containing the original Post in the thread which the Comment is replying to.

# For now assume taking comments as arguments, more difficult query

# Using obliviator:
#       Not necessary to join Comment.csv with Post.csv to get details of parent post. Only need parent post ID to join with forum
#       
#       Posts:
#           Join Post.csv with Forum.csv on ContainerForumId to get details of container forum
#           Join resulting table with Person.csv on ModeratorPersonId to get requested details of moderator
#           Filter for requested message
#
#       Comments:
#           Join Comment.csv with Post.csv on ParentPostId to get the ContainerForumId of parent post
#           (same from here)
#           Join result with Forum.csv on ContainerForumId to get details of container forum
#           Join resulting table with Person.csv on ModeratorPersonId to get requested details of moderator
#           Filter for requested message
#
def shortread6 (
    message_id: int,
    LDBC_dir_path: str,
    output_path: str,
    no_cleanup: bool
):
    print(f"--- Running LDBC Short Read 6 for Message ID {message_id} ---")
    # create temp directory for the query
    temp_dir = Path(f"tmp_ldbc_sr6_{os.getpid()}")
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:

        # For now do Comment implementation, figure out Comment/Post discrimination later

        # --- Step 1: FK join of Post.csv with Comment.csv (primary key) on ParentPostId to get ContainerForumId of parent post of comment
        print("Step 1: Joining Post.csv with Comment.csv on ParentPostId")
        post_path = LDBC_dir_path + "/Post.csv"
        comment_path = LDBC_dir_path + "/Comment.csv"
        join_output_path = temp_dir / "sr6part1.csv"
        join_cmd = [
            "python", "fkjoin.py",
            "--table1_path", post_path,
            "--key1", "id",
            "--payload1_cols", "ContainerForumId",
            "--table2_path", comment_path,
            "--key2", "ParentPostId",
            "--payload2_cols", "id",
            "--output_path", str(join_output_path)
        ]
        if no_cleanup:
            join_cmd.append("--no_cleanup")
        subprocess.run(join_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator join exited successfully.")


        # After execution, table format is





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
    parser = argparse.ArgumentParser(description="Runs LDBC Interactive Short Read 6.")
    parser.add_argument("--message_id", type=int, required=True, help="The ID of the message to look up.")
    parser.add_argument("--LDBC_dir_path", default="Big_LDBC", help="Path to LDBC database.")
    parser.add_argument("--output_path", default="Big_LDBC/sr_output/sr6_output.csv", help="Path for the final output CSV file.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories.")
    args = parser.parse_args()

    shortread6(
        args.message_id,
        args.LDBC_dir_path,
        args.output_path,
        args.no_cleanup
    )

if __name__ == "__main__":
    main()


