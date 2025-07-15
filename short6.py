
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

        # FOR NOW RESTRICT TO POSTS.
        # Comments can be replies of comments, creating large chain of hops.
        # Stick to posts, and do the 2 joins.

        # --- Step 1: FK join of Forum.csv with Post.csv to get details of container forum
        print("Step 1: Joining Post.csv with Comment.csv on ParentPostId")
        post_path = LDBC_dir_path + "/Post.csv"
        forum_path = LDBC_dir_path + "/Forum.csv"
        join_output_path = temp_dir / "sr6part1.csv"
        join_cmd = [
            "python", "fkjoin.py",
            "--table1_path", forum_path,
            "--key1", "id",
            "--payload1_cols", "title", "ModeratorPersonId",
            "--table2_path", post_path,
            "--key2", "ContainerForumId",
            "--payload2_cols", "id",
            "--output_path", str(join_output_path)
        ]
        if no_cleanup:
            join_cmd.append("--no_cleanup")
        subprocess.run(join_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator join exited successfully.")


        # At this point structure of table is
        # t1.id         t1.title        t1.ModeratorPersonId    t2.id
        # (Forum id)    (Forum title)   (ID of moderator)       (Post ID)
        

        # --- Step 2: Join result with Person.csv on ModeratorPersonId to get details of moderator
        print("Step 2: Joining Person.csv with resulting table on ModeratorPersonId")
        person_path = LDBC_dir_path + "/Person.csv"
        join2_output_path = temp_dir / "sr6part2.csv"
        join2_cmd = [
            "python", "fkjoin.py",
            "--table1_path", person_path,
            "--key1", "id",
            "--payload1_cols", "firstName", "lastName",
            "--table2_path", join_output_path,
            "--key2", "t1.ModeratorPersonId",
            "--payload2_cols", "t1.id", "t1.title", "t2.id",
            "--output_path", str(join2_output_path)
        ]
        if no_cleanup:
            join2_cmd.append("--no_cleanup")
        subprocess.run(join2_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator join exited successfully.")


        # At this point structure of table is
        # t1.id         t1.firstName    t1.lastName     t2.t1.id        t2.t1.title     t2.t2.id
        # Person ID     Person          Person          Forum ID        Forum title     Post ID


        # --- Step 3: Filter for selected Post
        print("Step 3: Filtering for requested post...")
        filter_cmd = [
            "python", "operator1.py",
            "--filepath", str(join2_output_path),
            "--output_path", output_path,
            "--filter_col", "t2.t2.id",
            "--payload_cols", "t2.t1.id", "t2.t1.title", "t1.id", "t1.firstName", "t1.lastName",
            "--filter_threshold_op1", str(message_id),
            "--filter_condition_op1", "=="
        ]
        if no_cleanup:
            filter_cmd.append("--no_cleanup")
        subprocess.run(filter_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator filter exited successfully.")
        print(f"Output of short read 6 written to {output_path}.")


        # Finally, calculate composite time of all obliviator operations
        total_time = 0.0
        join_time_file = str(join_output_path.with_suffix(".time"))
        with open(join_time_file, 'r') as tf:
            total_time += float(tf.read().strip())
        join2_time_file = str(join2_output_path.with_suffix(".time"))
        with open(join2_time_file, 'r') as tf:
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
        print(f"\n--- FATAL ERROR during LDBC SR6 execution: {e} ---")
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
    parser.add_argument("--LDBC_dir_path", default="LDBC_SF1", help="Path to LDBC database.")
    parser.add_argument("--output_path", default="LDBC_SF1/sr_output/sr6_output.csv", help="Path for the final output CSV file.")
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


