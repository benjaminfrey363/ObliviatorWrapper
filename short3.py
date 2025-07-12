
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

# LDBC Short Read 3
# Friends of a person
# Given a start Person with ID $personId, retrieve all of their friends,
# and the date at which they became friends.

# Using obliviator:
#   NFK join of Person_knows_Person.csv with Person.csv on Person2Id - find all friends of all people
#   Filter for desired $personId

def shortread3 (
    person_id: int,
    LDBC_dir_path: str,
    output_path: str,
    no_cleanup: bool
):
    print(f"--- Running LDBC Short Read 3 for Person ID {person_id} ---")
    # create temp directory for the query
    temp_dir = Path(f"tmp_ldbc_sr3_{os.getpid()}")
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:

        # --- Step 1: NFK join of Person.csv with Person_knows_Person.csv on Person1Id
        #       find all friends of all people
        print("Step 1: Joining Person_knows_Person.csv with Person.csv on Person2Id")
        person_path = LDBC_dir_path + "/Person.csv"
        edge_path = LDBC_dir_path + "/Person_knows_Person.csv"
        join1_output_path = temp_dir / "sr3part1.csv"
        join1_cmd = [
            "python", "join.py",
            "--table1_path", edge_path,
            "--key1", "Person2Id",
            "--payload1_cols", "Person1Id", "creationDate",
            "--table2_path", person_path,
            "--key2", "id",
            "--payload2_cols", "firstName", "lastName",
            "--output_path", str(join1_output_path)
        ]
        if no_cleanup:
            join1_cmd.append("--no_cleanup")
        subprocess.run(join1_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator join exited successfully.")

        # At this point columns are
        # t1.Person2Id|t1.Person1Id|t1.creationDate|t2.firstName|t2.lastName
        # filter this file on t1.Person1Id to find friends of person

        # --- Step 2: Filter this joined output on t1.Person1Id to get details of
        #       friends of specified person
        print(f"Step 2: Filtering result of join on $Person1Id = {person_id}")
        filter_cmd = [
            "python", "operator1.py",
            "--filepath", str(join1_output_path),
            "--output_path", output_path,
            "--filter_col", "t1.Person1Id",
            "--payload_cols", "t1.Person2Id", "t1.creationDate", "t2.firstName", "t2.lastName",
            "--filter_threshold_op1", str(person_id),
            "--filter_condition_op1", "=="
        ]
        if no_cleanup:
            filter_cmd.append("--no_cleanup")
        subprocess.run(filter_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator filter exited successfully.")
        print(f"Output of short read 3 written to {output_path}.")

    
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
    parser = argparse.ArgumentParser(description="Runs LDBC Interactive Short Read 3.")
    parser.add_argument("--person_id", type=int, required=True, help="The ID of the person to look up.")
    parser.add_argument("--LDBC_dir_path", default="Big_LDBC", help="Path to LDBC database.")
    parser.add_argument("--output_path", default="Big_LDBC/sr_output/sr3_output.csv", help="Path for the final output CSV file.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories.")
    args = parser.parse_args()

    shortread3(
        args.person_id,
        args.LDBC_dir_path,
        args.output_path,
        args.no_cleanup
    )

if __name__ == "__main__":
    main()
