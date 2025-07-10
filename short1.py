
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

# LDBC Short Read 1
# Profile of a person
# Given a start Person with ID $personId, retrieve their first name, last name, birthday, IP address,
# browser, and city of residence

# Using obliviator:
#   Join Person.csv with Place.csv on LocationCityId, carrying city name as payload
#   Filter to specified personId

def shortread1 (
    person_id: int,
    LDBC_dir_path: str,
    output_path: str,
    no_cleanup: bool
):
    print(f"--- Running LDBC Short Read 1 for Person ID {person_id} ---")
    # create temp directory for the query
    temp_dir = Path(f"tmp_ldbc_sr1_{os.getpid()}")
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:

        # --- Step 1: Join Person.csv with Place.csv on LocationCityId

        print("Step 1: Joining Person.csv with Place.csv on LocationCitId")
        person_path = LDBC_dir_path + "/Person.csv"
        place_path = LDBC_dir_path + "/Place.csv"
        join_output_path = temp_dir / "sr1part1.csv"
        join_cmd = [
            "python", "fkjoin.py",
            "--table1_path", str(person_path),
            "--key1", "LocationCityId",
            "--payload1_cols", "id", "firstName",
            "--table2_path", str(place_path),
            "--key2", "id",
            "--payload2_cols", "name",
            "--output_path", str(join_output_path)
        ]
        subprocess.run(join_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator join exited successfully.")


        # --- Step 2: Filter this joined output for the desired person

        print(f"Step 2: Filtering result of join for person with ID {person_id}.")
        filter_cmd = [
            "python", "operator1.py",
            "--filepath", str(join_output_path),
            "--output_path", output_path,
            "--filter_col", "id",
            "--payload_cols", "firstName", "name",
            "--filter_threshold_op1", str(person_id),
            "--filter_condition_op1", "==",
        ]
        subprocess.run(filter_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator filter exited succesfully.")
        print(f"Output of short read 1 written to {output_path}")

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
    parser.add_argument("--person_id", type=int, required=True, help="The ID of the person to look up.")
    parser.add_argument("--LDBC_dir_path", default="Big_LDBC", help="Path to LDBC database.")
    parser.add_argument("--output_path", default="Big_LDBC/sr4_output.csv", help="Path for the final output CSV file.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories.")
    args = parser.parse_args()

    shortread1(
        args.person_id,
        args.LDBC_dir_path,
        args.output_path,
        args.no_cleanup
    )

if __name__ == "__main__":
    main()
