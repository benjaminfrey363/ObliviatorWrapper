
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



def combine_csvs(file1_path, file2_path, output_path):
    """
    Combines two CSV files with the same columns in any order.

    The column order of the first file is used for the output file.
    """
    # First, get the header from the first file
    with open(file1_path, 'r', newline='', encoding='utf-8') as f_in:
        reader = csv.reader(f_in, delimiter='|')
        # Read the first row as the header
        header = next(reader)

    # Now, open files and start combining
    with open(output_path, 'w', newline='', encoding='utf-8') as f_out, \
         open(file1_path, 'r', newline='', encoding='utf-8') as f1_in, \
         open(file2_path, 'r', newline='', encoding='utf-8') as f2_in:

        # Create a writer that uses the standard header
        writer = csv.DictWriter(f_out, fieldnames=header, delimiter='|')
        writer.writeheader()

        # Process the first file
        reader1 = csv.DictReader(f1_in, delimiter='|')
        for row in reader1:
            writer.writerow(row)

        # Process the second file
        reader2 = csv.DictReader(f2_in, delimiter='|')
        for row in reader2:
            writer.writerow(row)

    print(f"Successfully combined '{file1_path}' and '{file2_path}' into '{output_path}'")



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

        # --- Step 1: NFK join of Person.csv with Person_knows_Person.csv on Person2Id
        #       find all friends of all people
        print("Step 1: Joining Person_knows_Person.csv with Person.csv on Person2Id")
        person_path = LDBC_dir_path + "/Person.csv"
        edge_path = LDBC_dir_path + "/Person_knows_Person.csv"
        join1_output_path = temp_dir / "sr3join1.csv"
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
        print("Obliviator join 1 exited successfully.")

        
        # Address asymmetry of edge table
        # -- Step 2: NFK join of Person.csv with Person_knows_Person.csv on Person1Id
        #       find all friends of all people, reversing direction of edges
        #       friendship table is not symmetric, so must perform 2 joins to get all friends.
        #       We will then filter the join outputs separately and combine the results
        print("Step 2: Joining Person_knows_Person.csv with Person.csv on Person1Id")
        join2_output_path = temp_dir / "sr3join2.csv"
        join2_cmd = [
            "python", "join.py",
            "--table1_path", edge_path,
            "--key1", "Person1Id",
            "--payload1_cols", "Person2Id", "creationDate",
            "--table2_path", person_path,
            "--key2", "id",
            "--payload2_cols", "firstName", "lastName",
            "--output_path", str(join2_output_path)
        ]
        if no_cleanup:
            join2_cmd.append("--no_cleanup")
        subprocess.run(join2_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator join 2 exited successfully.")



        # At this point columns in join1_output are
        # t1.Person2Id|t1.Person1Id|t1.creationDate|t2.firstName|t2.lastName
        # filter this file on t1.Person1Id to find friends of person (in first direction)

        # --- Step 3: Filter joint output 1 on t1.Person1Id to get details of
        #       friends of specified person
        print(f"Step 3: Filtering result of first join on $Person1Id = {person_id}")

        print(f"First filtering on Person1Id = {person_id}...")
        filter1_output_path = temp_dir / "sr3filter1.csv"
        filter1_cmd = [
            "python", "operator1.py",
            "--filepath", str(join1_output_path),
            "--output_path", str(filter1_output_path),
            "--filter_col", "t1.Person1Id",
            "--payload_cols", "t1.Person2Id", "t1.creationDate", "t2.firstName", "t2.lastName",
            "--filter_threshold_op1", str(person_id),
            "--filter_condition_op1", "=="
        ]
        if no_cleanup:
            filter1_cmd.append("--no_cleanup")
        subprocess.run(filter1_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator filter 1 exited successfully.")


        # At this point columns in join2_output are
        # t1.Person1Id|t1.Person2Id|t1.creationDate|t2.firstName|t2.lastName
        # filter this file on t1.Person2Id to find friends of people (in reverse direction)
        print(f"Step 4: Filtering result of second join on $Person2Id = {person_id}")

        print(f"First filtering on Person2Id = {person_id}...")
        filter2_output_path = temp_dir / "sr3filter2.csv"
        filter2_cmd = [
            "python", "operator1.py",
            "--filepath", str(join2_output_path),
            "--output_path", str(filter2_output_path),
            "--filter_col", "t1.Person2Id",
            "--payload_cols", "t1.Person1Id", "t1.creationDate", "t2.firstName", "t2.lastName",
            "--filter_threshold_op1", str(person_id),
            "--filter_condition_op1", "=="
        ]
        if no_cleanup:
            filter2_cmd.append("--no_cleanup")
        subprocess.run(filter2_cmd, check=True, cwd=Path(__file__).parent)
        print("Obliviator filter 2 exited successfully.")



        # combine the two filtered CSVs to obtain the final output
        combine_csvs ( str(filter1_output_path), str(filter2_output_path), output_path )

        print(f"Output of short read 3 written to {output_path}.")

        # Finally, calculate composite time of all obliviator operations
        total_time = 0.0
        join_time_file = str(join1_output_path.with_suffix(".time"))
        with open(join_time_file, 'r') as tf:
            total_time += float(tf.read().strip())
        filter1_time_file = str(filter1_output_path.with_suffix(".time"))
        with open(filter1_time_file, 'r') as tf:
            total_time += float(tf.read().strip())
        filter2_time_file = str(filter2_output_path.with_suffix(".time"))
        with open(filter2_time_file, 'r') as tf:
            total_time += float(tf.read().strip())
        # write this compiled time to the <output_path>.time file
        print(f"\n\nTotal time to execute Query 3: {total_time}\n\n")
        with open(Path(output_path).with_suffix('.time'), 'w') as tf:
            tf.write(str(total_time))

    
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
    parser.add_argument("--LDBC_dir_path", default="LDBC_SF1", help="Path to LDBC database.")
    parser.add_argument("--output_path", default="LDBC_SF1/sr_output/sr3_output.csv", help="Path for the final output CSV file.")
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
