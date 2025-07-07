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

def _cleanup_final_csv(temp_path: Path, final_path: Path):
    """
    Reads the intermediate CSV from the filter output and writes a clean one
    with a corrected header and column order.
    """
    print("\nStep 3: Cleaning and finalizing output CSV...")
    
    # Define the desired final header order and names
    final_header = [
        'person_id', 'firstName', 'lastName', 'birthday', 'locationIP', 
        'browserUsed', 'gender', 'creationDate', 'city_id', 'city_name', 'city_url'
    ]

    try:
        with open(temp_path, 'r', encoding='utf-8') as infile, \
             open(final_path, 'w', newline='', encoding='utf-8') as outfile:
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Read the messy header from the temp file (e.g., ['id', 'id', 'name', ...])
            # and write the clean, final header.
            next(reader) 
            writer.writerow(final_header)
            
            # Process the single data row
            for row in reader:
                if not row: continue
                # The intermediate row order is: person_id, city_id, city_name, city_url, firstName, ...
                # Reorder the data to match the final_header
                clean_row = [
                    row[0],  # person_id
                    row[4],  # firstName
                    row[5],  # lastName
                    row[6],  # birthday
                    row[7],  # locationIP
                    row[8],  # browserUsed
                    row[9],  # gender
                    row[10], # creationDate
                    row[1],  # city_id
                    row[2],  # city_name
                    row[3],  # city_url
                ]
                writer.writerow(clean_row)
        print("Final CSV cleanup complete.")
    except (FileNotFoundError, IndexError) as e:
        print(f"Warning: Could not clean up final CSV, the intermediate file might be empty. Error: {e}")
        # Create an empty file with just the header if the intermediate file was empty
        with open(final_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(final_header)


def run_ldbc_sr1_join_first(
    person_id: int,
    person_file: str,
    city_file: str,
    output_path: str,
    no_cleanup: bool
):
    """
    Executes LDBC Short Read 1 with a Join-then-Filter query plan.
    """
    print(f"--- Running LDBC Short Read 1 (Join-First Plan) for Person ID: {person_id} ---")
    
    temp_dir = Path(f"tmp_ldbc_sr1_{os.getpid()}")
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:
        # --- Step 1: Join the full Person and City tables ---
        print("\nStep 1: Joining full person and city tables...")
        joined_csv_path = temp_dir / "joined_person_city.csv"

        # Define payloads for the join step. We need all columns from both tables.
        city_payload_cols = ['name', 'url']
        person_payload_cols = ['id', 'firstName', 'lastName', 'birthday', 'locationIP', 'browserUsed', 'gender', 'creationDate']

        join_cmd = [
            "python", "fkjoin.py",
            "--table1_path", city_file,
            "--key1", "id",
            "--payload1_cols", *city_payload_cols,
            "--table2_path", person_file,
            "--key2", "isLocatedIn",
            "--payload2_cols", *person_payload_cols,
            "--output_path", str(joined_csv_path)
        ]
        subprocess.run(join_cmd, check=True, cwd=Path(__file__).parent)
        print("Join complete. Full joined data at:", joined_csv_path)

        # --- Step 2: Filter the large joined result ---
        print("\nStep 2: Filtering the joined table...")
        
        # The header of the joined file is ['id'(city), 'name', 'url', 'id'(person), 'firstName', ...].
        # We will filter on the second 'id' column, which belongs to the person.
        # The payload will be all other columns.
        filter_payload_cols = [
            'id', 'name', 'url', 'firstName', 'lastName', 'birthday',
            'locationIP', 'browserUsed', 'gender', 'creationDate'
        ]
        
        # The filter operator will write to a temporary file before final cleanup
        temp_filtered_path = temp_dir / "filtered_joined_data.csv"

        filter_cmd = [
            "python", "operator1.py",
            "--filepath", str(joined_csv_path),
            "--output_path", str(temp_filtered_path),
            "--filter_col", "id", # This targets the person's ID
            "--payload_cols", *filter_payload_cols,
            "--filter_threshold_op1", str(person_id),
            "--filter_condition_op1", "=="
        ]
        subprocess.run(filter_cmd, check=True, cwd=Path(__file__).parent)
        print("Filtering complete.")

        # --- Step 3: Cleanup the final CSV ---
        _cleanup_final_csv(temp_filtered_path, Path(output_path))
        
        print(f"\n✅ LDBC Short Read 1 (Join-First) complete. Final output at: {output_path}")

    except Exception as e:
        print(f"\n--- FATAL ERROR during LDBC SR1 execution: {e} ---")
        raise
    finally:
        if not no_cleanup:
            _cleanup_temp_dir(temp_dir)
        else:
            print(f"\nSkipping cleanup of {temp_dir}. Temporary files preserved.")


def main():
    parser = argparse.ArgumentParser(description="Runs LDBC Interactive Short Read 1.")
    parser.add_argument("--person_id", type=int, required=True, help="The ID of the person to look up.")
    parser.add_argument("--person_file", default="LDBC/data/Person.csv", help="Path to the person data CSV file.")
    parser.add_argument("--city_file", default="LDBC/data/City.csv", help="Path to the city data CSV file.")
    parser.add_argument("--output_path", default="LDBC/data/sr1_output.csv", help="Path for the final output CSV file.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories.")
    args = parser.parse_args()

    run_ldbc_sr1_join_first(
        args.person_id,
        os.path.expanduser(args.person_file),
        os.path.expanduser(args.city_file),
        os.path.expanduser(args.output_path),
        args.no_cleanup
    )

if __name__ == "__main__":
    main()
