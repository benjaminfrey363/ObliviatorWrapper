import os
import subprocess
from pathlib import Path
import argparse
import shutil
import pandas as pd

def _cleanup_temp_dir(temp_dir_path: Path):
    """Removes the specified temporary directory and its contents."""
    if temp_dir_path.exists() and temp_dir_path.is_dir():
        try:
            shutil.rmtree(temp_dir_path)
            print(f"Temporary directory {temp_dir_path} cleaned up successfully.")
        except OSError as e:
            print(f"Error cleaning up temporary directory {temp_dir_path}: {e}")

def shortread1_join_first(person_id: int, ldbc_dir_path: str, output_path: str, no_cleanup: bool):
    """
    Implementation of LDBC Interactive Short Read 1 with a "join-first" strategy.
    
    1. Joins the entire Person and Place tables.
    2. Filters the large, joined result to find the specific Person.
    """
    print(f"--- Running LDBC Short Read 1 (Join First) for Person ID {person_id} ---")
    
    temp_dir = Path(f"tmp_ldbc_sr1_{os.getpid()}")
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:
        # --- Step 1: Join Person.csv with Place.csv ---
        # This is an intentionally unselective join to test performance.
        print("Step 1: Joining Person.csv with Place.csv...")
        person_csv_path = Path(ldbc_dir_path) / "Person.csv"
        place_csv_path = Path(ldbc_dir_path) / "Place.csv"
        
        # This temporary file will hold the result of the massive join.
        joined_path = temp_dir / "person_with_place.csv"

        join_cmd = [
            "python", "fkjoin.py",
            # Table 1: Person data
            "--table1_path", str(person_csv_path),
            "--key1", "LocationCityId", # Join key
            "--payload1_cols", "id", "firstName", "lastName", "birthday", 
                             "locationIP", "browserUsed", "gender", "creationDate",
            # Table 2: Place data
            "--table2_path", str(place_csv_path),
            "--key2", "id", # Join key
            "--payload2_cols", "name", # We want the city name
            # Output
            "--output_path", str(joined_path),
        ]
        if no_cleanup:
            join_cmd.append("--no_cleanup")
        
        print("Running obliviator FK join...")
        subprocess.run(join_cmd, check=True, cwd=Path(__file__).parent)
        print("Join completed successfully.")

        # --- Step 2: Filter the joined result for the specific person ---
        print("\nStep 2: Filtering the joined table...")
        
        # The final output from the filter operation.
        filtered_path = temp_dir / "sr1_filtered.csv"

        filter_cmd = [
            "python", "operator1.py",
            "--filepath", str(joined_path),
            "--output_path", str(filtered_path),
            "--filter_col", "id", # Filter on the person's ID
            "--payload_cols", "firstName", "lastName", "birthday", "locationIP",
                             "browserUsed", "name", "gender", "creationDate",
            "--filter_threshold_op1", str(person_id),
            "--filter_condition_op1", "==",
        ]
        if no_cleanup:
            filter_cmd.append("--no_cleanup")

        print("Running obliviator filter...")
        subprocess.run(filter_cmd, check=True, cwd=Path(__file__).parent)
        print("Filter completed successfully.")

        # --- Step 3: Format final output ---
        print("\nStep 3: Formatting final output CSV...")
        final_headers = [
            "person.firstName", "person.lastName", "person.birthday",
            "person.locationIP", "person.browserUsed", "city.name",
            "person.gender", "person.creationDate"
        ]
        
        df = pd.read_csv(filtered_path)
        # The filter output includes the original key, which we don't need.
        df_payload = df.drop(columns=['id'])
        df_payload.columns = final_headers
        
        df_payload.to_csv(output_path, index=False)
        
        print(f"✅ Process complete. Final output written to: {output_path}")

    except Exception as e:
        print(f"\n--- FATAL ERROR during LDBC SR1 execution: {e} ---")
        raise
    finally:
        if not no_cleanup:
            _cleanup_temp_dir(temp_dir)

def main():
    parser = argparse.ArgumentParser(description="Runs LDBC Interactive Short Read 1 (Join First).")
    parser.add_argument("--person_id", type=int, required=True, help="The ID of the Person to look up.")
    parser.add_argument("--ldbc_dir_path", default="Big_LDBC", help="Path to LDBC data.")
    parser.add_argument("--output_path", default="sr1_output.csv", help="Path for the final output.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories.")
    args = parser.parse_args()

    shortread1_join_first(
        args.person_id,
        args.ldbc_dir_path,
        args.output_path,
        args.no_cleanup
    )

if __name__ == "__main__":
    main()
