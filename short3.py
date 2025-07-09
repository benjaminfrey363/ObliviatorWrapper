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

def shortread3_join_first(person_id: int, ldbc_dir_path: str, output_path: str, no_cleanup: bool):
    """
    Implementation of LDBC Interactive Short Read 3 with a "join-first" strategy.
    
    1. Joins the entire Person_knows_Person and Person tables.
    2. Filters the large, joined result to find friends of the specific Person.
    """
    print(f"--- Running LDBC Short Read 3 (Join First) for Person ID {person_id} ---")
    
    temp_dir = Path(f"tmp_ldbc_sr3_{os.getpid()}")
    temp_dir.mkdir(exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:
        # --- Step 1: Join Person_knows_Person with Person to get friend details ---
        print("Step 1: Joining Person_knows_Person.csv with Person.csv...")
        knows_csv_path = Path(ldbc_dir_path) / "Person_knows_Person.csv"
        person_csv_path = Path(ldbc_dir_path) / "Person.csv"
        
        # This temporary file will hold all friendships with friend details.
        all_friends_path = temp_dir / "all_friend_details.csv"

        join_cmd = [
            "python", "fkjoin.py",
            # Table 1: The full relationship table
            "--table1_path", str(knows_csv_path),
            "--key1", "Person2Id", # Join on the friend's ID
            "--payload1_cols", "Person1Id", "creationDate",
            # Table 2: The person details table
            "--table2_path", str(person_csv_path),
            "--key2", "id", # Join on the person's ID
            "--payload2_cols", "firstName", "lastName",
            # Output
            "--output_path", str(all_friends_path),
        ]
        if no_cleanup:
            join_cmd.append("--no_cleanup")
        
        print("Running obliviator FK join...")
        subprocess.run(join_cmd, check=True, cwd=Path(__file__).parent)
        print("Join completed successfully.")

        # --- Step 2: Filter the joined result for the specific person ---
        print("\nStep 2: Filtering the joined table for the start person...")
        
        filtered_path = temp_dir / "sr3_filtered.csv"

        filter_cmd = [
            "python", "operator1.py",
            "--filepath", str(all_friends_path),
            "--output_path", str(filtered_path),
            "--filter_col", "Person1Id", # Filter on the person who started the friendship
            "--payload_cols", "Person2Id", "creationDate", "firstName", "lastName",
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
            "friend.id", "friend.firstName",
            "friend.lastName", "knows.creationDate"
        ]
        
        df = pd.read_csv(filtered_path)
        
        # Reorder and rename columns to match the spec.
        df_final = df[['Person2Id', 'firstName', 'lastName', 'creationDate']]
        df_final.columns = final_headers
        
        # Sort the results as required by the spec.
        df_final = df_final.sort_values(by=["knows.creationDate", "friend.id"], ascending=[False, True])
        
        df_final.to_csv(output_path, index=False)

        print(f"✅ Process complete. Final output written to: {output_path}")

    except Exception as e:
        print(f"\n--- FATAL ERROR during LDBC SR3 execution: {e} ---")
        raise
    finally:
        if not no_cleanup:
            _cleanup_temp_dir(temp_dir)

def main():
    parser = argparse.ArgumentParser(description="Runs LDBC Interactive Short Read 3 (Join First).")
    parser.add_argument("--person_id", type=int, required=True, help="The ID of the Person to find friends for.")
    parser.add_argument("--ldbc_dir_path", default="Big_LDBC", help="Path to LDBC data.")
    parser.add_argument("--output_path", default="sr3_output.csv", help="Path for the final output.")
    parser.add_argument("--no_cleanup", action="store_true", help="Do not clean up temporary directories.")
    args = parser.parse_args()

    shortread3_join_first(
        args.person_id,
        args.ldbc_dir_path,
        args.output_path,
        args.no_cleanup
    )

if __name__ == "__main__":
    main()
