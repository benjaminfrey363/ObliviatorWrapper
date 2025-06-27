
import os
import subprocess
from pathlib import Path
import argparse

###########################
# OBLIVIATOR JOIN WRAPPER #
###########################

# Top-level script to execute an obliviator join
# Take as inputs filenames of objects to be joined and the join key of each object
# (join keys are to be matched, so should be equivalent to each other. Passed as separate
# arguments for case where they have different names, e.g. join of Person.id with Comment.CreatorPersonId)
# Finally, take as argument output path to write output of join.
# Output will consist of matched join key pairs.

def obliviator_join ( 
    filepath1: str, 
    filepath2: str, 
    join_key1: str, 
    join_key2: str, 
):
    
    print("Running oblivious join of " + filepath1 + " with join key " + join_key1 + " and " + filepath2 + " with join key " + join_key2)

    # Create temp directory
    temp_dir = Path("tmp_join")
    temp_dir.mkdir(exist_ok=True)

    print("Created temp directory " + str(temp_dir))

    #######################################
    # 1. Format input for obliviator join #
    #######################################
    
    print("Formatting input CSVs for obliviator join...")
    format_path = temp_dir / "format.txt"
    subprocess.run([
        "python", "obliviator_formatting/format_join.py",
        "--filepath1", filepath1, 
        "--filepath2", filepath2,
        "--join_key1", join_key1, 
        "--join_key2", join_key2,
        "--output_path", str(format_path)
  ], check=True, cwd=Path(__file__).parent)
    print("Formatted input written to " + str(format_path) + ".")

    ###################################################
    # 2. Relabel IDs to reduce into Obliviators range #
    ###################################################

    print("Relabeling IDs...")
    relabel_path = temp_dir / "relabel.txt"
    mapping_path = temp_dir / "map.txt"
    subprocess.run([
        "python", "obliviator_formatting/relabel_ids.py",
        "--input_path", str(format_path),
        "--output_path", str(relabel_path),
        "--mapping_path", str(mapping_path)
    ],check=True, cwd=Path(__file__).parent)
    print("Relabeled input written to " + str(relabel_path) + ", relabel map written to " + str(mapping_path) + ".")

    ##########################
    # 3. Run Obliviator Join #
    ##########################

    print("Running Obliviator join...")
    obliv_output_path = temp_dir / "obliv_output.txt"
    code_dir = os.path.expanduser("~/obliviator/join_kks")

    subprocess.run(["make", "clean"], cwd=code_dir, check=True)
    subprocess.run(["make", "L3=1"], cwd=code_dir, check=True)

    print("Build completed. Executing join with")
    print("\tInput path: " + str(relabel_path))
    print("\tOutput path: " + str(obliv_output_path))
    subprocess.run([
        "./app",
        "../" + str(obliv_output_path), "../" + str(relabel_path)
    ],cwd=code_dir,check=True)
    print("Exited Obliviator join successfully, output written to " + str(obliv_output_path) + ".")

    ######################
    # 4. Reverse Relabel #
    ######################

    print("Reverse-relabeling IDs...")
    output_path = temp_dir / "output.txt"

    subprocess.run([
        "python", "obliviator_formatting/reverse_relabel_ids.py",
        "--input_path", str(obliv_output_path),
        "--output_path", str(output_path),
        "--mapping_path", str(mapping_path)
    ],check=True, cwd=Path(__file__).parent)
    print("Reverse-relabeled output written to " + str(output_path) + ".\n\n")

    print(f"✅ Output of Obliviator join written to: {output_path}\n\n")  
    return  


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath1", default="flat_csv/dynamic__Person.csv")
    parser.add_argument("--filepath2", default="flat_csv/dynamic__Comment.csv")
    parser.add_argument("--join_key1", default="id")
    parser.add_argument("--join_key2", default="CreatorPersonId")
    args = parser.parse_args()
    obliviator_join(args.filepath1, args.filepath2, args.join_key1, args.join_key2)


if __name__ == "__main__":
    main()

