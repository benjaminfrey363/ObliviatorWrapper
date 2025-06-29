# obliviator_formatting/format_join.py (Configurable ID Columns)

import pandas as pd
import argparse

######################################
# Format data for an obliviator join #
######################################

def format_join(filepath1: str, filepath2: str, join_key1: str, id_col1: str, join_key2: str, id_col2: str, output_path: str):
    """
    Formats two CSV files into a single concatenated file for Obliviator join.
    The format is: <num_rows_table1> <num_rows_table2>\n
                  <join_key_value> <unique_id_value>\n... (for table1)
                  <join_key_value> <unique_id_value>\n... (for table2)

    Args:
        filepath1 (str): Path to the first CSV file.
        filepath2 (str): Path to the second CSV file.
        join_key1 (str): The column name to use as the join key in filepath1.
        id_col1 (str): The column name to use as the unique ID in filepath1.
        join_key2 (str): The column name to use as the join key in filepath2.
        id_col2 (str): The column name to use as the unique ID in filepath2.
        output_path (str): Path to the output formatted file.
    """
    # Load CSVs
    df1 = pd.read_csv(filepath1, sep="|")
    df2 = pd.read_csv(filepath2, sep="|")

    # Validate required columns
    for col, path, df in [(join_key1, filepath1, df1), (id_col1, filepath1, df1),
                          (join_key2, filepath2, df2), (id_col2, filepath2, df2)]:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in {path}")

    # Prepare t1 dataframe (join_key and id)
    t1 = pd.DataFrame({"key": df1[join_key1], "uid": df1[id_col1]})

    # Prepare t2 dataframe (join_key and id)
    t2 = pd.DataFrame({"key": df2[join_key2], "uid": df2[id_col2]})

    # Format header
    header = f"{len(t1)} {len(t2)}"

    # Format lines
    lines = [f"{row['key']} {row['uid']}" for _, row in pd.concat([t1, t2], ignore_index=True).iterrows()]

    # Write to file
    with open(output_path, "w") as f:
        f.write("\n".join([header] + lines))
    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath1", required=True)
    parser.add_argument("--filepath2", required=True)
    parser.add_argument("--join_key1", required=True)
    parser.add_argument("--id_col1", required=True, help="Column in filepath1 to use as unique ID.")
    parser.add_argument("--join_key2", required=True)
    parser.add_argument("--id_col2", required=True, help="Column in filepath2 to use as unique ID.")
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()
    format_join(args.filepath1, args.filepath2, args.join_key1, args.id_col1, args.join_key2, args.id_col2, args.output_path)


if __name__ == "__main__":
    main()
