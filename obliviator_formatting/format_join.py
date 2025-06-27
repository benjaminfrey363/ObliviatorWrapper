
import pandas as pd
import argparse

######################################
# Format data for an obliviator join #
######################################

# To perform a join, obliviator expects data to be passed in the form
# <num_rows_table1> <num_rows_table2>
# join_key[0]       id[0]
# ...               ...
# The first <num_rows_table1> rows will then correspond to table1, and the
# remainder of the rows (should be <num_rows_table2>) correspond to table2.
# Every table entry has a join key, and another value.
# The id in the second column should be a unique identifier so that
# we can meaningfully interpret the output of the join.
# -> We'll fix this to just be the unique "id" attribute which all LDBC objects have.

# So take as args the two files to be joined and the desired join key.
# Return as output a string consisting of the formatted data
# Have to pass 2 join keys, as join key may have different names in different dataframes.
# For example, a comment's CreatorPersonId corresponds to a person's id.

# EXPECTED OUTPUT FROM OBLIVIATOR JOIN:
# Obliviator will join the two tables and return matches of ids between the two.
# table1_id[0]  table2_matchid[0]
# ...           ...

def format_join(filepath1: str, filepath2: str, join_key1: str, join_key2: str, output_path: str):
    # Load CSVs
    df1 = pd.read_csv(filepath1, sep="|")
    df2 = pd.read_csv(filepath2, sep="|")

    # Validate required columns
    for col, path, df in [(join_key1, filepath1, df1), ("id", filepath1, df1),
                          (join_key2, filepath2, df2), ("id", filepath2, df2)]:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in {path}")

    # Handle t1, including case where join key is id
    if join_key1 == "id":
        t1 = pd.DataFrame({"key": df1["id"], "uid": df1["id"]})
    else:
        t1 = pd.DataFrame({"key": df1[join_key1], "uid": df1["id"]})

    # Handle t2, including case where join key is id
    if join_key2 == "id":
        t2 = pd.DataFrame({"key": df2["id"], "uid": df2["id"]})
    else:
        t2 = pd.DataFrame({"key": df2[join_key2], "uid": df2["id"]})

    # Format header
    header = f"{len(t1)} {len(t2)}"

    # Format lines
    lines = [f"{row['key']} {row['uid']}" for _, row in pd.concat([t1, t2], ignore_index=True).iterrows()]

    # Write to file
    with open(output_path, "w") as f:
        f.write("\n".join([header] + lines))
    return



def format_small_join(filepath1, filepath2, join_key1, join_key2, output_path, sample_size=500):
    # Load the entire df2 and sample from it
    df2 = pd.read_csv(filepath2, sep="|")
    df2_sampled = df2.head(sample_size)  # or .sample(sample_size) if random sampling preferred
    
    # Extract unique join keys from df2 sample, drop NaNs
    keys_to_match = df2_sampled[join_key2].dropna().unique()

    # Load df1 and filter rows with join_key1 in keys_to_match
    df1 = pd.read_csv(filepath1, sep="|")
    df1_filtered = df1[df1[join_key1].isin(keys_to_match)]

    # Validate required columns in filtered df1 and sampled df2
    for col, path, df in [(join_key1, filepath1, df1_filtered), ("id", filepath1, df1_filtered),
                          (join_key2, filepath2, df2_sampled), ("id", filepath2, df2_sampled)]:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in {path}")

    # Prepare t1 dataframe (join_key and id)
    if join_key1 == "id":
        t1 = pd.DataFrame({"key": df1_filtered["id"], "uid": df1_filtered["id"]})
    else:
        t1 = pd.DataFrame({"key": df1_filtered[join_key1], "uid": df1_filtered["id"]})

    # Prepare t2 dataframe (join_key and id)
    if join_key2 == "id":
        t2 = pd.DataFrame({"key": df2_sampled["id"], "uid": df2_sampled["id"]})
    else:
        t2 = pd.DataFrame({"key": df2_sampled[join_key2], "uid": df2_sampled["id"]})

    # Format header line: number of rows in t1 and t2
    header = f"{len(t1)} {len(t2)}"

    # Format data lines for join: "<join_key> <id>"
    lines = [f"{row['key']} {row['uid']}" for _, row in pd.concat([t1, t2], ignore_index=True).iterrows()]

    # Write to output file
    with open(output_path, "w") as f:
        f.write("\n".join([header] + lines))

    return



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath1")
    parser.add_argument("--filepath2")
    parser.add_argument("--join_key1")
    parser.add_argument("--join_key2")
    parser.add_argument("--output_path")
    args = parser.parse_args()
    #format_small_join(args.filepath1, args.filepath2, args.join_key1, args.join_key2, args.output_path, 500)
    format_join(args.filepath1, args.filepath2, args.join_key1, args.join_key2, args.output_path)


if __name__ == "__main__":
    main()



