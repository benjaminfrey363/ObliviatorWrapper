# obliviator_formatting/format_sr2.py

import argparse
import csv
from pathlib import Path

def format_for_sr2(post_file: str, comment_file: str, output_path: str):
    """
    Unifies Post.csv and Comment.csv into a single "message" file
    for LDBC Short Read 2, using the official pipe-delimited format.

    The output format is a standard CSV with headers:
    messageId,messageCreationDate,messageContent,messageCreatorId,originalPostId
    """
    print("--- Unifying Posts and Comments for SR2 (LDBC Schema) ---")
    unified_rows = []

    # Process Post.csv
    try:
        with open(post_file, mode='r', newline='', encoding='utf-8-sig') as infile:
            # Use '|' as the delimiter for LDBC files
            reader = csv.DictReader(infile, delimiter='|')
            for row in reader:
                # For a Post, the originalPostId is its own id.
                unified_rows.append({
                    'messageId': row['id'],
                    'messageCreationDate': row['creationDate'],
                    'messageContent': row['content'],
                    'messageCreatorId': row['CreatorPersonId'],
                    'originalPostId': row['id']
                })
    except FileNotFoundError:
        raise FileNotFoundError(f"Input file not found: {post_file}")
    except KeyError as e:
        raise KeyError(f"Missing expected column in {post_file}. Required column: {e}")


    # Process Comment.csv
    try:
        with open(comment_file, mode='r', newline='', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile, delimiter='|')
            for row in reader:
                # For a Comment, the originalPostId is its ParentPostId value.
                # The spec says ParentCommentId can be used too, but for SR2 we only care about the root Post.
                original_post_id = row.get('ParentPostId') or row.get('ParentCommentId')
                if not original_post_id:
                    continue # Skip comments that are not replies to posts for this query

                unified_rows.append({
                    'messageId': row['id'],
                    'messageCreationDate': row['creationDate'],
                    'messageContent': row['content'],
                    'messageCreatorId': row['CreatorPersonId'],
                    'originalPostId': original_post_id
                })
    except FileNotFoundError:
        raise FileNotFoundError(f"Input file not found: {comment_file}")
    except KeyError as e:
        raise KeyError(f"Missing expected column in {comment_file}. Required column: {e}")

    
    # Write the unified CSV file (using commas, as expected by our other scripts)
    header = ['messageId', 'messageCreationDate', 'messageContent', 'messageCreatorId', 'originalPostId']
    with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=header)
        writer.writeheader()
        writer.writerows(unified_rows)

    print(f"Unification complete. {len(unified_rows)} total messages written to {output_path}.")

def main():
    parser = argparse.ArgumentParser(description="Formats LDBC data for SR2.")
    parser.add_argument("--post_file", required=True)
    parser.add_argument("--comment_file", required=True)
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()
    format_for_sr2(args.post_file, args.comment_file, args.output_path)

if __name__ == "__main__":
    main()
