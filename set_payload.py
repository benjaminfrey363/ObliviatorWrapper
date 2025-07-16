
################################################
# Set the Payload size of Obliviator operators #
################################################

import os
import re
from pathlib import Path
import argparse

OP1_HEADER_PATH = Path(os.path.expanduser("~/obliviator/operator_1/common/elem_t.h"))
FKJOIN_HEADER_PATH = Path(os.path.expanduser("~/obliviator/fk_join/common/elem_t.h"))
JOIN_HEADER_PATH = Path(os.path.expanduser("~/obliviator/join/common/elem_t.h"))

HEADER_DICT = {"op1":OP1_HEADER_PATH, "fkjoin":FKJOIN_HEADER_PATH, "join":JOIN_HEADER_PATH}

def set_payload (size: int, operator: str):
    """Finds and replaces DATA_LENGTH in requested C header file."""
    if operator not in {"op1", "fkjoin", "join"}:
        print("Error in call to set_payload(): Specify an operator from [op1, fkjoin, join].")
    header_path = HEADER_DICT[operator]

    print(f"\n--- Setting DATA_LENGTH to {size} in {header_path} ---")
    if not header_path.exists():
        raise FileNotFoundError(f"C header file not found at: {header_path}")
    try:
        with open(header_path, "r") as f:
            content = f.read()
        new_content, count = re.subn(r"(#define\s+DATA_LENGTH\s+)\d+", rf"\g<1>{size}", content)
        if count == 0:
            raise ValueError(f"Could not find '#define DATA_LENGTH' in {header_path}")
        with open(header_path, "w") as f:
            f.write(new_content)
        print(f"Successfully set DATA_LENGTH to {size}")
    except Exception as e:
        print(f"Error modifying C header file: {e}")
        raise


def set_payloads (size: int):
    for op in ["op1", "fkjoin", "join"]:
        set_payload (size, op)
    print(f"Successfully set all payload sizes to {size}.")



def main():
    parser = argparse.ArgumentParser(description="Modifies Obliviator payload sizes")
    parser.add_argument("--payload_size", type=int, required=True, help="integer payload size")
    args = parser.parse_args()
    set_payloads(args.payload_size)

if __name__ == "__main__":
    main()










