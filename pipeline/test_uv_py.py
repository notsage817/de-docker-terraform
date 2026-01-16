import argparse
import sys

def main():
    # 1. Show raw arguments passed by the shell/uv
    print("--- Raw Arguments ---")
    print(sys.argv)
    print()

    # 2. Set up the parser to mimic your ingest_data.py
    parser = argparse.ArgumentParser(description="Testing input flags with uv")
    
    parser.add_argument('--pg-user', type=str)
    parser.add_argument('--pg-pass', type=str)
    parser.add_argument('--pg-host', type=str)
    parser.add_argument('--pg-port', type=int)
    parser.add_argument('--pg-db', type=str)
    parser.add_argument('--target-table', type=str)
    parser.add_argument('--year', type=int)
    parser.add_argument('--month', type=int)
    parser.add_argument('--chunksize', type=int)

    args = parser.parse_args()

    # 3. Print parsed values to verify types and content
    print("--- Parsed Values ---")
    for arg in vars(args):
        print(f"{arg}: {getattr(args, arg)} (Type: {type(getattr(args, arg)).__name__})")

if __name__ == "__main__":
    main()