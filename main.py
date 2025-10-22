import argparse

def parse_args():
    arg_parser = argparse.ArgumentParser(description="This project is designed to create preprocess data by instrument")
    arg_parser.add_argument("--bgn", type=str, required=True, help="begin date, format = 'YYYYMMDD'")
    arg_parser.add_argument("--end", type=str, default=None, help="end date, format = 'YYYYMMDD'")
    return arg_parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    bgn = args.bgn
    end = args.end or bgn