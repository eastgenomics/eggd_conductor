"""
Script to call workflow(s) / apps from a given config

Jethro Rainford 210902
"""
import argparse
from datetime import datetime
import json
import os

# import dxpy


def time_stamp() -> str:
    """
    Returns string of date & time
    """
    return datetime.now().strftime("%Y%m%d_%H%M")


def load_config(config_file) -> dict:
    """
    Read in given config json to dict
    """
    with open(config_file) as file:
        config = json.load(file)

    return config


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Trigger workflows from given config file"
    )

    parser.add_argument(
        '--config_file', required=True
    )
    parser.add_argument(
        '--samples', required=True,
        help='list of sample names to run analysis on'
    )

    args = parser.parse_args()

    return args


def main():
    """
    Main function to run workflows
    """
    args = parse_args()
    print(args)


if __name__ == "__main__":
    main()
