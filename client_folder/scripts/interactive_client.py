"""
MapReduce Interactive Client Shell

This script lets you run ANY MapReduce job by pointing to a Python
job file that defines:

    def map_function(input_key: str, input_value: str) -> list[tuple]
    def reduce_function(key, values) -> tuple

The job file is:
    - Uploaded to HDFS
    - Distributed to workers
    - Dynamically loaded by them using the function names:
        "map_function" and "reduce_function"

Example usage (inside client container):

    docker compose exec client python3 -m client_folder.scripts.interactive_client \
    --job /app/client_folder/jobs/<job file name> \
    --files /client_folder/data/<file 1> \
            /client_folder/data/<file 2> \
            ...
    --maps <number of mappers> \
    --reducers <number of reducers>
"""
from .client import Client

import argparse
import os
from typing import List
import time

def upload_job_script(client: Client, local_job_path: str) -> str:
    """
    Upload the local Python job script to HDFS and return its HDFS path.
    """
    if not os.path.isfile(local_job_path):
        raise FileNotFoundError(f"Job script not found: {local_job_path}")

    job_name = os.path.basename(local_job_path)

    # You can change this prefix if you want a different HDFS directory
    hdfs_path = f"/jobs/{job_name}"

    with open(local_job_path, "rb") as f:
        script_bytes = f.read()

    client.write_file(hdfs_path, script_bytes)
    print(f"Uploaded job script to HDFS: {hdfs_path}")

    return hdfs_path


def run_map_reduce_job(
    local_job_script: str,
    uploaded_job_script: str,
    file_paths: List[str],
    num_maps: int,
    num_reducers: int,
    iterator: str = None,
    map_fn: str = 'map_function',
    reduce_fn: str = 'reduce_function'
):
    """
    High-level entry: upload the job script, run MapReduce, print results.
    """
    client = Client(
            namenode_host='boss',
            namenode_hdfs_port=9000,
            namenode_client_port=50051
        )
    # Give time for HDFS to be ready
    time.sleep(5)

    # 1. Write the job file to HDFS
    try:
        with open(local_job_script, 'rb') as f:
            fcn_b = f.read()
        client.write_file(uploaded_job_script, fcn_b)
    except Exception as e:
        print(f"Failed to upload job script to HDFS: {e}")
        return

    # 2. Run distributed MapReduce
    print("Starting MapReduce job...")
    try:
        response = client.map_reduce(file_paths, map_fn, reduce_fn, uploaded_job_script, num_maps, num_reducers, iterator)
    except Exception as e:
        print(f"MapReduce job failed")
        return

    # 3. Read and print output files
    print("\n=== MapReduce Output Files ===")
    for each in response.file_paths:
        print(f"- {each}")
        content_bytes = client.read_file(each)
        try:
            content_str = content_bytes.decode("utf-8")
        except AttributeError:
            # If read_file already returns str
            content_str = content_bytes

        print("----- file contents -----")
        print(content_str)
        print("-------------------------\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Interactive MapReduce client (job = Python file path)."
    )

    parser.add_argument(
        "--job",
        required=True,
        help="Local path (inside client container) to the Python MapReduce job file.",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        required=True,
        help="List of HDFS input file paths to process.",
    )
    parser.add_argument(
        "--maps",
        type=int,
        default=None,
        help="Number of reducers to use (default: number of input files).",
    )
    parser.add_argument(
        "--reducers",
        type=int,
        default=2,
        help="Number of reducers to use (default: 2).",
    )
    parser.add_argument(
        "--iterator",
        type=str,
        default=None,
        help="Optional iterator function name defined in the job file (default: None).",
    )
    parser.add_argument(
        "--map_fn",
        type=str,
        default="map_function",
        help="Optional map function name defined in the job file (default: None).",
    )
    parser.add_argument(
        "--reduce_fn",
        type=str,
        default="reduce_function",
        help="Optional reduce function name defined in the job file (default: None).",
    )

    args = parser.parse_args()
    args.maps = len(args.files)
    return args


def main():
    args = parse_args()
    uploaded_job_script = f"/jobs/{os.path.basename(args.job)}"
    run_map_reduce_job(
        local_job_script=args.job,
        uploaded_job_script=uploaded_job_script,
        file_paths=args.files,
        num_maps=args.maps,
        num_reducers=args.reducers,
        iterator=args.iterator,
        map_fn=args.map_fn,
        reduce_fn=args.reduce_fn
    )


if __name__ == "__main__":
    main()
