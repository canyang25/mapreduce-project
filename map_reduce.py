#!/usr/bin/env python3
import argparse
import os
from typing import List
import time
from pathlib import Path
from typing import List, Optional, Tuple, Dict
import subprocess
import sys
def start_cluster():
    """Start the MapReduce cluster using Docker Compose."""
    subprocess.run(["docker", "build", "-f", "Dockerfile.hdfs", "-t", "hdfs", "."], check=True)
    subprocess.run(["docker", "compose", "up", "--build", "-d"], check=True)
    print("MapReduce cluster started.")
def stop_cluster():
    """Stop the MapReduce cluster using Docker Compose."""
    out = subprocess.run(["docker", "compose", "down"], check=True, capture_output=True)
    print(out.stdout.decode())
def show_logs(services: Optional[List[str]] = None, follow: Optional[bool] = False):
    """Show logs for specified services."""
    service_names = {"master" : "nn", "worker": "dn", "client": "client"}
    if not services:
        services = service_names.keys()
    services = [service_names[s] for s in services if s in service_names]
    cmd = ["docker", "compose", "logs"]
    if follow:
        cmd.append("-f")
    cmd.extend(services)
    subprocess.run(cmd)
def upload_data():
    cmd = ["docker", "exec", "mapreduce-project-client-1", "python3", "client_folder/scripts/upload_data.py"]
    out = subprocess.run(cmd, check=True, capture_output=True)
    print(out.stdout.decode())
def map_reduce(
    job_file: str, inputs: List[str], map_fn: Optional[str] = "map", 
    reduce_fn: Optional[str] = "reduce", num_reducers: Optional[int] = 4, 
    num_mappers: Optional[int] = 4, iterator_fn: Optional[str] = None ):
    """Submit a MapReduce job to the cluster."""
    job_path = Path(job_file)
    if not job_path.is_file():
        job_path = Path("./client_folder/jobs") / job_file
    if not job_path.is_file():
        print(f"Job file not found: {job_file}", file=sys.stderr)
        sys.exit(1)
    data_dir = Path("client_folder/data")
    for idx, input in enumerate(inputs):
        input_path = Path(input)
        if not input_path.resolve().is_relative_to(data_dir.resolve()):
            input_path = data_dir / input_path
            if not input_path.is_file():
                print(f"Input file not found in data directory: {input_path}", file=sys.stderr)
                sys.exit(1)
        input_abs = Path('/') /input_path
        inputs[idx] = str(input_abs)
            
        
    cmd = ["docker", "compose", "exec", "client", "python3", "-m", "client_folder.scripts.interactive_client",
           "--job", f"{job_path}",
           "--files"] + inputs + [
           "--reducers", str(num_reducers)
    ]
    out = subprocess.run(cmd, check=True, capture_output=True)
    print(out.stdout.decode())

    # Prepare the job submission request
usage_msg = """
map_reduce.py – Manage a MapReduce cluster and submit jobs via gRPC.

Commands:
  start                           Build and start the cluster (docker compose).
  stop                            Tear down the cluster (docker compose down).
  upload_data                     Upload all files in client_folder/data to HDFS.
  map_reduce <job_file> <inputs>  Run a MapReduce job to completion with specified job path and input path(local).
         [--map MAP_FN]           Optional map function name (default: 'map').
         [--reduce REDUCE_FN]     Optional reduce function name (default: 'reduce').
         [--num-reducers N]       Optional number of reducers (default: 4).
         [--num-mappers N]        Optional number of mappers (default: 4).
         [--iterator ITERATOR_FN] Optional iterator function name (default: None).
  help                           Show this help message.

Examples:
  python3 map_reduce.py start
  python3 map_reduce.py upload ./local.txt /data/local.txt
  python3 map_reduce.py submit job.py hdfs/data/file1.txt hdfs/data/file2.txt \
        --map=my_map --reduce=my_reduce --num-reducers=3
"""
def parse_args() -> argparse.Namespace:
    global usage_msg
    parser = argparse.ArgumentParser(
        prog="map_reduce",
        description="Manage a MapReduce cluster and submit jobs.",
        usage=usage_msg,
        add_help=True
    )
    subparsers = parser.add_subparsers(dest="command")

    # Start
    p_start = subparsers.add_parser("start", help="Start the MapReduce cluster (docker compose build & up).")

    # Stop
    subparsers.add_parser("stop", help="Stop the MapReduce cluster (docker compose down).")

    # Upload
    p_upload = subparsers.add_parser("upload_data", help="Upload all files in client_folder/data to HDFS.")


    # Logs
    p_logs = subparsers.add_parser("logs", help="Show logs of specified container(s).")
    p_logs.add_argument("services", nargs="+", help="Service names (e.g., master, worker, client)", default=['master', 'worker', 'client'])
    p_logs.add_argument("-f", "--follow", action="store_true", help="Follow logs", default=False)

    # MapReduce
    p_map_reduce = subparsers.add_parser("map_reduce", help="Run a MapReduce job.")
    p_map_reduce.add_argument("job_file", help="Path to the local Python job file")
    p_map_reduce.add_argument("inputs", nargs="+", help="Input file paths in HDFS")
    p_map_reduce.add_argument("--map", dest="map_fn", default="map", help="Map function name (default: map_fn)")
    p_map_reduce.add_argument("--reduce", dest="reduce_fn", default="reduce", help="Reduce function name (default: reduce_fn)")
    p_map_reduce.add_argument("--num-reducers", type=int, default=4, help="Number of reducers (default: 1)")
    p_map_reduce.add_argument("--iterator", dest="iterator_fn", default=None, help="Iterator function name (optional)")

    # Help fallback
    subparsers.add_parser("help", help="Show help")

    # No args -> show help
    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(0)

    return parser.parse_args()
def main():
    args = parse_args()
    command = args.command

    # Instantiate client for gRPC operations

    if command == "start":
        start_cluster()
    elif command == "stop":
        stop_cluster()
    elif command == "upload_data":
        upload_data()
    elif command == "logs":
        show_logs(args.services, args.follow)
    elif command == "map_reduce":
        map_reduce(
            job_file=args.job_file,
            inputs=args.inputs,
            map_fn=args.map_fn,
            reduce_fn=args.reduce_fn,
            num_reducers=args.num_reducers,
            iterator_fn=args.iterator_fn
        )
    elif command == "help" or command is None:
        # Show general help if unknown command
        print(usage_msg)
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        parse_args()

if __name__ == "__main__":
    main()