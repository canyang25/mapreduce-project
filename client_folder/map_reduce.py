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

    python -m client_folder.scripts.interactive_client \
        --job /app/client_folder/jobs/inverted_index.py \
        --files /client_folder/data/small/file1.txt \
                /client_folder/data/small/file2.txt \
        --reducers 2
"""

import argparse
import os
from typing import List
import time
import grpc
from pyarrow import fs
from pathlib import Path

import master_client_pb2
import master_client_pb2_grpc

from typing import List, Optional, Tuple, Dict
import subprocess
import sys



class MapReduceClient:
    """This class abstracts MapReduce and HDFS gRPC operations"""
    def __init__(self, namenode_host, namenode_hdfs_port, namenode_client_port):
        # Set up gRPC channel to master
        self.channel = grpc.insecure_channel(f'{namenode_host}:{namenode_client_port}')
        self.stub = master_client_pb2_grpc.MasterClientStub(self.channel)

        # Direct HDFS connection
        self.hdfs_client = fs.HadoopFileSystem(
            host=namenode_host,
            port=namenode_hdfs_port
        )
    
    def submit_job(self, file_paths: list[str], map: str, reduce: str, job_path: str, num_reducers: int, iterator: str = ""):
        """Call map reduce"""
        try:
            request = master_client_pb2.MapReduceRequest(
                file_paths=file_paths, 
                map=map, 
                reduce=reduce, 
                job_path=job_path, 
                num_reducers=num_reducers,
                iterator=iterator
            )
            
            response = self.stub.MapReduce(request)

            if response.ok:
                return response
            else:
                raise Exception(f"Failed map reduce task: {response.error_message}")
        except grpc.RpcError as e:
            raise Exception(f"gRPC error: {str(e)}")    
    def wait_for_completion(self, job_id: str) -> None:
        """Wait for completion of a job via gRPC"""
        try:
            request = master_client_pb2.WaitForCompletionRequest(job_id=job_id)
            response = self.stub.WaitForCompletion(request)
            if not response.ok:
                raise Exception(f"Failed to wait for job completion: {response.error_message}")
        except grpc.RpcError as e:
            raise Exception(f"gRPC error: {str(e)}")
    def write_file(self, hdfs_path: str, data: bytes) -> None:
        """Upload a file to HDFS via hdfs client"""
        try:
            with self.hdfs_client.open_output_stream(hdfs_path) as f:
                f.write(data)
        except Exception as e:
            raise Exception(f"Failed to write file to HDFS: {str(e)}")
    def read_file(self, hdfs_path: str) -> bytes:
        """Read a file from HDFS via hdfs client"""
        try:
            with self.hdfs_client.open_input_stream(hdfs_path) as f:
                return f.read()
        except Exception as e:
            raise Exception(f"Failed to read file from HDFS: {str(e)}")
def cluster_running() -> bool:
    """Check if the Docker Compose cluster is running."""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--services", "--filter", "status=running"],
            capture_output=True,
            text=True,
            check=True
        )
        running_services = result.stdout.strip().splitlines()
        return len(running_services) > 2  # At least master, worker
    except subprocess.CalledProcessError as e:
        print(f"Failed to check cluster status: {e}", file=sys.stderr)
        sys.exit(1)
def start_cluster(compose_args: Optional[List[str]] = None) -> None:
    """Build and start the Docker Compose cluster."""
    compose_args = compose_args or []
    try:
        # Build images
        subprocess.run(["docker", "build", "-f", "Dockerfile.hdfs", "-t", "hdfs", "."], check=True)
        subprocess.run(["docker", "compose", "build", *compose_args], check=True)
        # Start services in detached mode
        subprocess.run(["docker", "compose", "up", "-d", *compose_args], check=True)
        cluster_started = True
        print("Cluster started.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to start cluster: {e}", file=sys.stderr)
        sys.exit(1)

def stop_cluster(timeout: float = 2) -> None:
    """Tear down the Docker Compose cluster."""
    try:
        subprocess.run(["docker", "compose", "down", "--timeout", str(timeout)], check=True)
        print("Cluster stopped.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to stop cluster: {e}", file=sys.stderr)
        sys.exit(1)
def upload_files(client: MapReduceClient, local_to_hdfs: dict[Path, Path]) -> None:
    """Upload a file or directory to HDFS using gRPC."""
    for local_path, hdfs_path in local_to_hdfs.items():
        if local_path.is_file():
            with open(local_path, "rb") as f:
                file_data = (f.read())
            client.write_file(str(hdfs_path), file_data)
        elif local_path.is_dir():
            client.hdfs_client.create_dir(str(hdfs_path))
            for item in sorted(local_path.iterdir()):
                upload_files(client, {item: hdfs_path / item.name})
        else:
            print(f"ERROR: Local path is neither file nor directory: {local_path}", file=sys.stderr)
            sys.exit(1)
    return
def show_logs(services: List[str], follow: bool = False) -> None:
    """Show logs of specified Docker Compose services.
    Args:
        services: List of service names (e.g., master, worker, client)
        follow: Whether to follow logs in real-time
    """
    services_dict = {"master" : "nn", "worker": "dn", "client": "client"}
    cmd = ["docker", "compose", "logs"]
    if follow:
        cmd.append("-f")
    cmd.extend([services_dict[s] for s in services if s in services_dict])
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to show logs: {e}", file=sys.stderr)
        sys.exit(1)
def map_reduce(
    client: MapReduceClient,
    job_file: str,
    input_paths_hdfs: List[str],
    map_fn: Optional[str] = "map",
    reduce_fn: Optional[str] = "reduce",
    num_reducers: Optional[int] = 4,
    iterator_fn: Optional[str] = None
) -> None:
    """Run a MapReduce job via gRPC."""
    if not cluster_running():
        print("Cluster is not running. Please start the cluster first.", file=sys.stderr)
        sys.exit(1)
    # Upload job script to HDFS
    job_name = Path(job_file).name
    hdfs_job_path = f"/jobs/{job_name}"
    try:
        upload_files(client, {job_file: hdfs_job_path})
        print(f"Uploaded job script to HDFS: {hdfs_job_path}")
    except Exception as e:
        print(f"Failed to upload job script to HDFS: {e}", file=sys.stderr)
        sys.exit(1)
    

    # Submit MapReduce job
    try:
        response = client.submit_job(
            file_paths=input_paths_hdfs,
            map=map_fn,
            reduce=reduce_fn,
            job_path=hdfs_job_path,
            num_reducers=num_reducers,
            iterator=iterator_fn or None
        )
        if response.ok:
            print(f"Submitted MapReduce job with ID: {response.job_id}")
        else:
            raise Exception(f"MapReduce submission failed: {response.error_message}")
    except Exception as e:
        print(f"Failed to submit MapReduce job: {e}", file=sys.stderr)
        sys.exit(1)

    # Wait for completion
    try:
        result = client.wait_for_completion(response.job_id)
        if result.ok:
            print("MapReduce job completed successfully.")
            print("Output files:")
            file_bytes = [client.read_file(path) for path in result.file_paths]
            return file_bytes
        else:
            raise Exception(f"{result.error_message}")
    except Exception as e:
        print(f"Failed while waiting for job completion: {e}", file=sys.stderr)
        sys.exit(1)
def parse_args() -> argparse.Namespace:
    usage_msg = """
map_reduce.py – Manage a MapReduce cluster and submit jobs via gRPC.

Commands:
  start                           Build and start the cluster (docker compose).
  stop                            Tear down the cluster (docker compose down).
  upload <local> <hdfs>           Upload a file or directory to HDFS via gRPC.
  logs <service> [...]            Show logs for one or more services (master, worker, client).
  map_reduce <job_file> <inputs>  Run a MapReduce job to completion.
         [--map MAP_FN]           Optional map function name (default: 'map').
         [--reduce REDUCE_FN]     Optional reduce function name (default: 'reduce').
         [--num-reducers N]       Optional number of reducers (default: 4).
         [--iterator ITERATOR_FN] Optional iterator function name (default: None).

Examples:
  python3 map_reduce.py start
  python3 map_reduce.py upload ./local.txt /data/local.txt
  python3 map_reduce.py submit job.py hdfs/data/file1.txt hdfs/data/file2.txt \
        --map=my_map --reduce=my_reduce --num-reducers=3
"""
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
    p_upload = subparsers.add_parser("upload", help="Upload a file or directory to HDFS.")
    p_upload.add_argument("local", help="Local file or directory path to upload")
    p_upload.add_argument("hdfs", help="Destination path in HDFS")

    # Logs
    p_logs = subparsers.add_parser("logs", help="Show logs of specified container(s).")
    p_logs.add_argument("services", nargs="+", help="Service names (e.g., master, worker, client)")
    p_logs.add_argument("-f", "--follow", action="store_true", help="Follow logs")

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
    client = MapReduceClient(
            namenode_host='boss',
            namenode_hdfs_port=9000,
            namenode_client_port=50051
        )

    if command == "start":
        start_cluster()
    elif command == "stop":
        stop_cluster()
    elif command == "upload":
        upload_files(client, {Path(args.local): Path(args.hdfs)})
    elif command == "logs":
        show_logs(args.services, args.follow)
    elif command == "map_reduce":
        map_reduce(
            client,
            job_file=args.job_file,
            input_paths_hdfs=args.inputs,
            map_fn=args.map_fn,
            reduce_fn=args.reduce_fn,
            num_reducers=args.num_reducers,
            iterator_fn=args.iterator_fn
        )
    elif command == "help" or command is None:
        # Show general help if unknown command
        parse_args()
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        parse_args()

if __name__ == "__main__":
    main()