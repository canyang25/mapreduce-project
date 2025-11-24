import os
import socket
import re
import time
import random
import logging
import grpc
import threading
from concurrent import futures
import importlib.util
import pyarrow as pa
import pyarrow.parquet as pq

from pyarrow import fs
from pyarrow.fs import FileSelector, FileType

import worker_to_master_pb2
import worker_to_master_pb2_grpc

import master_to_worker_pb2
import master_to_worker_pb2_grpc

LOG = logging.getLogger("worker")
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

TASK_SERVER_PORT = 50051
WORKER_ID = socket.gethostname()

def get_hdfs():
    return fs.HadoopFileSystem("boss", 9000)

def ensure_parent_dir(fs, path: str) -> None:
    parent = path.rsplit("/", 1)[0] if "/" in path else ""
    if parent:
        try:
            fs.mkdir(parent)
        except Exception:
            pass

def load_user_function(job_path, function_name):
    fs = get_hdfs()
    with fs.open_input_stream(job_path) as f:
        code = f.read().decode("utf-8")
    spec = importlib.util.spec_from_loader("user_job", loader=None)
    module = importlib.util.module_from_spec(spec)
    exec(code, module.__dict__)
    fn = getattr(module, function_name, None)
    if fn is None:
        raise AttributeError(f"{function_name} not found in {job_path}")
    return fn

def write_lines(fs, path, lines):
    ensure_parent_dir(fs, path)
    with fs.open_output_stream(path) as out:
        for line in lines:
            if not line.endswith("\n"):
                line = line + "\n"
            out.write(line.encode("utf-8"))

def register_with_retry(namenode_host, namenode_port, max_attempts=8, initial_delay=1.0):
    """Register this worker with the registry service. Retries with exponential backoff.

    Raises RuntimeError on unrecoverable failure.
    """
    container_id = socket.gethostname()
    LOG.info("Registering worker: id=%s", container_id)
    attempt, delay, last_exc = 0, initial_delay, None

    while attempt < max_attempts:
        attempt += 1
        try:
            with grpc.insecure_channel(f'{namenode_host}:{namenode_port}') as ch:
                stub = worker_to_master_pb2_grpc.RegistryStub(ch)
                resp = stub.Register(worker_to_master_pb2.WorkerInfo(id=container_id))
                if not resp.ok:
                    raise RuntimeError(f"Registration rejected: {resp.message}")
                LOG.info("Registered successfully as %s", container_id)
                return resp
        except Exception as e:
            last_exc = e
            LOG.warning("Registration attempt %d failed: %s", attempt, e)
            time.sleep(delay)
            delay = min(delay * 2, 30)
    raise RuntimeError(f"Failed to register worker after {max_attempts} attempts") from last_exc


def heartbeat_loop(namenode_host: str, namenode_port: int, worker_id: str, interval_seconds: float = 5.0):
    """Continuously send heartbeat to master to indicate liveness.
    
    Uses jitter to prevent thundering herd problem when multiple workers heartbeat simultaneously.
    Runs indefinitely; logs warnings on failures but keeps retrying.
    """
    # Add initial random delay to spread out worker heartbeats (0-2 seconds)
    initial_jitter = random.uniform(0, 2.0)
    LOG.info("Starting heartbeat loop with %.2fs initial delay", initial_jitter)
    time.sleep(initial_jitter)
    
    while True:
        # Calculate next sleep time with jitter
        jitter = interval_seconds * 0.1 * (2 * random.random() - 1)
        sleep_time = interval_seconds + jitter
        
        try:
            with grpc.insecure_channel(f"{namenode_host}:{namenode_port}") as ch:
                stub = worker_to_master_pb2_grpc.RegistryStub(ch)
                response = stub.Heartbeat(worker_to_master_pb2.HeartbeatRequest(worker_id=worker_id))
                
                if not response.ok:
                    LOG.warning("Heartbeat rejected by master: %s", response.message)
                else:
                    LOG.debug("Heartbeat acknowledged by master (next in ~%.1fs)", sleep_time)
        except Exception as e:
            LOG.warning("Heartbeat failed: %s (will retry in ~%.1fs)", e, sleep_time)
        
        time.sleep(sleep_time)

class WorkerTaskServicer(master_to_worker_pb2_grpc.WorkerTaskServicer):
    def RunMap(self, request, context):
        LOG.info(f"[map] worker={WORKER_ID} task_id={request.task_id}")
        try:
            fs = get_hdfs()
            map_fn = load_user_function(request.job_path, request.function_name)
            iterator_fn = load_user_function(request.job_path, request.iterator_fn) if request.iterator_fn else None
            partitions = [
                {"key": [], "value": []} 
                for _ in range(request.num_reducers)
            ]

            for data_path in request.data_paths:
                with fs.open_input_stream(data_path) as f:
                    if iterator_fn:
                        file_bytes = f.readall()
                        metadata = {"size" : len(file_bytes), "file_path": data_path}
                        for key, val in iterator_fn(file_bytes, metadata):
                            for k, v in map_fn(key, val):
                                rid = (hash(k) % request.num_reducers)
                                partitions[rid]["key"].append(str(k))
                                partitions[rid]["value"].append(str(v))

                    else:
                        for count, line in enumerate(f.read().decode("utf-8").splitlines()):
                            for k, v in map_fn(count, line):
                                rid = (hash(k) % request.num_reducers)
                                partitions[rid]["key"].append(str(k))
                                partitions[rid]["value"].append(str(v))

            for rid, partition in enumerate(partitions):
                out_path = f"{request.output_dir.rstrip('/')}/{WORKER_ID}_{request.task_id}_{rid}.parquet"
                table = pa.table(partition)
                with fs.open_output_stream(out_path) as out:
                    pq.write_table(table, out)
                LOG.debug(f"[map] wrote partition rid={rid} -> {out_path}")
            LOG.info(f"[map] task {request.task_id} complete")
            return master_to_worker_pb2.Ack(ok=True, message="map done")
        except Exception as e:
            LOG.error(f"[map] task {request.task_id} failed: {e}", exc_info=True)
            return master_to_worker_pb2.Ack(ok=False, message=str(e))

    def RunReduce(self, request, context):
        LOG.info(f"[reduce] worker={WORKER_ID} task_id={request.task_id}")
        try:
            fs = get_hdfs()
            reduce_fn = load_user_function(request.job_path, request.function_name)
            selector = FileSelector(request.input_dir, recursive=False)
            try:
                infos = fs.get_file_info(selector)
            except Exception as e:
                # Listing can fail if HDFS consults a dead DataNode; continue best-effort.
                LOG.warning(f"[reduce] Unable to list {request.input_dir}: {e}")
                infos = []

            files = [
                info.path
                for info in infos
                if info.type == FileType.File and info.path.endswith(f"_{request.partition_id}.parquet")
            ]

            tables = []
            for path in files:
                try:
                    tables.append(pq.read_table(path, filesystem=fs))
                except Exception as e:
                    LOG.warning(
                        f"[reduce] Skipping unreadable file {path} (likely from dead worker): {e}"
                    )
                    continue

            if tables:
                merged = pa.concat_tables(tables)
            else:
                merged = pa.table({"key": [], "value": []})

            df = merged.to_pandas()
            total_in = len(df)
            if df.empty:
                grouped = {}
            else:
                grouped = df.groupby("key")["value"].apply(list)

            out_lines = []
            for k, values in grouped.items():
                for out in reduce_fn(k, values):
                    if isinstance(out, tuple) and len(out) == 2:
                        ok, ov = out
                        out_lines.append(f"{ok}\t{ov}")
                    else:
                        out_lines.append(str(out))

            write_lines(fs, request.output_path, out_lines)
            LOG.info(f"[reduce] complete (in={total_in}, out={len(out_lines)})")
            return master_to_worker_pb2.Ack(ok=True, message="reduce done")
        except Exception as e:
            LOG.error(f"[reduce] failed: {e}", exc_info=True)
            return master_to_worker_pb2.Ack(ok=False, message=str(e))

def start_task_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    master_to_worker_pb2_grpc.add_WorkerTaskServicer_to_server(WorkerTaskServicer(), server)
    server.add_insecure_port(f"[::]:{TASK_SERVER_PORT}")
    server.start()
    LOG.info(f"Worker task server listening on {TASK_SERVER_PORT}")
    server.wait_for_termination()

if __name__ == "__main__":
    try:
        resp = register_with_retry('boss', 8081)
        # Changed heartbeat to run in background thread
        # heartbeat_loop('boss', 8081, socket.gethostname())
        threading.Thread(
            target=heartbeat_loop,
            args=("boss", 8081, WORKER_ID),
            daemon=True
        ).start()
        # Start task server to accept tasks from master
        start_task_server()
    except Exception as e:
        LOG.error("Worker registration failed: %s", e)
        raise
