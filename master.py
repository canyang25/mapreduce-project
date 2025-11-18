import os
from subprocess import check_output
import threading
import time
import logging
import grpc
from concurrent import futures
import pyarrow as pa
import pyarrow.fs
import docker

import master_client_pb2
import master_client_pb2_grpc

import master_to_worker_pb2
import master_to_worker_pb2_grpc

from google.protobuf import empty_pb2
import worker_to_master_pb2
import worker_to_master_pb2_grpc

from collections import deque

# Set up logging
LOG = logging.getLogger("master")
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

WORKER_PORT = 50051

# Set up HDFS classpath (optional - health checking doesn't need it)
try:
    os.environ["CLASSPATH"] = str(check_output([os.environ["HADOOP_HOME"]+"/bin/hdfs", "classpath", "--glob"]), "utf-8")
except Exception as e:
    LOG.warning("Could not set HDFS classpath (HDFS may not be ready): %s", e)

class _State:
    """In-memory worker registry."""
    def __init__(self):
        self._worker_lock = threading.Lock() # private lock for worker info
        self._workers = {} # key: worker_id -> {"active": bool, "last_heartbeat": float, "idle": bool}
        self._idle_workers = 0

        self.job_lock = threading.Lock() # public lock for job info
        # job id -> {"map_fn": str, "reduce_fn": str, "job_path": str, "num_reducers": int, "pending_maps" : deque, 
        #           "pending_reduces": deque, maps_left: int, reduces_left: int, "stage": str}
        self.jobs = {}
        self.job_counter = 0
        self.assigned_tasks = {} # worker_id -> task dict
        self.job_done = threading.Condition(self.job_lock)

    def upsert(self, worker_id: str):
        """Register or update a worker by its ID."""
        now = time.time()
        with self._worker_lock:
                self._workers[worker_id] = {"last_heartbeat": now, "idle": True, "task": None, "channel": None}
                self._idle_workers += 1

    def mark_heartbeat(self, key: str):
        with self._worker_lock:
            if key in self._workers:
                self._workers[key]["last_heartbeat"] = time.time()
                #self._workers[key]["active"] = True

    def remove(self, key: str):
        with self._worker_lock:
            worker = self._workers.pop(key, None)
            if worker and worker["idle"]:
                self._idle_workers -= 1
        if worker:
            # Reschedule failed task
            worker["channel"].close() if worker["channel"] else None
            task = self.assigned_tasks.pop(key, None)
            self.retry_task(task)
    def list_workers(self):
        with self._worker_lock:
            return list(self._workers.keys())

    def list_active_workers(self, timeout_seconds: float = 15.0):
        cutoff = time.time() - timeout_seconds
        with self._worker_lock:
            return [k for k, v in self._workers.items() if v["last_heartbeat"] >= cutoff]
    def assign_if_idle(self) -> str | None:
        """Returns an idle worker ID and marks it busy, or None if none available."""
        with self._worker_lock:
            if self._idle_workers == 0:
                return None
            for k, v in self._workers.items():
                if v["idle"]:
                    v["idle"] = False
                    self._idle_workers -= 1
                    return k
        return None
    def mark_idle(self, worker_id: str):
        with self._worker_lock:
            if worker_id in self._workers and not self._workers[worker_id]["idle"]:
                self._workers[worker_id]["idle"] = True
                self._idle_workers += 1
    def retry_task(self, task: dict):
        task_type = task.pop("type", None)
        job_id = task.pop("job_id", None)
        if job_id is None:
            return
        with self.job_lock:
            if job_id in self.jobs:
                if task_type == "map":
                    self.jobs[job_id]["pending_maps"].append(task)
                elif task_type == "reduce":
                    self.jobs[job_id]["pending_reduces"].append(task)
    def create_channel(self, worker_id: str):
        with self._worker_lock:
            if worker_id in self._workers:
                channel = grpc.insecure_channel(f"{worker_id}:{WORKER_PORT}")
                self._workers[worker_id]["channel"] = channel
                return channel
        return None


STATE = _State()

class MasterClientService(master_client_pb2_grpc.MasterClientServicer):
    def __init__(self):
        # Connect to HDFS using the namenode hostname (boss) and port
        # Make connection lazy to avoid startup failures if HDFS isn't ready
        self.hdfs_client = None
        try:
            LOG.info("Initializing MasterClientService with HDFS connection to boss:9000")
            self.hdfs_client = pa.fs.HadoopFileSystem(
                host='boss',
                port=9000
            )
        except Exception as e:
            LOG.warning("HDFS connection failed (will retry when needed): %s", e)
            # Health checking doesn't need HDFS, so we continue
    
    def MapReduce(self, request, context):
        LOG.info("Received MapReduce request")
        if not self.hdfs_client:
            try:
                LOG.info("Establishing HDFS connection...")
                self.hdfs_client = pa.fs.HadoopFileSystem(
                    host='boss',
                    port=9000
                )
            except Exception as e:
                LOG.error("HDFS connection failed: %s", e)
                return master_client_pb2.MapReduceResponse(
                    ok=False,
                    file_paths=[]
                )
        try:
            # For test, assign all map tasks to the first available worker
            job_id = self.create_tasks(request)
            LOG.info(f"Created job {job_id} with pending tasks")
            with STATE.job_lock:
                while STATE.jobs[job_id]["maps_left"] > 0 or STATE.jobs[job_id]["reduces_left"] > 0:
                    STATE.job_done.wait()
            file_paths = [f"output/job_{job_id}/reduce_{i}.out" for i in range(request.num_reducers)]
            return master_client_pb2.MapReduceResponse(
                ok=True,
                message="Job scheduled",
                job_id=job_id,
                file_paths=file_paths
            )
        except Exception as e:
            LOG.error("MapReduce failed: %s", str(e))
            return master_client_pb2.MapReduceResponse(
                ok=False,
                message="MapReduce failed: " + str(e)
            )
    def create_tasks(self, request : master_client_pb2.MapReduceRequest) -> int:
        data_paths = request.file_paths
        map_fn = request.map
        reduce_fn = request.reduce
        job_path = request.job_path
        num_reducers = request.num_reducers
        task_counter = 0
        with STATE.job_lock:
            job_id = STATE.job_counter
            STATE.job_counter += 1
            STATE.jobs[job_id] = {
                "map_fn": map_fn,
                "reduce_fn": reduce_fn,
                "job_path": job_path,
                "num_reducers": num_reducers,
                "pending_maps": deque(),        # pending_maps/reduces -> deque of task dicts that are unscheduled
                "pending_reduces": deque(),
                "maps_left": 0,                 # reduces/maps_left -> count of map tasks unscheduled OR in-progress
                "reduces_left": num_reducers,
                "intermediate_output_dir" : f"output/job_{job_id}/temp/",
                "stage": "map"
            }
            for fp in data_paths:
                # Create a map task for each file
                STATE.jobs[job_id]["pending_maps"].append({
                        "data_paths": [fp],
                        "task_id": task_counter,
                        "iterator_fn": request.iterator
                })
                task_counter += 1
            STATE.jobs[job_id]["maps_left"] = task_counter
            for i in range(num_reducers):
                STATE.jobs[job_id]["pending_reduces"].append({
                    "task_id": task_counter,
                    "partition_id": i,
                    "output_path": f"output/job_{job_id}/reduce_{i}.txt"
                })
                task_counter += 1
        return job_id
# Background health monitor
def health_monitor(check_count):
    timeout_s = 15.0
    active = set(STATE.list_active_workers(timeout_s))
    all_workers = set(STATE.list_workers())
    dead = all_workers - active

    for key in dead:
        LOG.warning("Removing inactive worker: %s", key)
        STATE.remove(key)

    # Report status periodically (every 12 checks = 1 minute)
    if check_count % 12 == 0:
        if active:
            LOG.info("Health check: %d/%d workers active [%s]", 
                    len(active), len(all_workers), "; ".join(sorted(active)))
        elif all_workers:
            LOG.warning("Health check: 0/%d workers active (all dead!)", len(all_workers))

def schedule_loop(interval_seconds: float = 5.0):
    """Schedules tasks and monitors worker health"""
    check_count = 0
    while True:
        # Health check
        health_monitor(check_count)
        check_count += 1
        task = None
        with STATE.job_lock:
            if not task:
            # Try to find a pending task
                for job_id in list(STATE.jobs.keys()):
                    job_info = STATE.jobs[job_id]
                    if job_info["pending_maps"]:
                        task = job_info["pending_maps"].popleft()
                        task["type"] = "map"
                        task["job_id"] = job_id
                        break
                    elif job_info["stage"] == "map" and job_info["maps_left"] == 0:
                        # Transition to reduce stage
                        job_info["stage"] = "reduce"
                    if job_info["stage"] == "reduce" and job_info["pending_reduces"]:
                        task = job_info["pending_reduces"].popleft()
                        task["type"] = "reduce"
                        task["job_id"] = job_id
                        break
                    if job_info["stage"] == "reduce" and job_info["reduces_left"] == 0:
                        LOG.info("Job %d completed", job_id)
                        STATE.jobs.pop(job_id)
        if not task:
            # No pending tasks
            time.sleep(interval_seconds)
            continue
        # Assign task to an idle worker
        worker = STATE.assign_if_idle()
        if not worker:
            # No idle workers
            time.sleep(interval_seconds)
            continue
        # Send task to worker
        if (assign_task(worker, task)):
            task = None # successfully assigned
        time.sleep(interval_seconds)

def assign_task(worker_id: str, task: dict):
    STATE.assigned_tasks[worker_id] = task
    try:
        if not STATE._workers[worker_id]["channel"]:
            STATE.create_channel(worker_id)
        channel = STATE._workers[worker_id]["channel"]
        stub = master_to_worker_pb2_grpc.WorkerTaskStub(channel)
        if task["type"] == "map":
            req = master_to_worker_pb2.MapTaskRequest(
                job_id=task["job_id"],
                job_path=STATE.jobs[task["job_id"]]["job_path"],
                function_name=STATE.jobs[task["job_id"]]["map_fn"],
                data_paths=task["data_paths"],
                num_reducers=STATE.jobs[task["job_id"]]["num_reducers"],
                output_dir=STATE.jobs[task["job_id"]]["intermediate_output_dir"],
                task_id=task["task_id"],
                iterator_fn=task.get("iterator_fn", None)
            )
            fut = stub.RunMap.future(req)
            fut.add_done_callback(lambda r: task_callback(r, worker_id, task))
        elif task["type"] == "reduce":
            req = master_to_worker_pb2.ReduceTaskRequest(
                job_id=task["job_id"],
                job_path=STATE.jobs[task["job_id"]]["job_path"],
                function_name=STATE.jobs[task["job_id"]]["reduce_fn"],
                partition_id=task["partition_id"],
                output_path=task["output_path"],
                task_id=task["task_id"],
                input_dir=STATE.jobs[task["job_id"]]["intermediate_output_dir"]
            )
            fut = stub.RunReduce.future(req)
            fut.add_done_callback(lambda r: task_callback(r, worker_id, task))
    except Exception as e:
        LOG.error("Failed to assign task to worker %s: %s", worker_id, e)
        STATE.assigned_tasks.pop(worker_id, None)
        STATE.mark_idle(worker_id)
        return False
    return True
def task_callback(future, worker_id: str, task: dict):
    try:
        response = future.result()
        if response.ok:
            LOG.info("Task %d completed successfully on worker %s", task["task_id"], worker_id)
            with STATE.job_lock:
                job_info = STATE.jobs.get(task["job_id"], None)
                if job_info:
                    if task["type"] == "map":
                        job_info["maps_left"] -= 1
                    elif task["type"] == "reduce":
                        job_info["reduces_left"] -= 1
                    if job_info["maps_left"] == 0 and job_info["reduces_left"] == 0:
                        STATE.job_done.notify_all()

        else:
            LOG.warning("Task %d failed on worker %s: %s. Retrying...", task["task_id"], worker_id, response.message)
            STATE.retry_task(task)
    except Exception as e:
        LOG.error("Task %d callback failed for worker %s: %s. Retrying task...", task["task_id"], worker_id, e)
        STATE.retry_task(task)
    finally:
        STATE.assigned_tasks.pop(worker_id, None)
        STATE.mark_idle(worker_id)
    


def port_map(id: str):
    client = docker.from_env()
    c = client.containers.get(id)
    return c.attrs["NetworkSettings"]["Ports"]

def output_bindings(ports_dict):
    """Debugs output of port_map"""
    bindings = []
    for container_port, host_list in (ports_dict or {}).items():
        if host_list:
            for b in host_list:
                bindings.append(f'{b["HostIp"]}:{b["HostPort"]} -> {container_port}')
        else:
            bindings.append(f'(not published) -> {container_port}')
    return bindings


class RegistryServicer(worker_to_master_pb2_grpc.RegistryServicer):
    def Register(self, request: worker_to_master_pb2.WorkerInfo, context): 
        worker_id = request.id
        STATE.upsert(worker_id)
        LOG.info("Registered worker %s", worker_id)
        
        # Log current worker pool
        workers = STATE.list_workers()
        LOG.info("Current worker pool: %s", 
                 "; ".join(w for w in workers))
        
        return worker_to_master_pb2.RegisterReply(ok=True, message="registered")

    def Heartbeat(self, request: worker_to_master_pb2.HeartbeatRequest, context):
        """Process heartbeat from worker. Uses worker_id as key."""
        try:
            worker_id = request.worker_id
            STATE.mark_heartbeat(worker_id)
            LOG.debug("Heartbeat received from worker: %s", worker_id)
            return worker_to_master_pb2.HeartbeatReply(ok=True, message="pong")
        except Exception as e:
            LOG.warning("Heartbeat handling failed for %s: %s", request.worker_id, e)
            return worker_to_master_pb2.HeartbeatReply(ok=False, message=str(e))

def make_master_server(client_bind = '[::]:50051', worker_bind = '[::]:8081'):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_client_pb2_grpc.add_MasterClientServicer_to_server(MasterClientService(), server)
    worker_to_master_pb2_grpc.add_RegistryServicer_to_server(RegistryServicer(), server)
    server.add_insecure_port(client_bind)
    server.add_insecure_port(worker_bind)
    return server

def start_master():
    master_srv = make_master_server('[::]:50051', '[::]:8081')

    scheduler = threading.Thread(target=schedule_loop, daemon=True)
    scheduler.start()

    master_srv.start()
    LOG.info("Starting master_client gRPC  on port 50051")
    LOG.info("Starting registry gRPC on port 8081")
    # Wait on both; keep main thread alive
    try:
        master_srv.wait_for_termination()
    except KeyboardInterrupt:
        LOG.info("Shutting down gracefully...")
        master_srv.stop(grace=None)
        LOG.info("Shutdown complete")


if __name__ == '__main__':
    start_master()